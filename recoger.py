"""Recoge noticias de los periódicos de fuentes.json y las guarda en datos/AAAA-MM-DD.json.

Cada ejecución:
  1. Lee los RSS (o la portada, si el periódico no tiene RSS).
  2. Añade al archivo del día solo las noticias nuevas.
  3. Descarga el texto del artículo cuando se puede.
  4. Apunta qué periódicos han fallado.
  5. Borra los archivos de más de DIAS_GUARDADOS días.
"""

import json
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urljoin, urlparse
from zoneinfo import ZoneInfo

import feedparser
import requests
import trafilatura
from bs4 import BeautifulSoup

ZONA = ZoneInfo("Atlantic/Canary")
CARPETA = Path(__file__).parent
DATOS = CARPETA / "datos"
DIAS_GUARDADOS = 30
MAX_HORAS_ANTIGUEDAD = 48       # se ignoran noticias publicadas hace más de esto
MAX_POR_PORTADA = 60
MAX_TEXTO = 20000               # caracteres máximos de texto por artículo
TEXTO_RSS_SUFICIENTE = 1500     # si el RSS ya trae este texto, no se descarga el artículo
HILOS = 8
CABECERAS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/130.0 Safari/537.36",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8,pt;q=0.7",
}


def descargar(url):
    r = requests.get(url, headers=CABECERAS, timeout=30)
    r.raise_for_status()
    return r


def limpiar_html(html):
    if not html:
        return ""
    texto = BeautifulSoup(html, "html.parser").get_text(" ", strip=True)
    return re.sub(r"\s+", " ", texto).strip()


def fecha_entrada(e):
    for campo in ("published_parsed", "updated_parsed"):
        t = e.get(campo)
        if t:
            return datetime(*t[:6], tzinfo=timezone.utc)
    return None


def leer_rss(url):
    r = descargar(url)
    feed = feedparser.parse(r.content)
    if not feed.entries:
        raise ValueError("el RSS no tiene noticias")
    noticias = []
    for e in feed.entries:
        enlace = e.get("link")
        if not enlace:
            continue
        texto_rss = ""
        if e.get("content"):
            texto_rss = limpiar_html(" ".join(c.get("value", "") for c in e.content))
        noticias.append({
            "titulo": limpiar_html(e.get("title", "")),
            "enlace": enlace,
            "fecha": fecha_entrada(e),
            "resumen": limpiar_html(e.get("summary", ""))[:600],
            "texto_rss": texto_rss,
        })
    return noticias


def buscar_rss_en_portada(url):
    r = descargar(url)
    sopa = BeautifulSoup(r.text, "html.parser")
    for l in sopa.find_all("link", type=re.compile("rss|atom")):
        if l.get("href"):
            return urljoin(url, l["href"])
    raise ValueError("la portada no anuncia ningún RSS")


def parece_articulo(ruta):
    ultimo = ruta.rstrip("/").split("/")[-1]
    return ultimo.count("-") >= 3 or bool(re.search(r"\d{5,}", ruta))


def leer_portada(url, patron=None):
    r = descargar(url)
    sopa = BeautifulSoup(r.text, "html.parser")
    dominio = urlparse(url).netloc.removeprefix("www.")
    vistos, noticias = set(), []
    for a in sopa.find_all("a", href=True):
        enlace = urljoin(url, a["href"]).split("#")[0]
        p = urlparse(enlace)
        if not p.netloc.removeprefix("www.").endswith(dominio):
            continue
        if patron and patron not in p.path:
            continue
        if not patron and not parece_articulo(p.path):
            continue
        titulo = limpiar_html(a.get_text(" "))
        if len(titulo) < 25 or enlace in vistos:
            continue
        vistos.add(enlace)
        noticias.append({"titulo": titulo, "enlace": enlace, "fecha": None,
                         "resumen": "", "texto_rss": ""})
        if len(noticias) >= MAX_POR_PORTADA:
            break
    if not noticias:
        raise ValueError("no se encontraron noticias en la portada")
    return noticias


def leer_fuente(f):
    if f["tipo"] == "rss":
        return leer_rss(f["url"])
    if f["tipo"] == "rss_auto":
        return leer_rss(buscar_rss_en_portada(f["url"]))
    if f["tipo"] == "portada":
        return leer_portada(f["url"], f.get("patron"))
    raise ValueError(f"tipo desconocido: {f['tipo']}")


def bajar_texto(enlace):
    try:
        r = descargar(enlace)
        texto = trafilatura.extract(r.text, url=enlace, include_comments=False)
        if not texto or len(texto) < 300:
            return None, "no_descargado: texto vacío o muy corto (muro de pago, cookies o vídeo)"
        return texto[:MAX_TEXTO], "descargado"
    except Exception as e:  # noqa: BLE001
        return None, f"no_descargado: {type(e).__name__}: {str(e)[:150]}"


def archivo_del_dia(dia):
    return DATOS / f"{dia.isoformat()}.json"


def cargar(ruta, dia):
    if ruta.exists():
        return json.loads(ruta.read_text(encoding="utf-8"))
    return {"fecha": dia.isoformat(), "zona_horaria": "Atlantic/Canary",
            "recogidas": [], "noticias": []}


def borrar_antiguos(hoy):
    limite = hoy - timedelta(days=DIAS_GUARDADOS)
    for ruta in DATOS.glob("*.json"):
        try:
            if datetime.strptime(ruta.stem, "%Y-%m-%d").date() < limite:
                ruta.unlink()
        except ValueError:
            pass


def main():
    DATOS.mkdir(exist_ok=True)
    ahora = datetime.now(ZONA)
    hoy = ahora.date()
    fuentes = json.loads((CARPETA / "fuentes.json").read_text(encoding="utf-8"))

    ruta_hoy = archivo_del_dia(hoy)
    datos = cargar(ruta_hoy, hoy)
    ayer = cargar(archivo_del_dia(hoy - timedelta(days=1)), hoy - timedelta(days=1))
    ya_vistos = {n["enlace"] for n in datos["noticias"] + ayer["noticias"]}

    recogida = {"hora": ahora.isoformat(timespec="minutes"), "por_fuente": {}, "fallos": []}
    nuevas = []
    limite_fecha = datetime.now(timezone.utc) - timedelta(hours=MAX_HORAS_ANTIGUEDAD)

    for f in fuentes:
        try:
            lista = leer_fuente(f)
        except Exception as e:  # noqa: BLE001
            recogida["fallos"].append({"fuente": f["nombre"], "error": f"{type(e).__name__}: {str(e)[:200]}"})
            recogida["por_fuente"][f["nombre"]] = 0
            continue
        contador = 0
        for n in lista:
            if n["enlace"] in ya_vistos:
                continue
            if f.get("descartar") and re.search(f["descartar"], n["enlace"]):
                continue
            if n["fecha"] and n["fecha"] < limite_fecha:
                continue
            ya_vistos.add(n["enlace"])
            contador += 1
            nuevas.append({
                "fuente": f["nombre"],
                "bloque": f["bloque"],
                "titulo": n["titulo"],
                "enlace": n["enlace"],
                "publicada": n["fecha"].astimezone(ZONA).isoformat(timespec="minutes") if n["fecha"] else None,
                "vista": ahora.isoformat(timespec="minutes"),
                "resumen": n["resumen"],
                "texto": n["texto_rss"][:MAX_TEXTO] if n["texto_rss"] else None,
                "texto_estado": "rss" if n["texto_rss"] else None,
                "_articulo": f.get("articulo", True),
            })
        recogida["por_fuente"][f["nombre"]] = contador

    # Descargar el texto de los artículos nuevos que no lo traen en el RSS.
    por_bajar = [n for n in nuevas
                 if n["_articulo"] and len(n["texto"] or "") < TEXTO_RSS_SUFICIENTE]
    with ThreadPoolExecutor(HILOS) as hilos:
        for n, (texto, estado) in zip(por_bajar, hilos.map(lambda x: bajar_texto(x["enlace"]), por_bajar)):
            if texto:
                n["texto"], n["texto_estado"] = texto, estado
            elif not n["texto"]:
                n["texto_estado"] = estado
    for n in nuevas:
        if not n["_articulo"] and not n["texto"]:
            n["texto_estado"] = "no_pedido (solo titulares)"
        del n["_articulo"]

    recogida["noticias_nuevas"] = len(nuevas)
    datos["noticias"].extend(nuevas)
    datos["recogidas"].append(recogida)
    ruta_hoy.write_text(json.dumps(datos, ensure_ascii=False, indent=1), encoding="utf-8")
    borrar_antiguos(hoy)

    print(f"{len(nuevas)} noticias nuevas. Fallos: {len(recogida['fallos'])}")
    for fallo in recogida["fallos"]:
        print(f"  - {fallo['fuente']}: {fallo['error']}")
    return 0


if __name__ == "__main__":
    t = time.time()
    codigo = main()
    print(f"Tardó {time.time() - t:.0f} s")
    sys.exit(codigo)
