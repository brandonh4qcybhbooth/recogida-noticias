"""Recoge noticias de los periódicos de fuentes.json y las guarda en datos/AAAA-MM-DD.json.

Cada ejecución:
  1. Lee los RSS (o la portada, si el periódico no tiene RSS).
  2. Añade al archivo del día solo las noticias nuevas (no vistas en los últimos días).
  3. Descarga el texto del artículo cuando se puede y lee su fecha real.
  4. Descarta las noticias viejas y los enlaces que no son noticias.
  5. Apunta qué periódicos han fallado.
  6. Borra los archivos de más de DIAS_GUARDADOS días.

Cambios del 22-nov-2026 (mantenimiento semanal):
  - Compara con los archivos de los 3 últimos días (antes solo hoy y ayer).
  - Lee la fecha real del artículo (meta, JSON-LD, URL o texto) y descarta las de más de 48 h.
  - Fechas del futuro: se dejan vacías.
  - Cabeceras de navegador y un segundo intento (Euronews 406, France 24 y SIC 403).
  - RSS vacío: segundo intento (EUalive, Diário de Notícias).
  - Descarta títulos que no son noticias (lista general + lista de cada periódico).
  - Portadas: usa el título del propio artículo (og:title) para que título y enlace coincidan.
  - Marca los textos muy cortos, con muro de pago o cortados.
  - Tope de noticias por recogida en un periódico (max_por_recogida).
  - Campo nuevo «bloque_principal» (un solo nombre por bloque).
"""

import json
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import unquote, urljoin, urlparse
from zoneinfo import ZoneInfo

import feedparser
import requests
import trafilatura
from bs4 import BeautifulSoup

ZONA = ZoneInfo("Atlantic/Canary")
CARPETA = Path(__file__).parent
DATOS = CARPETA / "datos"
DIAS_GUARDADOS = 30
DIAS_COMPARAR = 3               # días de archivos con los que se comparan los enlaces
MAX_HORAS_ANTIGUEDAD = 48       # se ignoran noticias publicadas hace más de esto
MAX_POR_PORTADA = 60
MAX_TEXTO = 20000               # caracteres máximos de texto por artículo
TEXTO_MINIMO = 300              # menos que esto = texto muy corto
TEXTO_RSS_SUFICIENTE = 1500     # si el RSS ya trae este texto, no se descarga el artículo
HILOS = 8
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
CABECERAS = {
    "User-Agent": UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,"
              "application/rss+xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8,pt;q=0.7",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
}
CABECERAS_2 = {"User-Agent": UA, "Accept": "*/*", "Accept-Language": "en-US,en;q=0.9"}

# Títulos que no son noticias (en todos los periódicos).
DESCARTAR_TITULOS = re.compile(
    r"hor[oó]scopo|vi[ñn]eta|avance semanal|telenoticias"
    r"|venta y cesi[oó]n de im[aá]genes|otras noticias de inter[eé]s"
    r"|^las noticias del \d|en sumario|^sumario|esquela"
    r"|\(\d{1,2}/\d{1,2}/\d{2,4}\)\s*$",       # «Padylla (17/11/26)»
    re.IGNORECASE)
# Título que es solo un nombre de persona (esquelas, firmas): 2 a 5 palabras con mayúscula.
SOLO_NOMBRE = re.compile(r"^(?:[A-ZÁÉÍÓÚÑ][a-záéíóúñü]+|de|del|la|y)(?:\s+(?:[A-ZÁÉÍÓÚÑ][a-záéíóúñü]+|de|del|la|y)){1,4}$")
MURO_PAGO = re.compile(
    r"available only to .{0,40}subscribers|subscribe to (?:read|continue)|"
    r"solo para suscriptores|exclusivo para suscriptores|exclusivo para assinantes", re.IGNORECASE)
MESES = {"enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6, "julio": 7,
         "agosto": 8, "septiembre": 9, "setiembre": 9, "octubre": 10, "noviembre": 11,
         "diciembre": 12}


class RSSVacio(ValueError):
    pass


def descargar(url):
    r = requests.get(url, headers=CABECERAS, timeout=30)
    if r.status_code in (403, 406, 429):
        time.sleep(2)
        r = requests.get(url, headers=CABECERAS_2, timeout=30)
    r.raise_for_status()
    return r


def clave(enlace):
    """Forma normalizada de un enlace, para saber si ya se vio (tildes, barra final, utm…)."""
    p = urlparse(unquote(enlace.strip()))
    consulta = "&".join(x for x in p.query.split("&") if x and not x.startswith("utm_"))
    ruta = p.path.rstrip("/")
    return f"{p.netloc.lower().removeprefix('www.')}{ruta}" + (f"?{consulta}" if consulta else "")


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
        raise RSSVacio("el RSS no tiene noticias")
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
        if len(titulo) < 25 or clave(enlace) in vistos:
            continue
        vistos.add(clave(enlace))
        noticias.append({"titulo": titulo, "enlace": enlace, "fecha": fecha_en_url(enlace),
                         "resumen": "", "texto_rss": ""})
        if len(noticias) >= MAX_POR_PORTADA:
            break
    if not noticias:
        raise ValueError("no se encontraron noticias en la portada")
    return noticias


def leer_fuente(f):
    if f["tipo"] == "portada":
        return leer_portada(f["url"], f.get("patron"))
    url = buscar_rss_en_portada(f["url"]) if f["tipo"] == "rss_auto" else f["url"]
    if f["tipo"] not in ("rss", "rss_auto"):
        raise ValueError(f"tipo desconocido: {f['tipo']}")
    try:
        return leer_rss(url)
    except RSSVacio:
        time.sleep(5)            # segundo intento: a veces el RSS sale vacío un momento
        return leer_rss(url)


# ---------- Fechas ----------

def a_fecha(texto):
    """Convierte una fecha ISO (con o sin zona) en datetime con zona. Sin zona = hora de Canarias."""
    if not texto:
        return None
    texto = texto.strip().replace("Z", "+00:00")
    try:
        d = datetime.fromisoformat(texto)
    except ValueError:
        m = re.match(r"(\d{4}-\d{2}-\d{2})(?:[T ](\d{2}:\d{2}))?", texto)
        if not m:
            return None
        d = datetime.fromisoformat(m.group(1) + ("T" + m.group(2) if m.group(2) else ""))
    if d.tzinfo is None:
        d = d.replace(tzinfo=ZONA)
    return d


def fecha_en_url(enlace):
    """Fecha que algunos periódicos ponen en la URL (RTVE /20261121/, RTVC -16-noviembre-2026)."""
    ruta = unquote(urlparse(enlace).path).lower()
    m = re.search(r"/(20\d{2})(\d{2})(\d{2})/", ruta)
    if m:
        try:
            return datetime(int(m[1]), int(m[2]), int(m[3]), 12, tzinfo=ZONA)
        except ValueError:
            pass
    m = re.search(r"-(\d{1,2})-([a-z]+)-(20\d{2})/?$", ruta)
    if m and m[2] in MESES:
        try:
            return datetime(int(m[3]), MESES[m[2]], int(m[1]), 12, tzinfo=ZONA)
        except ValueError:
            pass
    return None


def fecha_del_articulo(html):
    """Fecha de publicación que trae la página del artículo."""
    sopa = BeautifulSoup(html, "html.parser")
    for atrib, nombre in (("property", "article:published_time"), ("name", "article:published_time"),
                          ("itemprop", "datePublished"), ("name", "date"), ("name", "DC.date.issued"),
                          ("property", "og:published_time")):
        m = sopa.find("meta", attrs={atrib: nombre})
        if m and a_fecha(m.get("content")):
            return a_fecha(m.get("content"))
    for s in sopa.find_all("script", type="application/ld+json"):
        m = re.search(r'"datePublished"\s*:\s*"([^"]+)"', s.string or "")
        if m and a_fecha(m.group(1)):
            return a_fecha(m.group(1))
    t = sopa.find("time", datetime=True)
    if t and a_fecha(t["datetime"]):
        return a_fecha(t["datetime"])
    # Texto tipo «16/11/2026 - 17:31» (Diario de Fuerteventura, Noticias Fuerteventura)
    m = re.search(r"(\d{2})/(\d{2})/(\d{4})\s*-?\s*(\d{1,2}):(\d{2})", sopa.get_text(" ")[:20000])
    if m:
        try:
            return datetime(int(m[3]), int(m[2]), int(m[1]), int(m[4]), int(m[5]), tzinfo=ZONA)
        except ValueError:
            pass
    return None


def titulo_del_articulo(html):
    sopa = BeautifulSoup(html, "html.parser")
    m = sopa.find("meta", attrs={"property": "og:title"})
    if m and m.get("content"):
        return limpiar_html(m["content"])
    return None


# ---------- Texto ----------

def bajar_articulo(enlace):
    """Devuelve (texto, estado, fecha, título) del artículo."""
    try:
        r = descargar(enlace)
    except Exception as e:  # noqa: BLE001
        return None, f"no_descargado: {type(e).__name__}: {str(e)[:150]}", None, None
    fecha, titulo = None, None
    try:
        fecha = fecha_del_articulo(r.text)
        titulo = titulo_del_articulo(r.text)
    except Exception:  # noqa: BLE001
        pass
    texto = trafilatura.extract(r.text, url=enlace, include_comments=False)
    if not texto or len(texto) < TEXTO_MINIMO:
        return None, "no_descargado: texto vacío o muy corto (muro de pago, cookies o vídeo)", fecha, titulo
    return texto, "descargado", fecha, titulo


def marcar_texto(n):
    """Pone el estado correcto según cómo es el texto."""
    texto = n["texto"] or ""
    if not texto:
        return
    if MURO_PAGO.search(texto[-600:]) or MURO_PAGO.search(texto[:600]):
        n["texto_estado"] = "cortado: muro de pago"
    elif len(texto) > MAX_TEXTO:
        n["texto"] = texto[:MAX_TEXTO]
        n["texto_estado"] = f"cortado: el artículo tiene más de {MAX_TEXTO} caracteres"
    elif len(texto) < TEXTO_MINIMO:
        n["texto_estado"] = f"no_descargado: texto muy corto ({len(texto)} caracteres)"


def no_es_noticia(titulo, f):
    if DESCARTAR_TITULOS.search(titulo) or SOLO_NOMBRE.match(titulo.strip()):
        return True
    extra = f.get("descartar_titulos")
    return bool(extra and re.search(extra, titulo, re.IGNORECASE))


def bloque_principal(f):
    if f.get("bloque_principal"):
        return f["bloque_principal"]
    return f["bloque"].split(" (")[0].split(" / ")[0].strip()


# ---------- Archivos ----------

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
    ya_vistos = {clave(n["enlace"]) for n in datos["noticias"]}
    for d in range(1, DIAS_COMPARAR):
        dia = hoy - timedelta(days=d)
        ya_vistos |= {clave(n["enlace"]) for n in cargar(archivo_del_dia(dia), dia)["noticias"]}

    recogida = {"hora": ahora.isoformat(timespec="minutes"), "por_fuente": {}, "fallos": [],
                "leer_con_chrome": [], "detalle": {}}
    nuevas = []
    limite_fecha = datetime.now(timezone.utc) - timedelta(hours=MAX_HORAS_ANTIGUEDAD)
    futuro = datetime.now(timezone.utc) + timedelta(minutes=30)

    for f in fuentes:
        if f["tipo"] == "chrome":
            recogida["leer_con_chrome"].append({"fuente": f["nombre"], "url": f["url"],
                                                "motivo": f.get("nota", "")})
            continue
        try:
            lista = leer_fuente(f)
        except Exception as e:  # noqa: BLE001
            recogida["fallos"].append({"fuente": f["nombre"], "error": f"{type(e).__name__}: {str(e)[:200]}"})
            recogida["por_fuente"][f["nombre"]] = 0
            continue
        det = {"leidas": len(lista), "repetidas": 0, "antiguas": 0, "descartadas": 0,
               "no_noticias": 0, "sobran": 0, "mas_reciente": None}
        fechas = [n["fecha"] for n in lista if n["fecha"] and n["fecha"] <= futuro]
        if fechas:
            det["mas_reciente"] = max(fechas).astimezone(ZONA).isoformat(timespec="minutes")
        tope = f.get("max_por_recogida")
        for n in lista:
            k = clave(n["enlace"])
            if k in ya_vistos:
                det["repetidas"] += 1
                continue
            if f.get("descartar") and re.search(f["descartar"], n["enlace"]):
                det["descartadas"] += 1
                continue
            if no_es_noticia(n["titulo"], f):
                det["no_noticias"] += 1
                continue
            if n["fecha"] and n["fecha"] > futuro:
                n["fecha"] = None                 # fecha imposible (del futuro)
            if n["fecha"] and n["fecha"] < limite_fecha:
                det["antiguas"] += 1
                continue
            if tope and sum(1 for x in nuevas if x["fuente"] == f["nombre"]) >= tope:
                det["sobran"] += 1
                continue
            ya_vistos.add(k)
            nuevas.append({
                "fuente": f["nombre"],
                "bloque": f["bloque"],
                "bloque_principal": bloque_principal(f),
                "titulo": n["titulo"],
                "enlace": n["enlace"],
                "publicada": n["fecha"],
                "vista": ahora.isoformat(timespec="minutes"),
                "resumen": n["resumen"],
                "texto": n["texto_rss"] or None,
                "texto_estado": "rss" if n["texto_rss"] else None,
                "_articulo": f.get("articulo", True),
                "_portada": f["tipo"] == "portada",
                "_det": det,
            })
        recogida["detalle"][f["nombre"]] = det

    # Descargar los artículos nuevos: texto, fecha real y (en portadas) título real.
    por_bajar = [n for n in nuevas
                 if n["_articulo"] and (len(n["texto"] or "") < TEXTO_RSS_SUFICIENTE
                                        or n["publicada"] is None)]
    with ThreadPoolExecutor(HILOS) as hilos:
        resultados = hilos.map(lambda x: bajar_articulo(x["enlace"]), por_bajar)
        for n, (texto, estado, fecha, titulo) in zip(por_bajar, resultados):
            if texto and len(texto) > len(n["texto"] or ""):
                n["texto"], n["texto_estado"] = texto, estado
            elif not n["texto"]:
                n["texto_estado"] = estado
            if fecha and fecha <= futuro and (n["publicada"] is None or n["_portada"]):
                n["publicada"] = fecha
            if titulo and n["_portada"]:
                n["titulo"] = titulo

    # Quitar lo que, con la fecha real, resulta viejo o no es noticia.
    guardar = []
    for n in nuevas:
        det = n.pop("_det")
        if n["publicada"] and n["publicada"] < limite_fecha:
            det["antiguas"] += 1
            continue
        if n["_portada"] and no_es_noticia(n["titulo"], {}):
            det["no_noticias"] += 1
            continue
        if not n["_articulo"] and not n["texto"]:
            n["texto_estado"] = "no_pedido (solo titulares)"
        marcar_texto(n)
        if n["publicada"]:
            n["publicada"] = n["publicada"].astimezone(ZONA).isoformat(timespec="minutes")
        del n["_articulo"], n["_portada"]
        guardar.append(n)

    for f in fuentes:
        if f["nombre"] in recogida["detalle"]:
            recogida["por_fuente"][f["nombre"]] = sum(1 for n in guardar if n["fuente"] == f["nombre"])

    recogida["noticias_nuevas"] = len(guardar)
    datos["noticias"].extend(guardar)
    datos["recogidas"].append(recogida)
    ruta_hoy.write_text(json.dumps(datos, ensure_ascii=False, indent=1), encoding="utf-8")
    borrar_antiguos(hoy)

    print(f"{len(guardar)} noticias nuevas. Fallos: {len(recogida['fallos'])}")
    for fallo in recogida["fallos"]:
        print(f"  - {fallo['fuente']}: {fallo['error']}")
    for nombre, det in recogida["detalle"].items():
        print(f"  {nombre}: {det}")
    return 0


if __name__ == "__main__":
    t = time.time()
    codigo = main()
    print(f"Tardó {time.time() - t:.0f} s")
    sys.exit(codigo)
