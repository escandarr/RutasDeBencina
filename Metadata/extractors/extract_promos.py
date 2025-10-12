# scrape_promos_aliados_meta.py
# -*- coding: utf-8 -*-
"""
Extrae promociones de combustible desde páginas de aliados (HTML estático en su mayoría)
y genera:
 - data/metadata/promos_aliados.json
 - data/metadata/promos_aliados.csv

Requisitos:
    pip install requests beautifulsoup4 lxml
"""

import os, re, csv, json, time, random
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/124.0 Safari/537.36 RutasDeBencina/1.1")
}

# ==== CONFIGURA AQUÍ LAS FUENTES (excluye Petrobras / Aramco como pediste) ====
SOURCES = [
    # Itaú - Legend (suele publicar $150/lt los viernes de octubre, ejemplo)
    {"name": "itau_legend", "url": "https://www.itau.cl/personas/beneficios/legend", "parser": "itau_legend"},
    # Scotiabank - Copec Miércoles (Visa)
    {"name": "scotia_copec", "url": "https://www.scotiabankchile.cl/beneficios/tarjetas/combustible-copec", "parser": "scotia_copec"},
    # Tarjeta Cencosud Scotiabank - Copec Lunes
    {"name": "cencosud_copec", "url": "https://www.tarjetacencosud.cl/beneficios/combustible-copec", "parser": "cencosud_copec"},
    # WOM beneficios MiCopiloto (Shell Miércoles)
    {"name": "wom_micopiloto", "url": "https://www.wom.cl/beneficios/marcas/micopiloto", "parser": "wom_micopiloto"},
    # Banco BICE - MiCopiloto Shell (Domingos)
    {"name": "bice_shell", "url": "https://www.bice.cl/personas/beneficios/micopiloto-shell", "parser": "bice_shell"},
    # Puedes agregar otras páginas de bancos aliados aquí...
]

OUT_DIR = os.path.join(os.path.dirname(__file__), "..", "outputs", "promos")
JSON_PATH = os.path.join(OUT_DIR, "promos_aliados.json")
CSV_PATH  = os.path.join(OUT_DIR, "promos_aliados.csv")

DAYS_MAP = {
    "lunes": "Lunes", "martes": "Martes", "miercoles": "Miércoles", "miércoles": "Miércoles",
    "jueves": "Jueves", "viernes": "Viernes", "sabado": "Sábado", "sábado": "Sábado", "domingo": "Domingo"
}

# ========= helpers =========

def ensure_outdir():
    os.makedirs(OUT_DIR, exist_ok=True)

def clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def fetch(url: str, timeout=30) -> str:
    r = requests.get(url, headers=HEADERS, timeout=timeout)
    r.raise_for_status()
    return r.text

def extract_days(text: str):
    found = re.findall(r"\b(lunes|martes|miércoles|miercoles|jueves|viernes|sábado|sabado|domingo)s?\b", text, re.I)
    seen, out = set(), []
    for d in found:
        key = d.lower()
        key = "miercoles" if key == "miércoles" else key
        norm = DAYS_MAP.get(key, d.capitalize())
        if norm not in seen:
            seen.add(norm); out.append(norm)
    return out

def extract_amounts(text: str):
    # $15, $25, $150, $1.500 (toma el valor textual)
    found = [clean(m) for m in re.findall(r"\$ ?\d{1,3}(?:\.\d{3})?", text)]
    seen, out = set(), []
    for a in found:
        if a not in seen:
            seen.add(a); out.append(a)
    return out

def collapse_fields(montos, dias):
    return ", ".join(montos), ", ".join(dias)

def make_row(titulo, banco, descuento_list, vigencia_list, url):
    desc_str, vig_str = collapse_fields(descuento_list, vigencia_list)
    return {
        "titulo": clean(titulo),
        "banco":  clean(banco),
        "descuento": desc_str,
        "vigencia":  vig_str,
        "fuente": url
    }

# ========= parsers específicos por sitio =========
# Nota: Están hechos para textos típicos. Si cambian el HTML, cae al parser genérico.

def parse_itau_legend(html, url):
    """
    Busca frases tipo: "$150 de dcto. por litro ... los viernes ... Aramco/Copec/Petrobras/Shell ... Hasta ..."
    """
    soup = BeautifulSoup(html, "lxml")
    rows = []
    text = clean(soup.get_text(" "))
    if "dcto" in text.lower() or "descuento" in text.lower():
        titulo = "Itaú Legend – Descuento combustible"
        banco  = "Itaú Legend"
        montos = extract_amounts(text)
        dias   = extract_days(text)
        if montos or dias:
            rows.append(make_row(titulo, banco, montos, dias, url))
    # Intenta también capturar bloques/secciones si existen
    for blk in soup.select("section, article, .beneficio, .card, .promo"):
        t = clean(blk.get_text(" "))
        if "litro" in t.lower() and ("dcto" in t.lower() or "descuento" in t.lower()):
            montos = extract_amounts(t)
            dias   = extract_days(t)
            if montos or dias:
                rows.append(make_row("Itaú Legend – Combustible", "Itaú", montos, dias, url))
    return rows

def parse_scotia_copec(html, url):
    """
    Busca "Miércoles" + "$100/$75/$50/$25 por litro" y menciones Visa/Signature/Black/etc.
    """
    soup = BeautifulSoup(html, "lxml")
    rows = []
    body_text = clean(soup.get_text(" "))
    if "copec" in body_text.lower() and "miércoles" in body_text.lower():
        # bloque general
        montos = extract_amounts(body_text)
        dias   = extract_days(body_text)
        if montos or dias:
            rows.append(make_row("Scotiabank – Copec Miércoles", "Scotiabank Visa", montos, dias, url))

    # Detalle por tipo de tarjeta si aparece en listas
    for blk in soup.select("li, .card, .beneficio, .promo, section"):
        t = clean(blk.get_text(" "))
        if "descuento" in t.lower() or "dcto" in t.lower():
            montos = extract_amounts(t)
            dias   = extract_days(t)
            if montos:
                # heurística de banco/tipo tarjeta
                banco = "Scotiabank Visa"
                if "signature" in t.lower(): banco = "Visa Signature"
                if "black" in t.lower():     banco = "Visa Signature Black"
                if "infinite" in t.lower():  banco = "Visa Infinite"
                if "platinum" in t.lower():  banco = "Visa Platinum"
                if "gold" in t.lower():      banco = "Visa Gold"
                rows.append(make_row("Scotiabank – Detalle tarjeta", banco, montos, dias, url))
    return rows

def parse_cencosud_copec(html, url):
    """
    Busca "$100 por litro" / "$50 por litro", "Todos los lunes", etc. Diferencia Black vs Mastercard/Platinum.
    """
    soup = BeautifulSoup(html, "lxml")
    rows = []
    body_text = clean(soup.get_text(" "))
    if "copec" in body_text.lower() and ("lunes" in body_text.lower() or "todos los lunes" in body_text.lower()):
        montos = extract_amounts(body_text)
        dias   = extract_days(body_text)
        # filas por tipo si aparece
        rows.append(make_row("Cencosud–Copec Lunes (Black)", "Cencosud Scotiabank Black", montos, dias, url))
        rows.append(make_row("Cencosud–Copec Lunes (Mastercard/Platinum)", "Cencosud Scotiabank", montos, dias, url))
    # También intenta tarjetas específicas en bloques
    for blk in soup.select("li, .card, .beneficio, .promo, section, p"):
        t = clean(blk.get_text(" "))
        if "descuento" in t.lower() or "por litro" in t.lower():
            montos = extract_amounts(t); dias = extract_days(t)
            if montos or dias:
                banco = "Cencosud Scotiabank"
                if "black" in t.lower(): banco = "Cencosud Scotiabank Black"
                rows.append(make_row("Cencosud – Detalle", banco, montos, dias, url))
    return rows

def parse_wom_micopiloto(html, url):
    """
    WOM describe: "$50/L los miércoles ... Máx. 2 códigos al mes ... Vigente hasta ..."
    """
    soup = BeautifulSoup(html, "lxml")
    rows = []
    text = clean(soup.get_text(" "))
    if "micopiloto" in text.lower() and ("miércoles" in text.lower() or "miercoles" in text.lower()):
        montos = extract_amounts(text)
        dias   = extract_days(text)
        rows.append(make_row("WOM – Shell Miércoles", "WOM → Shell MiCopiloto", montos, dias, url))
    return rows

def parse_bice_shell(html, url):
    """
    BICE suele publicar: "$100 de dcto por litro los domingos ... Tope $5.000 ... Hasta el 31/10/2025"
    """
    soup = BeautifulSoup(html, "lxml")
    rows = []
    text = clean(soup.get_text(" "))
    if "shell" in text.lower() and ("domingo" in text.lower() or "domingos" in text.lower()):
        montos = extract_amounts(text)
        dias   = extract_days(text)
        rows.append(make_row("BICE – Shell Domingo", "Banco BICE", montos, dias, url))
    return rows

# ========= parser genérico de respaldo =========

def parse_generic(html, url):
    soup = BeautifulSoup(html, "lxml")
    rows = []
    nodes = soup.select("article, .card, .beneficio, .promo, .promotion, section, li, p, .tile, .item")
    if not nodes:
        nodes = soup.find_all(True, recursive=True)

    for node in nodes:
        text = clean(node.get_text(" "))
        if not text:
            continue
        # buscamos evidencia de beneficio combustible
        if ("descuento" in text.lower() or "dcto" in text.lower()) and ("litro" in text.lower() or "$" in text):
            montos = extract_amounts(text)
            dias   = extract_days(text)
            # título cercano si existe
            h = node.find(["h2","h3","h4"])
            titulo = clean(h.get_text()) if h else text[:120]
            # banco heurístico
            banco = ""
            for key in ["Scotiabank","Cencosud","Itaú","BICE","Visa","Mastercard","MiCopiloto","Shell","Copec","Tenpo","Dale","Coopeuch","BCI"]:
                if re.search(rf"\b{key}\b", text, re.I):
                    banco = key; break
            rows.append(make_row(titulo, banco or titulo, montos, dias, url))
    return rows

# ========= enrutador por dominio / clave =========

def route_parser(source_key: str, html: str, url: str):
    try:
        if source_key == "itau_legend":
            return parse_itau_legend(html, url)
        if source_key == "scotia_copec":
            return parse_scotia_copec(html, url)
        if source_key == "cencosud_copec":
            return parse_cencosud_copec(html, url)
        if source_key == "wom_micopiloto":
            return parse_wom_micopiloto(html, url)
        if source_key == "bice_shell":
            return parse_bice_shell(html, url)
    except Exception:
        pass
    # fallback
    return parse_generic(html, url)

# ========= main =========

def main():
    ensure_outdir()
    all_rows = []
    for s in SOURCES:
        url = s["url"]; key = s["parser"]
        try:
            html = fetch(url)
            rows = route_parser(key, html, url)
            all_rows.extend(rows)
            print(f"[OK] {key} -> {len(rows)} filas")
            time.sleep(random.uniform(0.7,1.3))
        except Exception as e:
            print(f"[ERR] {key} {url}: {e}")

    # desduplicar
    seen, dedup = set(), []
    for r in all_rows:
        key = (r["fuente"], r["titulo"], r["banco"], r["descuento"], r["vigencia"])
        if key not in seen:
            seen.add(key); dedup.append(r)

    # guardar JSON
    with open(JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(dedup, f, ensure_ascii=False, indent=2)

    # guardar CSV
    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["titulo","banco","descuento","vigencia","fuente"])
        w.writeheader(); w.writerows(dedup)

    print(f"JSON → {JSON_PATH}")
    print(f"CSV  → {CSV_PATH}")
    print(f"Total filas: {len(dedup)}")
    for r in dedup[:5]:
        print(r)

if __name__ == "__main__":
    main()
