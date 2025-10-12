# scraping_promos_estatico_meta.py
# -*- coding: utf-8 -*-
"""
Extrae promociones desde páginas ESTÁTICAS y genera:
 - data/metadata/promos_estatico.json
 - data/metadata/promos_estatico.csv

Requisitos:
  pip install requests beautifulsoup4 lxml
"""

import os, csv, json, re, time, random
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/124.0 Safari/537.36 RutasDeBencina/1.0")
}

# === URLs ESTÁTICAS sugeridas ===
URLS = [
    # Petrobras (HTML cargado directo)
    "https://www.petrobrasdistribucion.cl/promociones/descuento-con-medios-de-pago/",
    "https://www.petrobrasdistribucion.cl/promociones/descuento-con-tu-rut/",
    # Si ves que Aramco entrega HTML directo en tu entorno, puedes volver a activarla:
    # "https://www.aramcoestaciones.cl/alianzas-y-beneficios",
]

DAYS_MAP = {
    "lunes": "Lunes", "martes": "Martes", "miercoles": "Miércoles", "miércoles": "Miércoles",
    "jueves": "Jueves", "viernes": "Viernes", "sabado": "Sábado", "sábado": "Sábado", "domingo": "Domingo"
}

OUT_DIR = os.path.join(os.path.dirname(__file__), "..", "outputs", "promos")
JSON_PATH = os.path.join(OUT_DIR, "promos_estatico.json")
CSV_PATH  = os.path.join(OUT_DIR, "promos_estatico.csv")

def ensure_outdir():
    os.makedirs(OUT_DIR, exist_ok=True)

def clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

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
    # $15, $25, $150, $1.500
    found = [clean(m) for m in re.findall(r"\$ ?\d{1,3}(?:\.\d{3})?", text)]
    seen, out = set(), []
    for a in found:
        if a not in seen:
            seen.add(a); out.append(a)
    return out

def fetch(url: str, timeout=30):
    r = requests.get(url, headers=HEADERS, timeout=timeout)
    r.raise_for_status()
    return r.text

# ---------- Parsers específicos ----------

def parse_petrobras_medios(html, url):
    soup = BeautifulSoup(html, "lxml")
    rows = []
    # heuristic: cada h3/h4 es “Banco/Entidad”; descripción en p/li próximos
    for h in soup.select("h3, h4"):
        titulo = clean(h.get_text())
        if not titulo: 
            continue
        blk = []
        for sib in h.find_all_next(["p","li","ul"], limit=8):
            if sib.name in ("h3","h4"):
                break
            blk.append(clean(sib.get_text()))
        text = " ".join(blk)
        if "descuento" not in text.lower():
            continue
        montos = extract_amounts(text)
        dias   = extract_days(text)
        if montos or dias:
            rows.append({
                "titulo": titulo,
                "banco":  titulo,
                "descuento": ", ".join(montos),
                "vigencia":  ", ".join(dias),
                "fuente": url
            })
    return rows

def parse_petrobras_rut(html, url):
    soup = BeautifulSoup(html, "lxml")
    rows = []
    for h in soup.select("h3, h4"):
        titulo = clean(h.get_text())
        if not titulo:
            continue
        blk = []
        for sib in h.find_all_next(["p","li","ul"], limit=10):
            if sib.name in ("h3","h4"):
                break
            blk.append(clean(sib.get_text()))
        text = " ".join(blk)
        if not text: 
            continue
        montos = extract_amounts(text)
        dias   = extract_days(text)
        if montos or "descuento" in text.lower():
            rows.append({
                "titulo": titulo,
                "banco":  "RUT/Convenio",
                "descuento": ", ".join(montos),
                "vigencia":  ", ".join(dias),
                "fuente": url
            })
    return rows

def route_parser(url, html):
    host = urlparse(url).netloc
    if "petrobrasdistribucion.cl" in host and "medios-de-pago" in url:
        return parse_petrobras_medios(html, url)
    if "petrobrasdistribucion.cl" in host and "descuento-con-tu-rut" in url:
        return parse_petrobras_rut(html, url)
    # fallback genérico
    soup = BeautifulSoup(html, "lxml")
    rows = []
    for node in soup.select("article, .card, .promo, .beneficio, section, li"):
        text = clean(node.get_text(" "))
        if not text or "descuento" not in text.lower():
            continue
        h = node.find(["h2","h3","h4"])
        titulo = clean(h.get_text()) if h else text[:120]
        montos = extract_amounts(text)
        dias   = extract_days(text)
        rows.append({
            "titulo": titulo,
            "banco":  "",
            "descuento": ", ".join(montos),
            "vigencia":  ", ".join(dias),
            "fuente": url
        })
    return rows

def main():
    ensure_outdir()
    all_rows = []
    for u in URLS:
        try:
            html = fetch(u)
            rows = route_parser(u, html)
            all_rows.extend(rows)
            print(f"[OK] {u} -> {len(rows)} filas")
            time.sleep(random.uniform(0.7,1.4))
        except Exception as e:
            print(f"[ERR] {u}: {e}")

    # dedup
    seen, dedup = set(), []
    for r in all_rows:
        key = (r["fuente"], r["titulo"], r["banco"], r["descuento"], r["vigencia"])
        if key not in seen:
            seen.add(key); dedup.append(r)

    with open(JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(dedup, f, ensure_ascii=False, indent=2)

    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["titulo","banco","descuento","vigencia","fuente"])
        w.writeheader(); w.writerows(dedup)

    print(f"JSON → {JSON_PATH}")
    print(f"CSV  → {CSV_PATH}")
    print(f"Total filas: {len(dedup)}")

if __name__ == "__main__":
    main()
