# scraping_promos_estatico.py
# -*- coding: utf-8 -*-
"""
PÁGINAS ESTÁTICAS SUGERIDAS (HTML directo; sirven con requests+BS):
- Petrobras (medios de pago): https://www.petrobrasdistribucion.cl/promociones/descuento-con-medios-de-pago/
- Petrobras (descuento con tu RUT): https://www.petrobrasdistribucion.cl/promociones/descuento-con-tu-rut/
- Aramco (alianzas y beneficios): https://www.aramcoestaciones.cl/alianzas-y-beneficios

Si agregas más, procura que el contenido esté en el HTML inicial (sin JS).
"""

import csv, re, time, random
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/124.0 Safari/537.36 PromoScraper/1.0"
}

URLS = [
    "https://www.petrobrasdistribucion.cl/promociones/descuento-con-medios-de-pago/",
    "https://www.petrobrasdistribucion.cl/promociones/descuento-con-tu-rut/",
    "https://www.aramcoestaciones.cl/alianzas-y-beneficios",
]

def fetch(url, timeout=30):
    r = requests.get(url, headers=HEADERS, timeout=timeout)
    r.raise_for_status()
    return r.text

def clean(text):
    return re.sub(r"\s+", " ", (text or "")).strip()

def parse_petrobras_medios(html, url):
    """
    Extrae tarjetas tipo:
      #### Banco Consorcio
      ... '$150 de descuento por litro' ... 'Lunes'
    """
    soup = BeautifulSoup(html, "lxml")
    rows = []
    # Cada bloque de promo está bajo headings tipo h4/h3 (#### en markdown)
    for h in soup.select("h4, h3"):
        title = clean(h.get_text())
        if not title:
            continue
        # Buscar bloque de descripción inmediato
        blk = []
        for sib in h.find_all_next(["p","li","ul"], limit=6):
            # cortamos si encontramos otro h3/h4
            if sib.name in ("h3","h4"):
                break
            blk.append(clean(sib.get_text()))
        text = " ".join(blk)

        # heurísticas simples
        m_desc = re.search(r"(\$ ?\d{1,3}(?:\.\d{3})?)\s*de descuento por litro", text, re.I)
        m_day  = re.search(r"(lunes|martes|miércoles|miercoles|jueves|viernes|sábado|sabado|domingo)s?", text, re.I)

        if m_desc or "descuento" in text.lower():
            rows.append({
                "titulo": title,
                "banco": title,                     # en Petrobras el h4 suele ser el banco/entidad
                "descuento": m_desc.group(1) if m_desc else "",
                "vigencia": m_day.group(1).capitalize() if m_day else "",
                "fuente": url
            })
    return rows

def parse_petrobras_rut(html, url):
    soup = BeautifulSoup(html, "lxml")
    rows = []
    for h in soup.select("h4, h3"):
        titulo = clean(h.get_text())
        txt = []
        for sib in h.find_all_next(["p","li","ul"], limit=6):
            if sib.name in ("h3","h4"):
                break
            txt.append(clean(sib.get_text()))
        text = " ".join(txt)
        if any(k in text.lower() for k in ["descuento", "parafina", "combustible", "rut"]):
            # Captura montos tipo $15 o $40
            m_desc = re.findall(r"\$ ?\d{1,3}", text)
            rows.append({
                "titulo": titulo,
                "banco": "RUT/Convenio",
                "descuento": ", ".join(m_desc),
                "vigencia": " ".join(re.findall(r"(lunes|martes|miércoles|miercoles|jueves|viernes|sábado|sabado|domingo)s?", text, re.I)).capitalize(),
                "fuente": url
            })
    return rows

def parse_aramco(html, url):
    """
    En Aramco hay secciones 'Medios de pago' con bloques:
      #### Hasta $200
      Consorcio
      de descuento por litro ... Todos los Lunes
    """
    soup = BeautifulSoup(html, "lxml")
    rows = []

    # Buscamos la sección 'Medios de pago' y sus bloques cercanos
    medios_hdr = None
    for tag in soup.select("h2,h3,h4,h5"):
        if "medios de pago" in tag.get_text(strip=True).lower():
            medios_hdr = tag
            break

    start = medios_hdr if medios_hdr else soup
    # bloques: títulos h4 seguidos por el nombre del banco y la frase
    for blk in start.find_all(["h4","h5"]):
        title = clean(blk.get_text())
        if not title:
            continue
        # Tomamos vecinos próximos
        neighborhood = []
        for sib in blk.find_all_next(["p","li","div","span"], limit=8):
            if sib.name in ("h3","h4","h5"):
                break
            neighborhood.append(clean(sib.get_text()))
        text = " ".join(neighborhood)
        if not text:
            continue

        # Extraer banco/entidad (palabra capitalizada cercana al título)
        # A veces Aramco pone: "Hasta $300" (h4) y en la línea siguiente "Tenpo"
        bank = ""
        # primera línea no vacía diferente al título
        for line in neighborhood:
            if line and line != title:
                bank = line.split(" ")[0] if len(line.split()) == 1 else line.split("  ")[0]
                # si el primer renglón es claramente el banco (ej. 'Tenpo', 'Consorcio')
                if re.match(r"^[A-Za-zÁÉÍÓÚÑáéíóúÜü][\wÁÉÍÓÚÑáéíóúÜü\-\. ]{1,40}$", bank):
                    bank = line
                break

        m_desc = re.search(r"(\$ ?\d{1,3}(?:\.\d{3})?)\s*de descuento por litro", (title+" "+text), re.I)
        m_day  = re.search(r"(lunes|martes|miércoles|miercoles|jueves|viernes|sábado|sabado|domingo)s?", text, re.I)

        if m_desc:
            rows.append({
                "titulo": title,
                "banco": clean(bank),
                "descuento": m_desc.group(1),
                "vigencia": m_day.group(1).capitalize() if m_day else "",
                "fuente": url
            })
    return rows

def route_parser(url, html):
    host = urlparse(url).netloc
    if "petrobrasdistribucion.cl" in host and "medios-de-pago" in url:
        return parse_petrobras_medios(html, url)
    if "petrobrasdistribucion.cl" in host and "descuento-con-tu-rut" in url:
        return parse_petrobras_rut(html, url)
    if "aramcoestaciones.cl" in host:
        return parse_aramco(html, url)
    # fallback: buscar artículos/promos genéricos
    soup = BeautifulSoup(html, "lxml")
    rows = []
    for card in soup.select("article, .promo, .card, .beneficio, section"):
        text = clean(card.get_text(" "))
        if not text or "descuento" not in text.lower():
            continue
        m_desc = re.search(r"\$ ?\d{1,3}(?:\.\d{3})?", text)
        m_day  = re.search(r"(lunes|martes|miércoles|miercoles|jueves|viernes|sábado|sabado|domingo)s?", text, re.I)
        title  = clean((card.find(["h2","h3","h4"]) or {}).get_text(strip=True) if card.find(["h2","h3","h4"]) else "")
        rows.append({
            "titulo": title,
            "banco": "",
            "descuento": m_desc.group(0) if m_desc else "",
            "vigencia": m_day.group(1).capitalize() if m_day else "",
            "fuente": url
        })
    return rows

def main():
    all_rows = []
    for u in URLS:
        try:
            html = fetch(u)
            rows = route_parser(u, html)
            all_rows.extend(rows)
            time.sleep(random.uniform(0.8, 1.6))
            print(f"[OK] {u} -> {len(rows)} filas")
        except Exception as e:
            print(f"[ERR] {u}: {e}")

    with open("promos_estaciones.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["titulo","banco","descuento","vigencia","fuente"])
        w.writeheader(); w.writerows(all_rows)

    print(f"Total filas: {len(all_rows)}")
    for r in all_rows[:5]:
        print(r)
    print("CSV generado: promos_estaciones.csv")

if __name__ == "__main__":
    main()
