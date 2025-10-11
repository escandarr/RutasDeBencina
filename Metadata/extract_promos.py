# scraping_promos_dinamico_meta.py
# -*- coding: utf-8 -*-
"""
Extrae promociones desde páginas DINÁMICAS y genera:
 - data/metadata/promos_dinamico.json
 - data/metadata/promos_dinamico.csv

Requisitos:
  pip install selenium webdriver-manager beautifulsoup4 lxml
"""

import os, csv, json, re, time, random
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

HEADLESS = True
WAIT_SEC = 8
WAIT_MAX = 15

# === URLs DINÁMICAS sugeridas ===
URLS = [
    "https://www.appcopec.cl/promo-y-beneficios/",
    "https://www.shell.cl/estaciones-de-servicio/micopiloto.html",
    # Puedes añadir portales de prensa si necesitas (ojo con paywalls/cookies).
]

DAYS_MAP = {
    "lunes": "Lunes", "martes": "Martes", "miercoles": "Miércoles", "miércoles": "Miércoles",
    "jueves": "Jueves", "viernes": "Viernes", "sabado": "Sábado", "sábado": "Sábado", "domingo": "Domingo"
}

OUT_DIR = os.path.join("salidas promos", "metadata")
JSON_PATH = os.path.join(OUT_DIR, "promos_dinamico.json")
CSV_PATH  = os.path.join(OUT_DIR, "promos_dinamico.csv")

def ensure_outdir():
    os.makedirs(OUT_DIR, exist_ok=True)

def build_driver():
    opts = Options()
    if HEADLESS:
        opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1280,2200")
    opts.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/124.0 Safari/537.36 RutasDeBencina/1.0")
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=opts)
    driver.set_page_load_timeout(45)
    return driver

def clean(s: str) -> str:
    import re
    return re.sub(r"\s+", " ", (s or "")).strip()

def try_close_cookies(driver):
    sels = [
        "#onetrust-accept-btn-handler", "button[aria-label='Aceptar']",
        "button[aria-label='Accept']", ".accept", ".btn-accept", ".cookie-accept", ".cookie__accept"
    ]
    for sel in sels:
        try:
            driver.find_element(By.CSS_SELECTOR, sel).click()
            time.sleep(0.6)
            return
        except Exception:
            pass

def wait_dom_ready(driver, timeout=WAIT_MAX):
    try:
        WebDriverWait(driver, timeout).until(
            lambda d: d.execute_script("return document.readyState") in ("interactive", "complete")
        )
    except Exception:
        pass
    time.sleep(1.0)

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
    found = [clean(m) for m in re.findall(r"\$ ?\d{1,3}(?:\.\d{3})?", text)]
    seen, out = set(), []
    for a in found:
        if a not in seen:
            seen.add(a); out.append(a)
    return out

def parse_generic_promos(html: str, url: str):
    soup = BeautifulSoup(html, "lxml")
    rows = []

    # tarjetas comunes
    nodes = soup.select("article, .card, .beneficio, .promo, .promotion, section, li, .tile, .item")
    if not nodes:
        nodes = soup.find_all(True, recursive=True)

    for node in nodes:
        text = clean(node.get_text(" "))
        if not text or "descuento" not in text.lower():
            continue

        h = node.find(["h2","h3","h4"])
        titulo = clean(h.get_text()) if h else text[:120]

        m_bank = re.search(
            r"\b(Scotiabank|Cencosud|BCI|Banco\s*Internacional|Mastercard|Visa|Tenpo|Dale|Coopeuch|"
            r"Lider\s*BCI|Mercado Pago|Consorcio|Ripley|ABC|SBPay|Spin|Hites|La\s*Polar|Santander)\b",
            text, re.I
        )
        banco = clean(m_bank.group(1)) if m_bank else titulo  # fallback

        montos = extract_amounts(text)
        dias   = extract_days(text)

        if montos or "descuento" in text.lower():
            rows.append({
                "titulo": titulo,
                "banco":  banco,
                "descuento": ", ".join(montos),
                "vigencia":  ", ".join(dias),
                "fuente": url
            })
    return rows

def scrape_dynamic(driver, url: str):
    driver.get(url)
    wait_dom_ready(driver)
    try_close_cookies(driver)
    try:
        WebDriverWait(driver, WAIT_MAX).until(
            EC.presence_of_all_elements_located(
                (By.CSS_SELECTOR, "article, .card, .beneficio, .promo, .promotion, section, li, h2, h3")
            )
        )
    except Exception:
        pass
    time.sleep(WAIT_SEC)
    return parse_generic_promos(driver.page_source, url)

def main():
    ensure_outdir()
    driver = build_driver()
    all_rows = []
    try:
        for u in URLS:
            try:
                rows = scrape_dynamic(driver, u)
                all_rows.extend(rows)
                print(f"[OK] {u} -> {len(rows)} filas")
                time.sleep(random.uniform(0.7,1.3))
            except Exception as e:
                print(f"[ERR] {u}: {e}")
    finally:
        driver.quit()

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
