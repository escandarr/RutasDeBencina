# scraping_promos_dinamico.py
# -*- coding: utf-8 -*-
"""
PÁGINAS DINÁMICAS SUGERIDAS (requieren JS):
- Copec (beneficios Copec Pay / App Copec): https://www.appcopec.cl/promo-y-beneficios/
- Shell MiCopiloto (oficial): https://www.shell.cl/estaciones-de-servicio/micopiloto.html
- Medios/portales que suelen ser dinámicos (útiles como respaldo):
  * La Tercera (descuentos del mes)
  * Chócale (guía mensual)
  * El Dínamo, Biobio, Meganoticias, etc.

Requisitos:
    pip install selenium webdriver-manager beautifulsoup4 lxml
(En Linux podría requerir: apt-get install -y chromium-browser chromium-chromedriver)
"""

import csv
import re
import time
import random

from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from webdriver_manager.chrome import ChromeDriverManager


# --------- Config ----------
HEADLESS = True
WAIT_SEC = 8      # espera base para que cargue el JS
WAIT_MAX = 15     # espera explícita con WebDriverWait
URLS = [
    "https://www.appcopec.cl/promo-y-beneficios/",
    "https://www.shell.cl/estaciones-de-servicio/micopiloto.html",
    # agrega aquí notas de prensa del mes si quieres consolidar (ojo con paywalls)
    # "https://www.latercera.com/servicios/noticia/estos-son-los-descuentos-en-bencina-de-hasta-300-por-litro-disponibles-en-septiembre/",
]
# ---------------------------


def build_driver():
    chrome_options = Options()
    if HEADLESS:
        chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1280,2200")
    chrome_options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36 PromoScraper/1.0"
    )

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.set_page_load_timeout(45)
    return driver


def clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def try_close_cookies(driver):
    """Heurísticas para cerrar banners de cookies comunes."""
    selectors = [
        "#onetrust-accept-btn-handler",                   # OneTrust
        "button[aria-label='Aceptar']",
        "button[aria-label='Accept']",
        "button:contains('Aceptar')",
        "button:contains('Aceptar todo')",
        ".ot-sdk-container #onetrust-accept-btn-handler",
        ".accept", ".btn-accept", ".cookie-accept", ".cookie__accept",
    ]
    for sel in selectors:
        try:
            el = driver.find_element(By.CSS_SELECTOR, sel)
            el.click()
            time.sleep(0.8)
            return True
        except Exception:
            pass
    return False


def wait_dom_ready(driver, timeout=WAIT_MAX):
    """Espera a que el documento esté interactivo/complete y haya algo de contenido."""
    try:
        WebDriverWait(driver, timeout).until(
            lambda d: d.execute_script("return document.readyState") in ("interactive", "complete")
        )
    except Exception:
        pass
    # pequeña espera adicional para pintar zonas con JS
    time.sleep(1.0)


def parse_generic_promos(html: str, url: str):
    """
    Fallback genérico: busca bloques con 'descuento' y extrae monto/día si están.
    Sirve para AppCopec y páginas de beneficios donde el markup cambia.
    """
    soup = BeautifulSoup(html, "lxml")
    rows = []

    # intenta con tarjetas comunes
    candidates = soup.select("article, .card, .beneficio, .promo, .promotion, section, li")
    if not candidates:
        candidates = soup.find_all(True, recursive=True)

    for node in candidates:
        text = clean(node.get_text(" "))
        if not text or "descuento" not in text.lower():
            continue

        # título cercano
        h = node.find(["h2", "h3", "h4"])
        titulo = clean(h.get_text()) if h else text[:120]

        m_desc = re.search(r"(\$ ?\d{1,3}(?:\.\d{3})?)\s*(?:de)?\s*descuento\s*por\s*litro", text, re.I)
        if not m_desc:
            # algunos dicen "$100 por litro"
            m_desc = re.search(r"(\$ ?\d{1,3}(?:\.\d{3})?)\s*por\s*litro", text, re.I)

        m_day = re.search(
            r"\b(lunes|martes|miércoles|miercoles|jueves|viernes|sábado|sabado|domingo)s?\b", text, re.I
        )

        # Banco/entidad: buscar marcas típicas
        m_bank = re.search(
            r"\b(Scotiabank|Cencosud|BCI|Banco\s*Internacional|Mastercard|Visa|Tenpo|Dale|Coopeuch|"
            r"Lider\s*BCI|Mercado Pago|Consorcio|Ripley|ABC|SBPay|Spin)\b",
            text, re.I
        )

        if m_desc or m_bank:
            rows.append({
                "titulo": titulo,
                "banco": clean(m_bank.group(1)) if m_bank else "",
                "descuento": clean(m_desc.group(1)) if m_desc else "",
                "vigencia": m_day.group(1).capitalize() if m_day else "",
                "fuente": url
            })
    return rows


def scrape_dynamic(driver, url: str):
    driver.get(url)
    wait_dom_ready(driver)
    try_close_cookies(driver)

    # espera adicional a que aparezca algo de contenido típico
    try:
        WebDriverWait(driver, WAIT_MAX).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "article, .card, .beneficio, .promo, .promotion, section, li, h2, h3")
            )
        )
    except Exception:
        pass

    # tiempo base para que terminen requests XHR
    time.sleep(WAIT_SEC)

    html = driver.page_source
    return parse_generic_promos(html, url)


def main():
    driver = build_driver()
    all_rows = []
    try:
        for u in URLS:
            try:
                rows = scrape_dynamic(driver, u)
                all_rows.extend(rows)
                print(f"[OK] {u} -> {len(rows)} filas")
                time.sleep(random.uniform(0.8, 1.5))
            except Exception as e:
                print(f"[ERR] {u}: {e}")
    finally:
        driver.quit()

    # desduplicar por (fuente, titulo, banco, descuento, vigencia)
    seen = set()
    dedup = []
    for r in all_rows:
        key = (r["fuente"], r["titulo"], r["banco"], r["descuento"], r["vigencia"])
        if key not in seen:
            seen.add(key)
            dedup.append(r)

    with open("promos_estaciones_dinamico.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["titulo", "banco", "descuento", "vigencia", "fuente"])
        w.writeheader()
        w.writerows(dedup)

    print(f"Total filas (dedup): {len(dedup)}")
    for r in dedup[:5]:
        print(r)
    print("CSV generado: promos_estaciones_dinamico.csv")


if __name__ == "__main__":
    main()
