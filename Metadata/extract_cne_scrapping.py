#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json, os, re, sys
from typing import Dict, List
import pandas as pd
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

URL = "https://www.bencinaenlinea.cl/#/reporte_comunal"
CSV_OUT = "bencineras_scrape.csv"
JSON_OUT = "bencineras_scrape.json"

TABS = ["Gasolina 93", "Gasolina 95", "Gasolina 97"]
TARGETS = [
    ("Metropolitana", "Metropolitana de Santiago"),
    ("Zona Sur",      "De la Araucanía"),
]

def clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def to_float(x: str):
    if not x: return None
    x = x.replace(".", "").replace(",", ".")
    m = re.search(r"[-+]?\d*\.?\d+", x)
    return float(m.group(0)) if m else None

def wait_settled(page):
    page.wait_for_load_state("domcontentloaded")
    try: page.wait_for_load_state("networkidle", timeout=8000)
    except PWTimeout: pass
    page.wait_for_timeout(350)

def open_tab(page, label: str):
    print(f"[INFO] Tab -> {label}")
    # varios selectores posibles para el tab
    selectors = [
        f"a:has-text('{label}')",
        f"button:has-text('{label}')",
        f"[role='tab']:has-text('{label}')",
        f"[role='link']:has-text('{label}')",
        f"text={label}",
        # algunos usan data-target con texto
        f"[data-tab*='{label.split()[-1]}']",
    ]
    clicked = False
    for sel in selectors:
        loc = page.locator(sel).first
        if loc.count():
            try:
                loc.scroll_into_view_if_needed()
                loc.click(timeout=2500)
                clicked = True
                break
            except Exception:
                # intento por JS (último recurso)
                try:
                    page.evaluate("(el)=>el.click()", loc)
                    clicked = True
                    break
                except Exception:
                    continue
    if not clicked:
        # como último recurso: clic en el contenedor de pestañas por texto parcial (93/95/97)
        num = re.findall(r"\d+", label)
        if num:
            page.get_by_text(num[0], exact=False).first.click()
            clicked = True
    wait_settled(page)
    # confirmar que cambió el header grande “Gasolina XX”
    try:
        page.get_by_role("heading", name=re.compile(label, re.I)).first.wait_for(timeout=3000)
    except Exception:
        # no es crítico, seguimos
        pass

def expand_section(page, label):
    print(f"[INFO] Sección -> {label}")
    for sel in [
        f"button:has-text('{label}')",
        f"h2:has-text('{label}')",
        f"h3:has-text('{label}')",
        f"div[role='button']:has-text('{label}')",
        f"text={label}",
    ]:
        loc = page.locator(sel).first
        if loc.count():
            try:
                loc.scroll_into_view_if_needed()
                loc.click()
                page.wait_for_timeout(250)
                return True
            except Exception:
                continue
    return False

def click_region_item(page, region_text):
    print(f"[INFO] Región -> {region_text}")
    candidates = [
        ("link", True),
        ("button", True),
        ("*", False),
    ]
    for where, use_role in candidates:
        try:
            if use_role:
                el = page.get_by_role(where, name=re.compile(region_text, re.I)).first
            else:
                el = page.get_by_text(region_text, exact=False).first
            el.scroll_into_view_if_needed()
            el.click()
            wait_settled(page)
            return True
        except Exception:
            continue
    return False

def locate_table(page):
    tables = page.locator("table")
    for i in range(tables.count()):
        tbl = tables.nth(i)
        try:
            tbl.wait_for(state="visible", timeout=2500)
        except PWTimeout:
            continue
        heads = [clean(th.inner_text()) for th in tbl.locator("thead tr th").all()]
        if not heads: 
            continue
        ok = any("Marca" in h for h in heads) and \
             any(("Dirección" in h) or ("Direccion" in h) for h in heads) and \
             any("Comuna" in h for h in heads) and \
             any(("Atención" in h) or ("Atencion" in h) for h in heads) and \
             any("Precio" in h for h in heads)
        if ok:
            return tbl
    return None

def parse_table(page, gas, zona, region) -> List[Dict]:
    tbl = locate_table(page)
    if not tbl:
        raise RuntimeError("Tabla no encontrada")

    heads = [clean(th.inner_text()) for th in tbl.locator("thead tr th").all()]

    def idx_like(name):
        for i, h in enumerate(heads):
            if name.lower() in h.lower():
                return i
        return None

    i_marca = idx_like("marca")
    i_dir   = idx_like("dirección") if idx_like("dirección") is not None else idx_like("direccion")
    i_com   = idx_like("comuna")
    i_aten  = idx_like("atención") if idx_like("atención") is not None else idx_like("atencion")
    i_prec  = idx_like("precio")

    rows = []
    trs = tbl.locator("tbody tr")
    for r in range(trs.count()):
        tds = trs.nth(r).locator("td")
        cells = [clean(td.inner_text()) for td in tds.all()]
        rows.append({
            "zona": zona,
            "region": region,
            "gasolina": gas,
            "marca": cells[i_marca] if i_marca is not None else None,
            "direccion": cells[i_dir] if i_dir is not None else None,
            "comuna": cells[i_com] if i_com is not None else None,
            "atencion": cells[i_aten] if i_aten is not None else None,
            "precio_por_litro": to_float(cells[i_prec] if i_prec is not None else None),
            "precio_raw": cells[i_prec] if i_prec is not None else None,
        })
    return rows

def paginate_all(page, gas, zona, region) -> List[Dict]:
    """
    Recorre todas las páginas de la tabla, incluyendo bloques con « 1 2 3 4 5 »
    Estrategia:
      - Parsear la tabla de la página actual
      - Click en cada número visible (1..N) en orden
      - Luego click en » para avanzar de bloque y repetir
      - Termina cuando ya no aparecen firmas nuevas de página
    """
    resultados: List[Dict] = []
    firmas_vistas = set()

    def firma_pagina() -> str:
        # Usamos un hash simple con primera y última fila visibles + total filas
        tbl = locate_table(page)
        if not tbl:
            return "no-table"
        trs = tbl.locator("tbody tr")
        n = trs.count()
        if n == 0:
            return "vacia"
        def row_sig(idx):
            tds = trs.nth(idx).locator("td")
            return "|".join((tds.nth(j).inner_text().strip() for j in range(min(3, tds.count()))))
        first = row_sig(0)
        last  = row_sig(n-1)
        return f"{n}:{first}::{last}"

    def parse_and_store():
        filas = parse_table(page, gas, zona, region)
        resultados.extend(filas)

    while True:
        # Asegura que registramos la página actual (por si llegamos desde la región)
        sig = firma_pagina()
        if sig not in firmas_vistas:
            firmas_vistas.add(sig)
            parse_and_store()

        # Recorre números visibles de izquierda a derecha
        nums = page.locator("a,button").filter(has_text=re.compile(r"^\d+$"))
        count_nums = nums.count()
        # Si no hay números, no hay paginación
        if count_nums == 0:
            break

        avanzamos_en_bloque = False
        for i in range(count_nums):
            btn = nums.nth(i)
            try:
                txt = btn.inner_text().strip()
            except Exception:
                continue
            if not re.fullmatch(r"\d+", txt):
                continue
            try:
                btn.scroll_into_view_if_needed()
                btn.click()
                wait_settled(page)
                nsig = firma_pagina()
                if nsig not in firmas_vistas:
                    firmas_vistas.add(nsig)
                    parse_and_store()
                    avanzamos_en_bloque = True
            except Exception:
                continue

        # Intentar pasar al siguiente bloque con »
        next_btn = page.locator("a,button").filter(has_text=re.compile(r"[»]|>>")).last
        try:
            if next_btn.count():
                next_btn.scroll_into_view_if_needed()
                next_btn.click()
                wait_settled(page)
                nsig = firma_pagina()
                # si la firma no cambia, ya no hay más bloques
                if nsig in firmas_vistas:
                    break
                firmas_vistas.add(nsig)
                parse_and_store()
                # seguir con el nuevo bloque
                continue
            else:
                # no existe » -> si no avanzamos dentro del bloque, terminamos
                if not avanzamos_en_bloque:
                    break
        except Exception:
            # si falla el click en » y tampoco avanzamos dentro del bloque, salimos
            if not avanzamos_en_bloque:
                break

    return resultados


def main():
    headless = os.getenv("HEADFUL", "") == ""  # export HEADFUL=1 para ver el navegador
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        ctx = browser.new_context()
        page = ctx.new_page()

        print("[INFO] Cargando página…")
        page.goto(URL, wait_until="domcontentloaded")
        wait_settled(page)

        all_rows: List[Dict] = []

        for gas in TABS:
            open_tab(page, gas)

            for zona, region_text in TARGETS:
                expand_section(page, zona)
                if not click_region_item(page, region_text):
                    print(f"[WARN] No pude abrir la región '{region_text}' dentro de '{zona}'")
                    continue

                # pulsa "Consultar" si existe
                try:
                    page.get_by_role("button", name=re.compile("Consultar", re.I)).first.click()
                    wait_settled(page)
                except Exception:
                    pass

                try:
                    rows = paginate_all(page, gas, zona, region_text)
                    print(f"[INFO] {gas} / {zona} / {region_text}: {len(rows)} filas")
                    all_rows.extend(rows)
                except Exception as e:
                    print(f"[WARN] {gas} / {zona} / {region_text}: {e}")
                    try:
                        page.screenshot(path="scrape_debug.png", full_page=True)
                        print("[INFO] Screenshot -> scrape_debug.png")
                    except Exception:
                        pass

        browser.close()

    all_rows = [r for r in all_rows if r.get("direccion") or r.get("marca") or r.get("precio_por_litro")]

    if not all_rows:
        print("No se extrajo ninguna fila. Ejecuta con HEADFUL=1 para ver el flujo y comparte el scrape_debug.png.")
        sys.exit(2)

    df = pd.DataFrame(all_rows)
    cols = ["zona","region","gasolina","marca","direccion","comuna","atencion","precio_por_litro","precio_raw"]
    for c in cols:
        if c not in df.columns: df[c] = None
    df = df[cols]

    df.to_csv(CSV_OUT, index=False, encoding="utf-8-sig")
    with open(JSON_OUT, "w", encoding="utf-8") as f:
        json.dump(df.to_dict(orient="records"), f, ensure_ascii=False, indent=2)

    print(f"[OK] {len(df)} filas -> {CSV_OUT} y {JSON_OUT}")

if __name__ == "__main__":
    main()
