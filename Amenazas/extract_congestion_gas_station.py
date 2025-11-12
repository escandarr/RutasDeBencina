import json
import time
import os
from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.firefox import GeckoDriverManager

options = Options()
options.add_argument("--headless") 
options.add_argument("--width=1920")
options.add_argument("--height=1080")
options.set_preference("intl.accept_languages", "es-ES, es")

service = Service(GeckoDriverManager().install())
driver = webdriver.Firefox(service=service, options=options)
print("Scraping iniciado.")

os.makedirs("Amenazas/outputs", exist_ok=True)

try:
    with open("Metadata/outputs/cne/cne_estaciones_full.json", "r", encoding="utf-8") as f:
        data = json.load(f)
    print(f"JSON cargado con {len(data)} estaciones.")
except FileNotFoundError:
    print("No se encontró el archivo JSON de entrada.")
    driver.quit()
    exit()

resultados = []
estaciones_a_procesar = data[:5]

for i, estacion in enumerate(estaciones_a_procesar):
    codigo = estacion.get("codigo", "N/A") 
    marca = estacion["distribuidor"]["marca"].strip()
    direccion = estacion["ubicacion"]["direccion"]
    comuna = estacion["ubicacion"]["nombre_comuna"]
    region = estacion["ubicacion"]["nombre_region"]
    
    ubicacion_completa = f"{direccion}, {comuna}, {region}"

    query = f"{marca} {ubicacion_completa}, Chile"
    url = f"https://www.google.com/maps/search/?api=1&query=...{query.replace(' ', '+')}"

    print(f"\n({i+1}/{len(estaciones_a_procesar)}) Buscando: {query}")
    driver.get(url)

    try:
        wait_cookie = WebDriverWait(driver, 3) 
        boton_aceptar = wait_cookie.until(
            EC.element_to_be_clickable((By.XPATH, '//button[.//span[contains(text(), "Aceptar todo")]]'))
        )
        boton_aceptar.click()
        print("Banner de cookies.")
        time.sleep(1)
    except TimeoutException:
        pass 

    concurrencia_final = None 
    try:
        wait_concurrencia = WebDriverWait(driver, 10)
        concurrencia_element = wait_concurrencia.until(
            EC.visibility_of_element_located((By.CSS_SELECTOR, 'div[aria-label$="%"]'))
        )
        
        estado_crudo = concurrencia_element.get_attribute("aria-label")
        print(f"Concurrencia encontrada: {estado_crudo}")

        if '%' in estado_crudo:
            valor_numerico = float(estado_crudo.replace('%', ''))
            concurrencia_final = round(valor_numerico / 100, 3) 

    except TimeoutException:
        concurrencia_final = "Sin datos de concurrencia"
        print(f"No se encontraron datos de concurrencia.")
    except (ValueError, TypeError):
        concurrencia_final = "Error de formato"
        print(f"No se pudo convertir el valor '{estado_crudo}' a número.")
    except Exception as e:
        concurrencia_final = "Error inesperado"
        print(f"Error inesperado: {e}")

    resultados.append({
        "codigo": codigo,
        "marca": marca,
        "ubicacion_completa": ubicacion_completa,
        "concurrencia": concurrencia_final
    })

print("\nProceso finalizado.")
driver.quit()

ruta_salida = "Amenazas/outputs/resultados_concurrencia.json"
with open(ruta_salida, "w", encoding="utf-8") as f:
    json.dump(resultados, f, indent=2, ensure_ascii=False)

print(f"\nResultados guardados en '{ruta_salida}'")