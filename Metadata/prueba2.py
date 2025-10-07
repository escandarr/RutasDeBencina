import os
import csv
import requests

# ==== Config ====
TOKEN = os.environ.get("CNE_TOKEN", "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwOi8vYXBpLmNuZS5jbC9hcGkvbG9naW4iLCJpYXQiOjE3NTY5MjU4NzYsImV4cCI6MTc1NjkyOTQ3NiwibmJmIjoxNzU2OTI1ODc2LCJqdGkiOiJtOTdocGpXZG9GdjdCUWhpIiwic3ViIjoiMzY2MyIsInBydiI6IjIzYmQ1Yzg5NDlmNjAwYWRiMzllNzAxYzQwMDg3MmRiN2E1OTc2ZjcifQ.H-idl3Q_ixmP6Pz2z8qheBGm4dS0sRZiwvmCJZOUAOE")
URL = "https://api.cne.cl/api/v4/estaciones"

# Regiones corredor Santiago (RM) → Puerto Montt:
# 13 = Metropolitana, 06 = O'Higgins, 07 = Maule, 16 = Ñuble,
# 08 = Biobío, 09 = Araucanía, 14 = Los Ríos, 10 = Los Lagos
CORREDOR = {"13","06","07","16","08","09","14","10"}
FILTRAR_CORREDOR = True  # pon False si quieres todas

headers = {"Authorization": f"Bearer {TOKEN}"}
r = requests.get(URL, headers=headers, timeout=60)
print("Status:", r.status_code)
r.raise_for_status()

data = r.json()
# API ya devuelve lista directamente
estaciones = data if isinstance(data, list) else data.get("data", [])

rows = []
for e in estaciones:
    ubic = e.get("ubicacion", {}) or {}
    precios = e.get("precios", {}) or {}

    cod_region = (ubic.get("codigo_region") or "").strip()  # p.ej. "13"
    if FILTRAR_CORREDOR and cod_region and cod_region not in CORREDOR:
        continue

    fila = {
        "codigo": e.get("codigo"),
        "razon_social": (e.get("razon_social") or "").strip(),
        "distribuidor": (e.get("distribuidor", {}) or {}).get("marca"),
        "direccion": ubic.get("direccion"),
        "nombre_region": ubic.get("nombre_region"),
        "codigo_region": cod_region,
        "nombre_comuna": ubic.get("nombre_comuna"),
        "codigo_comuna": ubic.get("codigo_comuna"),
        "latitud": ubic.get("latitud"),
        "longitud": ubic.get("longitud"),
        # precios (pueden venir como dict anidado con unidad/fecha/etc.)
        "precio_93": (precios.get("93") or {}).get("precio"),
        "precio_95": (precios.get("95") or {}).get("precio"),
        "precio_97": (precios.get("97") or {}).get("precio"),
        "precio_DI": (precios.get("DI") or {}).get("precio"),
        "fecha_act_93": (precios.get("93") or {}).get("fecha_actualizacion"),
        "fecha_act_95": (precios.get("95") or {}).get("fecha_actualizacion"),
        "fecha_act_97": (precios.get("97") or {}).get("fecha_actualizacion"),
        "fecha_act_DI": (precios.get("DI") or {}).get("fecha_actualizacion"),
    }
    rows.append(fila)

print(f"Estaciones procesadas: {len(rows)}")

# Guardar CSV
out = "estaciones_cne.csv"
campos = list(rows[0].keys()) if rows else [
    "codigo","razon_social","distribuidor","direccion","nombre_region","codigo_region",
    "nombre_comuna","codigo_comuna","latitud","longitud","precio_93","precio_95",
    "precio_97","precio_DI","fecha_act_93","fecha_act_95","fecha_act_97","fecha_act_DI"
]
with open(out, "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=campos)
    w.writeheader()
    w.writerows(rows)

# Mostrar 5 filas ejemplo
for fila in rows[:5]:
    print("-"*60)
    print(fila)
print(f"\nCSV generado: {out}")
