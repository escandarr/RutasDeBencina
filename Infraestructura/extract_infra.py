import os
import json
import requests

TOKEN = os.environ.get("CNE_TOKEN", "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwOi8vYXBpLmNuZS5jbC9hcGkvbG9naW4iLCJpYXQiOjE3NTY5MjU4NzYsImV4cCI6MTc1NjkyOTQ3NiwibmJmIjoxNzU2OTI1ODc2LCJqdGkiOiJtOTdocGpXZG9GdjdCUWhpIiwic3ViIjoiMzY2MyIsInBydiI6IjIzYmQ1Yzg5NDlmNjAwYWRiMzllNzAxYzQwMDg3MmRiN2E1OTc2ZjcifQ.H-idl3Q_ixmP6Pz2z8qheBGm4dS0sRZiwvmCJZOUAOE")

URL = "https://api.cne.cl/api/v4/estaciones"
headers = {"Authorization": f"Bearer {TOKEN}"}

r = requests.get(URL, headers=headers)
print("Status:", r.status_code)

if r.status_code != 200:
    print(r.text)
    raise SystemExit()

data = r.json()

# 1) Normalizamos la lista de estaciones (a veces viene como lista,
# otras como {"data": [...]}, {"results": [...]}, etc.)
if isinstance(data, list):
    estaciones = data
elif isinstance(data, dict):
    estaciones = data.get("data") or data.get("results") or data.get("estaciones") or []
else:
    estaciones = []

# Si no sabemos la estructura, mostramos un ejemplo bien formateado:
if not estaciones:
    print("No hay estaciones o clave desconocida. Estructura recibida:")
    print(json.dumps(data, indent=2, ensure_ascii=False))
    raise SystemExit()

# --- Helpers para campos con nombres variables ---
def first_key(d, *keys):
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return d[k]
    return None

def get_nombre(e):
    return first_key(e,
        "nombre", "razon_social", "razonsocial", "nombre_fantasia",
        "razonsocial_estacion", "empresa"
    )

def get_direccion(e):
    return first_key(e, "direccion", "address", "calle")

def get_region(e):
    return first_key(e, "region", "region_nombre", "nombreregion", "idregion")

def get_comuna(e):
    return first_key(e, "comuna", "comuna_nombre", "nombrecomuna", "idcomuna")

def get_lat(e):
    return first_key(e, "lat", "latitud", "y", "latitude")

def get_lng(e):
    return first_key(e, "lng", "long", "longitud", "x", "longitude")

def get_precios(e):
    """
    Algunos esquemas traen:
      - 'combustibles': [ { 'nombre': '93', 'precio': 1299 }, ... ]
      - 'precios': { '93': 1299, '95': 1349, ... }
      - o campos planos tipo 'precio93', 'precio95', etc.
    """
    # 1) lista de combustibles
    combustibles = e.get("combustibles") or e.get("hidrocarburos") or []
    precios = {}
    if isinstance(combustibles, list):
        for c in combustibles:
            nombre = first_key(c, "nombre", "tipo", "octanaje")
            precio = first_key(c, "precio", "valor")
            if nombre and precio:
                precios[str(nombre)] = precio

    # 2) dict de precios
    if not precios and isinstance(e.get("precios"), dict):
        precios = e["precios"]

    # 3) campos planos
    for k, v in e.items():
        lk = k.lower()
        if ("93" in lk or "95" in lk or "97" in lk or "diesel" in lk) and isinstance(v, (int, float, str)):
            precios[lk] = v

    return precios or None

# 2) (Opcional) Filtrar por Región Metropolitana (13)
def es_rm(e):
    reg = get_region(e)
    return str(reg) == "13" or (isinstance(reg, str) and "metropolitana" in reg.lower())

# Quita el comentario de la línea siguiente si quieres filtrar solo RM:
# estaciones = [e for e in estaciones if es_rm(e)]

# 3) Mostrar las primeras 10 estaciones con campos limpios
for e in estaciones[:10]:
    nombre = get_nombre(e)
    direccion = get_direccion(e)
    region = get_region(e)
    comuna = get_comuna(e)
    lat = get_lat(e)
    lng = get_lng(e)
    precios = get_precios(e)

    print("—"*60)
    print("Nombre     :", nombre)
    print("Dirección  :", direccion)
    print("Región/Com :", region, "/", comuna)
    print("Coords     :", lat, ",", lng)
    print("Precios    :", precios)

# Si necesitas ver exactamente cómo viene el primero:
print("\nEjemplo JSON bruto de una estación:")
print(json.dumps(estaciones[0], indent=2, ensure_ascii=False))
