#import os
#import json
#import requests

#TOKEN = os.environ.get("CNE_TOKEN", "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwOi8vYXBpLmNuZS5jbC9hcGkvbG9naW4iLCJpYXQiOjE3NTY5MjU4NzYsImV4cCI6MTc1NjkyOTQ3NiwibmJmIjoxNzU2OTI1ODc2LCJqdGkiOiJtOTdocGpXZG9GdjdCUWhpIiwic3ViIjoiMzY2MyIsInBydiI6IjIzYmQ1Yzg5NDlmNjAwYWRiMzllNzAxYzQwMDg3MmRiN2E1OTc2ZjcifQ.H-idl3Q_ixmP6Pz2z8qheBGm4dS0sRZiwvmCJZOUAOE")

#URL = "https://api.cne.cl/api/v4/estaciones"
#headers = {"Authorization": f"Bearer {TOKEN}"}

#r = requests.get(URL, headers=headers)
#print("Status:", r.status_code)

#if r.status_code != 200:
#    print(r.text)
#    raise SystemExit()

#data = r.json()

#if isinstance(data, list):
#    estaciones = data
#elif isinstance(data, dict):
#    estaciones = data.get("data") or data.get("results") or data.get("estaciones") or []
#else:
#    estaciones = []

#if not estaciones:
#    print("No hay estaciones o clave desconocida. Estructura recibida:")
#    print(json.dumps(data, indent=2, ensure_ascii=False))
#    raise SystemExit()

#def first_key(d, *keys):
#    for k in keys:
#        if k in d and d[k] not in (None, ""):
#            return d[k]
#    return None

#def get_nombre(e):
#    return first_key(e,
#        "nombre", "razon_social", "razonsocial", "nombre_fantasia",
#        "razonsocial_estacion", "empresa"
#    )

#def get_direccion(e):
#    return first_key(e, "direccion", "address", "calle")

#def get_region(e):
#    return first_key(e, "region", "region_nombre", "nombreregion", "idregion")

#def get_comuna(e):
#    return first_key(e, "comuna", "comuna_nombre", "nombrecomuna", "idcomuna")

#def get_lat(e):
#    return first_key(e, "lat", "latitud", "y", "latitude")

#def get_lng(e):
#    return first_key(e, "lng", "long", "longitud", "x", "longitude")

#def get_precios(e):
#    combustibles = e.get("combustibles") or e.get("hidrocarburos") or []
#    precios = {}
#    if isinstance(combustibles, list):
#        for c in combustibles:
#            nombre = first_key(c, "nombre", "tipo", "octanaje")
#            precio = first_key(c, "precio", "valor")
#            if nombre and precio:
#                precios[str(nombre)] = precio
#    if not precios and isinstance(e.get("precios"), dict):
#        precios = e["precios"]
#    for k, v in e.items():
#        lk = k.lower()
#        if ("93" in lk or "95" in lk or "97" in lk or "diesel" in lk) and isinstance(v, (int, float, str)):
#            precios[lk] = v
#    return precios or None

#def es_rm(e):
#    reg = get_region(e)
#    return str(reg) == "13" or (isinstance(reg, str) and "metropolitana" in reg.lower())

# estaciones = [e for e in estaciones if es_rm(e)]

#for e in estaciones[:10]:
#    nombre = get_nombre(e)
#    direccion = get_direccion(e)
#    region = get_region(e)
#    comuna = get_comuna(e)
#    lat = get_lat(e)
#    lng = get_lng(e)
#    precios = get_precios(e)
#    print("—"*60)
#    print("Nombre     :", nombre)
#    print("Dirección  :", direccion)
#    print("Región/Com :", region, "/", comuna)
#    print("Coords     :", lat, ",", lng)
#    print("Precios    :", precios)

#print("\nEjemplo JSON bruto de una estación:")
#print(json.dumps(estaciones[0], indent=2, ensure_ascii=False))

# Infraestructura/extract_infra.py
import requests, json, pathlib

BASE = pathlib.Path(__file__).resolve().parent
OUT_JSON = BASE / "infra.json"
OUT_GEOJSON = BASE / "infra.geojson"

# bbox aprox. Santiago (-33.2) a Puerto Montt (-41.5); ampliar si quieres
# formato: [lat_min, lon_min, lat_max, lon_max]
BBOX = [-41.9, -74.0, -33.0, -70.2]

QUERY = f"""
[out:json][timeout:180];
way["highway"~"motorway|trunk|primary|secondary|tertiary"]({BBOX[0]},{BBOX[1]},{BBOX[2]},{BBOX[3]});
(._;>;);
out geom;
"""

def main():
    print("🔄 Descargando red vial desde Overpass…")
    r = requests.post("https://overpass-api.de/api/interpreter", data=QUERY, timeout=300)
    r.raise_for_status()
    data = r.json()

    nodes = {}
    edges = []

    for el in data.get("elements", []):
        if el["type"] == "node":
            nodes[el["id"]] = {"id": el["id"], "lat": el["lat"], "lon": el["lon"]}
        elif el["type"] == "way":
            tags = el.get("tags", {}) or {}
            geom = el.get("geometry", []) or []
            nds = el.get("nodes", []) or []
            for i in range(len(nds) - 1):
                src, tgt = nds[i], nds[i+1]
                p1, p2 = geom[i], geom[i+1]
                edges.append({
                    "id": f"{el['id']}_{i}",
                    "source": src,
                    "target": tgt,
                    "name": tags.get("name"),
                    "highway": tags.get("highway"),
                    "oneway": tags.get("oneway", "no"),
                    "geometry": [p1, p2]
                })

    OUT_JSON.write_text(json.dumps({"nodes": list(nodes.values()), "edges": edges}, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"✅ Guardado: {OUT_JSON}")

    # GeoJSON de líneas (útil para Leaflet/visualización)
    features = []
    for e in edges:
        coords = [(p["lon"], p["lat"]) for p in e["geometry"]]
        features.append({
            "type": "Feature",
            "geometry": {"type": "LineString", "coordinates": coords},
            "properties": {
                "id": e["id"], "name": e["name"], "highway": e["highway"], "oneway": e["oneway"]
            }
        })
    gj = {"type": "FeatureCollection", "features": features}
    OUT_GEOJSON.write_text(json.dumps(gj, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"🌍 Guardado: {OUT_GEOJSON}")

if __name__ == "__main__":
    main()
