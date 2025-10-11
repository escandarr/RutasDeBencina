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
