# Metadata/extract_cne.py
# -----------------------------------------------------------
# Extracción CNE con auto-renovación de token (login si expira)
# Salidas: cne.json (todo) y cne_sample.json (primeras 5)
# -----------------------------------------------------------

import os
import sys
import json
import pathlib
from datetime import datetime

import requests

BASE = pathlib.Path(__file__).resolve().parent
OUT_JSON = BASE / "cne.json"
OUT_SAMPLE = BASE / "cne_sample.json"
TOKEN_FILE_CANDIDATES = [
    BASE.parent / "token.txt",  # raíz del repo
    BASE / "token.txt",         # dentro de Metadata
    pathlib.Path("token.txt"),  # cwd
]

CNE_LOGIN_URL = "https://api.cne.cl/api/login"
CNE_ESTACIONES_URL = "https://api.cne.cl/api/v4/estaciones"


# ------------------------------
# Utilidades de token
# ------------------------------
def load_token():
    """Primero ENV CNE_TOKEN. Si no, busca token.txt en rutas conocidas."""
    token = os.environ.get("CNE_TOKEN")
    if token:
        return token.strip()
    for p in TOKEN_FILE_CANDIDATES:
        if p.exists():
            t = p.read_text(encoding="utf-8").strip()
            if t:
                return t
    return None


def save_token(token):
    """Guarda el token en el primer token.txt disponible (o en Metadata)."""
    if not token:
        return
    # prioriza la raíz del repo
    for p in TOKEN_FILE_CANDIDATES:
        try:
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(token, encoding="utf-8")
            print(f"💾 Token guardado en: {p}")
            return
        except Exception as e:
            # intenta siguiente candidato
            continue
    print("⚠️ No se pudo guardar token en token.txt (sin permisos o ruta inexistente).")


def login_cne():
    """Hace login con ENV CNE_EMAIL y CNE_PASSWORD. Devuelve token o None."""
    email = os.environ.get("CNE_EMAIL")
    password = os.environ.get("CNE_PASSWORD")
    if not email or not password:
        print("⚠️ Falta CNE_EMAIL o CNE_PASSWORD en variables de entorno; no puedo reloguear.")
        return None

    try:
        r = requests.post(
            CNE_LOGIN_URL,
            json={"email": email, "password": password},
            timeout=30,
            headers={"Content-Type": "application/json", "Accept": "application/json"},
        )
    except requests.RequestException as e:
        print(f"❌ Error de red al hacer login CNE: {e}")
        return None

    if r.status_code != 200:
        print(f"❌ Login CNE falló. Status {r.status_code}: {r.text[:300]}")
        return None

    try:
        j = r.json()
    except Exception:
        print("❌ Respuesta de login no es JSON.")
        return None

    token = j.get("token")
    if not token:
        print(f"❌ Login CNE sin 'token' en respuesta: {j}")
        return None
    return token


# ------------------------------
# Normalización de estaciones
# ------------------------------
def normalize(e):
    ubic = e.get("ubicacion") or {}
    precios = e.get("precios") or {}

    def get(d, *keys):
        for k in keys:
            if isinstance(d, dict) and k in d and d[k] not in (None, ""):
                return d[k]
        return None

    def to_float(x):
        try:
            return float(x)
        except Exception:
            return None

    lat = to_float(get(ubic, "latitud", "lat", "latitude", "y"))
    lon = to_float(get(ubic, "longitud", "lng", "long", "longitude", "x"))

    return {
        "station_id": get(e, "codigo", "id", "station_id"),
        "name": (get(e, "nombre", "razon_social", "razonsocial", "nombre_fantasia", "empresa") or "").strip(),
        "brand": (get(e.get("distribuidor") or {}, "marca") or None),
        "address": get(ubic, "direccion", "address", "calle"),
        "region_code": get(ubic, "codigo_region"),
        "region_name": get(ubic, "nombre_region"),
        "commune_code": get(ubic, "codigo_comuna"),
        "commune_name": get(ubic, "nombre_comuna"),
        "lat": lat,
        "lon": lon,
        "prices": {
            "93":  (precios.get("93")  or {}).get("precio"),
            "95":  (precios.get("95")  or {}).get("precio"),
            "97":  (precios.get("97")  or {}).get("precio"),
            "DI":  (precios.get("DI")  or {}).get("precio"),
            "GLP": (precios.get("GLP") or {}).get("precio"),
        },
        "fetched_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }


def first_list_of_dicts(obj):
    """Busca recursivamente la primera lista de diccionarios en un JSON arbitrario."""
    if isinstance(obj, list):
        if len(obj) == 0 or isinstance(obj[0], dict):
            return obj
        return None
    if isinstance(obj, dict):
        for v in obj.values():
            res = first_list_of_dicts(v)
            if res is not None:
                return res
    return None


# ------------------------------
# Fetch con manejo de expiración
# ------------------------------
def fetch_estaciones_with_token(token):
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "User-Agent": "RutasDeBencina/1.0",
    }
    r = requests.get(CNE_ESTACIONES_URL, headers=headers, timeout=60)
    return r


def parse_estaciones(data):
    """Intenta obtener la lista de estaciones cualquiera sea la estructura."""
    estaciones = None
    if isinstance(data, list):
        estaciones = data
    elif isinstance(data, dict):
        # pruebas comunes
        for k in ["data", "results", "estaciones", "items", "result"]:
            v = data.get(k)
            if isinstance(v, list):
                estaciones = v
                break
            if isinstance(v, dict):
                for kk in ["estaciones", "items", "results"]:
                    inner = v.get(kk)
                    if isinstance(inner, list):
                        estaciones = inner
                        break
            if estaciones:
                break

    if estaciones is None:
        estaciones = first_list_of_dicts(data)

    return estaciones or []


def main():
    # 1) Cargar token inicial si existe
    token = load_token()

    # 2) Primer intento
    print(f"🔄 Llamando API CNE… {CNE_ESTACIONES_URL}")
    try:
        r = fetch_estaciones_with_token(token) if token else None
    except requests.RequestException as e:
        print(f"❌ Error de red: {e}")
        sys.exit(1)

    need_login = False
    resp_data = None

    if r is None:
        need_login = True
    else:
        print("Status:", r.status_code)
        if r.status_code == 200:
            # revisar si contenido dice "Token is Expired"
            ctype = r.headers.get("Content-Type", "")
            if "application/json" in ctype.lower():
                try:
                    j = r.json()
                    if isinstance(j, dict) and j.get("status") == "Token is Expired":
                        need_login = True
                    else:
                        resp_data = j
                except Exception:
                    print("⚠️ Respuesta 200 no es JSON válido.")
                    need_login = True
            else:
                # si no es JSON, forzamos login
                need_login = True
        elif r.status_code in (401, 403):
            need_login = True
        else:
            print("❌ Error HTTP:", r.status_code, r.text[:300])
            sys.exit(1)

    # 3) Si necesitamos login, lo hacemos y reintentamos una vez
    if need_login:
        print("🔐 Token inválido/ausente/expirado. Intentando login…")
        new_token = login_cne()
        if not new_token:
            print("❌ No fue posible obtener un token nuevo (revisa CNE_EMAIL/CNE_PASSWORD).")
            sys.exit(1)
        save_token(new_token)  # persistimos para próximas ejecuciones
        try:
            r = fetch_estaciones_with_token(new_token)
        except requests.RequestException as e:
            print(f"❌ Error de red tras login: {e}")
            sys.exit(1)
        print("Status (reintento):", r.status_code)
        if r.status_code != 200:
            print("❌ Error HTTP tras login:", r.status_code, r.text[:300])
            sys.exit(1)
        try:
            resp_data = r.json()
        except Exception:
            print("❌ Respuesta tras login no es JSON.")
            sys.exit(1)

    # 4) Parsear estaciones (estructura flexible)
    estaciones = parse_estaciones(resp_data)
    if not estaciones:
        print(f"⚠️ Estructura inesperada; dump completo en {OUT_SAMPLE}")
        OUT_SAMPLE.write_text(json.dumps(resp_data, ensure_ascii=False, indent=2), encoding="utf-8")
        sys.exit(0)

    # 5) Normalizar y guardar
    out = [normalize(e) for e in estaciones]
    OUT_JSON.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    OUT_SAMPLE.write_text(json.dumps(out[:5], ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"✅ Guardado: {OUT_JSON}")
    print(f"👀 Muestra:  {OUT_SAMPLE} (5 filas)")


if __name__ == "__main__":
    main()
