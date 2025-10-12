# Metadata/extract_cne_all.py
import os, csv, json, sys, time
from pathlib import Path
import requests

LOGIN_URL = "https://api.cne.cl/api/login"
EST_URL   = "https://api.cne.cl/api/v4/estaciones"
#EST_URL   = "https://api.cne.cl/api/v3/combustible/calentacion/puntosdeventa"

ROOT = Path(__file__).resolve().parent.parent.parent
OUT  = Path(__file__).resolve().parent.parent / "outputs" / "cne"
OUT.mkdir(parents=True, exist_ok=True)

TOKEN_FILE = ROOT / "token.txt"                 # guarda/lee el token aquí
CREDS_FILE = ROOT / "secrets" / "cne_credentials.json"  # opcional (si no usas variables de entorno)

# ---------- utilidades ----------
def read_json(path: Path):
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return None

def write_json(path: Path, data):
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

def normalize_price(v):
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        s = v.strip().replace(".", "").replace(",", ".")
        try:
            return float(s)
        except:
            return None
    return None

# ---------- credenciales / token ----------
def get_credentials():
    email = os.environ.get("CNE_EMAIL")
    password = os.environ.get("CNE_PASS")
    if email and password:
        return email, password
    creds = read_json(CREDS_FILE)
    if creds and "email" in creds and "password" in creds:
        return creds["email"], creds["password"]
    return None, None

def login(email: str, password: str) -> str:
    r = requests.post(LOGIN_URL, json={"email": email, "password": password}, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"Login HTTP {r.status_code}: {r.text}")
    data = r.json()
    token = data.get("token")
    if not token:
        raise RuntimeError(f"Login sin token: {data}")
    return token

def probe_token(token: str) -> bool:
    try:
        r = requests.get(EST_URL, headers={"Authorization": f"Bearer {token}"}, timeout=30)
        if r.status_code == 200:
            return True
        # algunos expirados devuelven 200 con {"status":"Token is Expired"}
        if r.status_code == 401:
            return False
        try:
            j = r.json()
            if isinstance(j, dict) and j.get("status", "").lower().startswith("token is expired"):
                return False
        except Exception:
            pass
        return r.ok
    except Exception:
        return False

def get_fresh_token() -> str:
    """
    1) Si hay token.txt y sirve -> úsalo.
    2) Si no sirve o no existe -> login con credenciales (env o secrets/cne_credentials.json) y guarda token.txt.
    """
    if TOKEN_FILE.exists():
        token = TOKEN_FILE.read_text(encoding="utf-8").strip()
        if token and probe_token(token):
            return token

    email, password = get_credentials()
    if not email or not password:
        raise RuntimeError(
            "No hay credenciales. Exporta CNE_EMAIL y CNE_PASS o crea secrets/cne_credentials.json"
        )
    token = login(email, password)
    TOKEN_FILE.parent.mkdir(parents=True, exist_ok=True)
    TOKEN_FILE.write_text(token, encoding="utf-8")
    return token

# ---------- extracción ----------
def fetch_estaciones(token: str):
    r = requests.get(EST_URL, headers={"Authorization": f"Bearer {token}"}, timeout=180)
    if r.status_code == 401:
        # token cayó entre probe y fetch -> reintentar con login directo:
        email, password = get_credentials()
        if not email or not password:
            raise RuntimeError("401 y sin credenciales para renovar token.")
        new_token = login(email, password)
        TOKEN_FILE.write_text(new_token, encoding="utf-8")
        r = requests.get(EST_URL, headers={"Authorization": f"Bearer {new_token}"}, timeout=180)
    r.raise_for_status()
    data = r.json()
    return data if isinstance(data, list) else data.get("data", [])

def normalize_station(e: dict) -> dict:
    ubic = e.get("ubicacion", {}) or {}
    precios = e.get("precios", {}) or {}

    def precio_info(key):
        info = precios.get(key) or {}
        return {
            "precio": normalize_price(info.get("precio")),
            "unidad": info.get("unidad_cobro"),
            "fecha": info.get("fecha_actualizacion"),
            "hora": info.get("hora_actualizacion"),
            "tipo_atencion": info.get("tipo_atencion"),
        }

    return {
        "codigo": e.get("codigo"),
        "marca": (e.get("distribuidor", {}) or {}).get("marca"),
        "razon_social": (e.get("razon_social") or "").strip(),
        "direccion": (ubic.get("direccion") or "").strip(),
        "region": ubic.get("nombre_region"),
        "cod_region": (ubic.get("codigo_region") or "").strip(),
        "comuna": ubic.get("nombre_comuna"),
        "cod_comuna": ubic.get("codigo_comuna"),
        "lat": ubic.get("latitud"),
        "lng": ubic.get("longitud"),
        # precios (todas estas llaves si existen en la API)
        "precio_93": precio_info("93"),
        "precio_95": precio_info("95"),
        "precio_97": precio_info("97"),
        "precio_DI": precio_info("DI"),
    }

def flat_row(n: dict) -> dict:
    """aplana para CSV: toma solo el valor numérico del precio y fechas"""
    def pick(d, k):
        v = d.get(k) or {}
        return v.get("precio")
    def pick_date(d, k):
        v = d.get(k) or {}
        return v.get("fecha")

    return {
        "codigo": n["codigo"],
        "marca": n["marca"],
        "razon_social": n["razon_social"],
        "direccion": n["direccion"],
        "region": n["region"],
        "cod_region": n["cod_region"],
        "comuna": n["comuna"],
        "cod_comuna": n["cod_comuna"],
        "lat": n["lat"],
        "lng": n["lng"],
        "precio_93": pick(n, "precio_93"),
        "precio_95": pick(n, "precio_95"),
        "precio_97": pick(n, "precio_97"),
        "precio_DI": pick(n, "precio_DI"),
        "fecha_93": pick_date(n, "precio_93"),
        "fecha_95": pick_date(n, "precio_95"),
        "fecha_97": pick_date(n, "precio_97"),
        "fecha_DI": pick_date(n, "precio_DI"),
    }

def main():
    t0 = time.time()
    print("🔐 Obteniendo/renovando token…")
    token = get_fresh_token()
    print("✅ Token listo (longitud:", len(token), ")")

    print("⬇️  Descargando estaciones…")
    estaciones = fetch_estaciones(token)
    print("Total estaciones recibidas:", len(estaciones))

    # ---- Guardar JSON crudo para trazabilidad
    write_json(OUT / "cne_estaciones_full.json", estaciones)

    # ---- Normalizar para consumo (incluye precios con metadatos)
    normalizado = [normalize_station(e) for e in estaciones]
    write_json(OUT / "cne_estaciones_normalizado.json", normalizado)

    # ---- CSV plano (valores de precios y fechas)
    rows = [flat_row(n) for n in normalizado]
    campos = list(rows[0].keys()) if rows else [
        "codigo","marca","razon_social","direccion","region","cod_region",
        "comuna","cod_comuna","lat","lng",
        "precio_93","precio_95","precio_97","precio_DI",
        "fecha_93","fecha_95","fecha_97","fecha_DI"
    ]
    csv_path = OUT / "cne_estaciones.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=campos)
        w.writeheader()
        w.writerows(rows)

    dt = time.time() - t0
    print("✅ Salidas:")
    print(" -", OUT / "cne_estaciones_full.json")
    print(" -", OUT / "cne_estaciones_normalizado.json")
    print(" -", csv_path)
    print(f"⏱️  Hecho en {dt:.1f}s")

if __name__ == "__main__":
    main()
