#!/usr/bin/env python3
# save_token.py
"""
Obtener token desde api.cne.cl y guardarlo en token.txt (form-urlencoded POST)

Uso:
  python3 save_token.py

Credenciales (busca en orden):
  1) Variables de entorno: CNE_EMAIL, CNE_PASS
  2) secrets/cne_credentials.json -> {"email":"...","password":"..."}
  3) Prompt interactivo
"""
from pathlib import Path
import os, json, sys, base64, datetime

try:
    import requests
except Exception:
    print("Instala requests: pip install requests")
    sys.exit(1)

ROOT = Path(__file__).resolve().parent
SECRETS = ROOT / "secrets" / "cne_credentials.json"
TOKEN_FILE = ROOT / "token.txt"
LOGIN_URL = "https://api.cne.cl/api/login"   # acepta form-urlencoded

def read_creds_from_file(path: Path):
    if path.exists():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            return data.get("email"), data.get("password")
        except Exception:
            return None, None
    return None, None

def get_credentials():
    email = os.environ.get("CNE_EMAIL")
    password = os.environ.get("CNE_PASS")
    if email and password:
        return email, password

    email, password = read_creds_from_file(SECRETS)
    if email and password:
        return email, password

    # fallback interactive
    try:
        email = input("Email CNE: ").strip()
        import getpass
        password = getpass.getpass("Password CNE: ").strip()
        if email and password:
            return email, password
    except KeyboardInterrupt:
        pass
    return None, None

def login_form(email: str, password: str):
    """
    Hace un POST en form-urlencoded (como curl --data "email=...&password=...")
    """
    data = {"email": email, "password": password}
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    try:
        r = requests.post(LOGIN_URL, data=data, headers=headers, timeout=30)
    except Exception as e:
        print("Error de conexión:", e)
        return None, None
    try:
        j = r.json()
    except Exception:
        print("Respuesta no JSON:", r.status_code, r.text[:400])
        return None, r
    if r.status_code != 200:
        print("Login falló:", r.status_code, j)
        return None, r
    # token puede venir en 'token' o 'access_token' u otra clave
    token = j.get("token") or j.get("access_token") or (j.get("data") or {}).get("token")
    return token, r

def decode_jwt_exp(token: str):
    # decode sin verificar para obtener 'exp' si existe
    try:
        parts = token.split(".")
        if len(parts) < 2:
            return None
        payload_b64 = parts[1]
        rem = len(payload_b64) % 4
        if rem:
            payload_b64 += "=" * (4 - rem)
        payload = base64.urlsafe_b64decode(payload_b64.encode("utf-8"))
        obj = json.loads(payload.decode("utf-8"))
        exp = obj.get("exp")
        if exp:
            return datetime.datetime.utcfromtimestamp(int(exp))
    except Exception:
        return None
    return None

def main():
    email, password = get_credentials()
    if not (email and password):
        print("No se hallaron credenciales. Exporta CNE_EMAIL/CNE_PASS o crea secrets/cne_credentials.json")
        sys.exit(1)

    print("Login CNE ->", email)
    token, resp = login_form(email, password)
    if not token:
        print("No se obtuvo token. Revisar credenciales o respuesta.")
        if resp is not None:
            print("Respuesta:", getattr(resp, "status_code", None), getattr(resp, "text", None)[:1000])
        sys.exit(1)

    TOKEN_FILE.write_text(token, encoding="utf-8")
    print("Token guardado en:", TOKEN_FILE)

    # mostrar expiración si es JWT
    exp_dt = decode_jwt_exp(token)
    if exp_dt:
        now = datetime.datetime.utcnow()
        remaining = exp_dt - now
        mins = int(remaining.total_seconds() / 60)
        print(f"Token expira (UTC): {exp_dt}  (≈ {mins} min restantes)")
    else:
        print("No se pudo determinar expiración (no JWT o 'exp' ausente).")

if __name__ == "__main__":
    main()
