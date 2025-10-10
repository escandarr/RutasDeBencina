#!/usr/bin/env python3
# save_token.py
"""
Obtener token desde api.cne.cl y guardarlo en token.txt

Uso:
  python3 save_token.py

Busca credenciales en:
  1) Variables de entorno: CNE_EMAIL, CNE_PASS
  2) secrets/cne_credentials.json -> {"email": "...", "password": "..."}
  3) Entrada interactiva (prompt)
"""
import os
import json
import sys
import base64
import datetime
from pathlib import Path

try:
    import requests
except Exception as e:
    print("Necesitas instalar requests: pip install requests")
    raise SystemExit(1)

ROOT = Path(__file__).resolve().parent
SECRETS = ROOT / "secrets" / "cne_credentials.json"
TOKEN_FILE = ROOT / "token.txt"
LOGIN_URL = "https://api.cne.cl/api/login"

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

    # fallback: prompt
    try:
        email = input("Email CNE: ").strip()
        import getpass
        password = getpass.getpass("Password CNE: ").strip()
        if email and password:
            return email, password
    except KeyboardInterrupt:
        pass
    return None, None

def login(email: str, password: str):
    payload = {"email": email, "password": password}
    try:
        r = requests.post(LOGIN_URL, json=payload, timeout=30)
    except Exception as e:
        print("Error al conectar con la API:", e)
        return None, r if 'r' in locals() else None
    try:
        j = r.json()
    except Exception:
        print("Respuesta no JSON:", r.status_code, r.text[:400])
        return None, r
    if r.status_code != 200:
        print("Login falló:", r.status_code, j)
        return None, r
    token = j.get("token") or j.get("access_token") or j.get("data", {}).get("token")
    if not token:
        print("Respuesta sin token:", j)
        return None, r
    return token, r

def decode_jwt_exp(token: str):
    # Sin verificar, solo para leer 'exp' si es JWT
    try:
        parts = token.split(".")
        if len(parts) < 2:
            return None
        payload_b64 = parts[1]
        # pad
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
        print("No se encontraron credenciales. Exporta CNE_EMAIL/CNE_PASS o crea secrets/cne_credentials.json.")
        sys.exit(1)

    print("Iniciando login para:", email)
    token, resp = login(email, password)
    if not token:
        print("No se obtuvo token. Revisa credenciales / respuesta:", getattr(resp, "text", None))
        sys.exit(1)

    TOKEN_FILE.write_text(token, encoding="utf-8")
    print("Token guardado en:", TOKEN_FILE)

    # mostrar expiración si es JWT
    exp_dt = decode_jwt_exp(token)
    if exp_dt:
        now = datetime.datetime.utcnow()
        remaining = exp_dt - now
        mins = int(remaining.total_seconds() / 60)
        print(f"Token parece expirar a las (UTC): {exp_dt}  (≈ {mins} min restantes)")
    else:
        print("No se pudo obtener expiración desde el token (no es JWT o campo 'exp' ausente).")

if __name__ == "__main__":
    main()
