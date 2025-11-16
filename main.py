#!/usr/bin/env python3
# main.py — Orquestador E2E para RutasDeBencina
# Uso rápido:
#   python main.py            # corre flujo completo por defecto (token+docker+etl+web)
#   python main.py all --wait-db --bbox -33.6 -70.9 -33.3 -70.5
#   python main.py up --wait-db
#   python main.py schema
#   python main.py infra --bbox <lat_min> <lon_min> <lat_max> <lon_max>
#   python main.py etl
#   python main.py import
#   python main.py web

import argparse
import base64
import datetime
import json
import os
import sys
import time
import subprocess
from pathlib import Path
from shutil import which

ROOT = Path(__file__).resolve().parent

# Rutas del repo
DOCKER_COMPOSE = ROOT / "DockerFolder" / "docker-compose.yml"
SCHEMA_SQL      = ROOT / "db" / "schema.sql"
OVERPASS_PY     = ROOT / "Infraestructura" / "import_overpass_to_db.py"

# Metadata (ETLs y cargas)
EXTRACTORS_DIR  = ROOT / "Metadata" / "extractors"
EXTRACT_CNE     = EXTRACTORS_DIR / "extract_cne.py"
EXTRACT_PROMOS  = EXTRACTORS_DIR / "extract_promos.py"
EXTRACT_PROMOS2 = EXTRACTORS_DIR / "extract_promos2.py"
EXTRACT_CONSUMO = EXTRACTORS_DIR / "extract_consumo.py"
IMPORT_ALL_META = EXTRACTORS_DIR / "import_all_metadata.py"
IMPORT_CNE_DB   = EXTRACTORS_DIR / "import_cne_to_db.py"
IMPORT_PROMOS_DB= EXTRACTORS_DIR / "import_promos_to_db.py"

WEB_APP_MODULE  = "web.web"  # Flask app: web/web.py

def wait_db(timeout=180):
    # Fallback: psql local si existe
    if which("psql"):
        r = subprocess.run(["psql", DSN, "-c", "SELECT 1"], capture_output=True)
        if r.returncode == 0:
            print("[INFO] Postgres listo (psql local).")
            return
    time.sleep(3)
    print("[ERROR] Timeout esperando la BD.")
    sys.exit(1)

def schema(_):
    if not SCHEMA_SQL.exists():
        print(f"[WARN] No encontrado esquema: {SCHEMA_SQL}")
        return
    cid = service_container_id("db")
    if cid:
        # copiar y ejecutar dentro del contenedor
        run(["docker", "cp", str(SCHEMA_SQL), f"{cid}:/tmp/schema.sql"])
        run(["docker", "exec", cid, "psql", DSN, "-f", "/tmp/schema.sql"])
    elif which("psql"):
        run(["psql", DSN, "-f", str(SCHEMA_SQL)])
    else:
        print("[ERROR] No hay contenedor 'db' ni psql local.")
        sys.exit(1)

# Ejecuta save_token.py si falta token.txt
def decode_jwt_exp(token: str):
    try:
        parts = token.split(".")
        if len(parts) < 2:
            return None
        payload_b64 = parts[1]
        rem = len(payload_b64) % 4
        if rem:
            payload_b64 += "=" * (4 - rem)
        payload = base64.urlsafe_b64decode(payload_b64.encode("utf-8"))
        data = json.loads(payload.decode("utf-8"))
        exp = data.get("exp")
        if exp:
            return datetime.datetime.utcfromtimestamp(int(exp))
    except Exception:
        return None
    return None


def token_is_valid(token_file: Path, min_valid_minutes: int = 10) -> bool:
    if not token_file.exists() or token_file.stat().st_size == 0:
        return False
    token = token_file.read_text(encoding="utf-8").strip()
    if not token:
        return False
    exp_dt = decode_jwt_exp(token)
    if exp_dt is None:
        # Si no es JWT asumimos que sigue válido
        return True
    remaining = exp_dt - datetime.datetime.utcnow()
    return remaining.total_seconds() > min_valid_minutes * 60


def ensure_token(force=False):
    """Genera o renueva token.txt usando save_token.py"""
    token_file = ROOT / "token.txt"
    if not force and token_is_valid(token_file):
        return
    save_token = ROOT / "save_token.py"
    if save_token.exists():
        print("[INFO] Generando token CNE con save_token.py ...")
        run([sys.executable, str(save_token)])
    else:
        print("[WARN] No se encontró save_token.py; continúa sin regenerar token.")

def infra(args):
    if not OVERPASS_PY.exists():
        print(f"[WARN] No encontrado: {OVERPASS_PY}")
        return
    cmd = [sys.executable, str(OVERPASS_PY), "--dsn", DSN]
    if args.bbox and len(args.bbox) == 4:
        cmd += ["--bbox", *map(str, args.bbox)]
    if args.input:
        cmd += ["--input", args.input]
    if args.no_truncate:
        cmd += ["--no-truncate"]
    if args.skip_topology:
        cmd += ["--skip-topology"]
    run(cmd)

def etl(_):
    """
@@ -155,82 +192,92 @@ 
"""
def import_to_db(_):

    if IMPORT_CNE_DB.exists():
        print("[INFO] Importando CNE a BD...")
        run([sys.executable, str(IMPORT_CNE_DB), "--dsn", DSN])
        ran = True
    if IMPORT_PROMOS_DB.exists():
        print("[INFO] Importando Promos a BD...")
        run([sys.executable, str(IMPORT_PROMOS_DB), "--dsn", DSN])
        ran = True
    if not ran:
        # Fallback genérico: intenta usar db/data_access/import_metadata.py
        generic = ROOT / "db" / "data_access" / "import_metadata.py"
        if generic.exists():
            print("[INFO] Importando metadata con import_metadata.py ...")
            run([sys.executable, str(generic), "--dsn", DSN])
        else:
            print("[WARN] No se encontraron scripts de importación a BD.")

def web(_):
    env = os.environ.copy()
    env["FLASK_APP"] = WEB_APP_MODULE
    env.setdefault("FLASK_RUN_HOST", FLASK_HOST)
    env.setdefault("FLASK_RUN_PORT", str(FLASK_PORT))
    run([sys.executable, "-m", "flask", "run"], env=env)

def all_steps(args):
    ensure_token(force=args.force_token if hasattr(args, "force_token") else False)
    up(args)
    schema(args)
    infra(args)
    etl(args)
    import_to_db(args)
    web(args)

def parse_args():
    p = argparse.ArgumentParser(description="RutasDeBencina Orquestador")
    sub = p.add_subparsers(dest="cmd", required=True)

    sp_token = sub.add_parser("token", help="Obtiene/renueva token.txt")
    sp_token.add_argument("--force", action="store_true", help="Regenera incluso si parece vigente")
    sp_token.set_defaults(func=lambda a: ensure_token(force=a.force))

    sp_up = sub.add_parser("up", help="Levanta Docker")
    sp_up.add_argument("--wait-db", action="store_true")
    sp_up.add_argument("--db-timeout", type=int, default=180)
    sp_up.set_defaults(func=up)

    sp_down = sub.add_parser("down", help="Baja Docker")
    sp_down.set_defaults(func=down)

    sp_schema = sub.add_parser("schema", help="Carga esquema SQL")
    sp_schema.set_defaults(func=schema)

    sp_infra = sub.add_parser("infra", help="Importa red vial OSM (Overpass) a PostGIS")
    sp_infra.add_argument("--bbox", nargs=4, type=float, metavar=("LAT_MIN","LON_MIN","LAT_MAX","LON_MAX"))
    sp_infra.add_argument("--input")
    sp_infra.add_argument("--no-truncate", action="store_true")
    sp_infra.add_argument("--skip-topology", action="store_true")
    sp_infra.set_defaults(func=infra)

    sp_etl = sub.add_parser("etl", help="Corre extractores de Metadata")
    sp_etl.set_defaults(func=etl)

    sp_imp = sub.add_parser("import", help="Importa metadata a la BD")
    sp_imp.set_defaults(func=import_to_db)

    sp_web = sub.add_parser("web", help="Levanta la web Flask")
    sp_web.set_defaults(func=web)
    sp_all = sub.add_parser("all", help="Todo: token -> up -> schema -> infra -> etl -> import -> web")
    sp_all.add_argument("--wait-db", action="store_true")
    sp_all.add_argument("--wait-db", action="store_true")
    sp_all.add_argument("--db-timeout", type=int, default=180)
    sp_all.add_argument("--bbox", nargs=4, type=float, metavar=("LAT_MIN","LON_MIN","LAT_MAX","LON_MAX"))
    sp_all.add_argument("--input")
    sp_all.add_argument("--no-truncate", action="store_true")
    sp_all.add_argument("--skip-topology", action="store_true")
    sp_all.add_argument("--force-token", action="store_true", help="Regenera token antes de comenzar")
    sp_all.set_defaults(func=all_steps)

    return p.parse_args()
    return p


def main():
    if not DOCKER_COMPOSE.exists():
        print(f"[WARN] No se encontró {DOCKER_COMPOSE}. Puedes ejecutar 'schema/infra/etl/import/web' en local si tienes Postgres y dependencias instaladas.")
    parser = parse_args()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()