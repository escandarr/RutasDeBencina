import atexit
import os
import re
import subprocess
import sys
import threading
import time
import unicodedata
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Dict, Sequence, Tuple

from flask import Flask, abort, jsonify, render_template, request, url_for
from psycopg.rows import dict_row

from db.data_access import Database, RouteResult, compute_route_between_points

# Create an instance of the Flask application
app = Flask(__name__)

DATABASE = Database()
atexit.register(DATABASE.close)


Point = Tuple[float, float]

BRAND_LOGOS: Dict[str, str] = {
    "copec": "media/Copec.jpg",
    "prontocopec": "media/Copec.jpg",
    "petrobras": "media/petrobras.jpg",
    "shell": "media/shell.png",
    "abastible": "media/abastible.jpg",
}

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_TIMEOUT_SECONDS = 15 * 60  # 15 minutes
OUTPUT_CHAR_LIMIT = 4000
_DATASET_ALIASES = {
    "cne": "cne",
    "cne_data": "cne",
    "estaciones": "cne",
    "stations": "cne",
    "promos": "promotions",
    "promo": "promotions",
    "promotion": "promotions",
    "promotions": "promotions",
}
DATA_TASKS: Dict[str, Sequence[dict]] = {
    "cne": (
        {"label": "extract_cne", "path": Path("Metadata/extractors/extract_cne.py")},
        {"label": "import_cne", "path": Path("Metadata/extractors/import_cne_to_db.py")},
    ),
    "promotions": (
        {"label": "extract_promos", "path": Path("Metadata/extractors/extract_promos.py")},
        {"label": "extract_promos_static", "path": Path("Metadata/extractors/extract_promos3.py")},
        {"label": "import_promos", "path": Path("Metadata/extractors/import_promos_to_db.py")},
    ),
}
_management_lock = threading.Lock()


def _parse_point_payload(payload: dict, key: str) -> Point:
    data = payload.get(key)
    if not isinstance(data, dict):
        raise ValueError(f'"{key}" must be an object with "lat" and "lon" fields.')

    try:
        lat = float(data["lat"])
        lon = float(data["lon"])
    except (KeyError, TypeError, ValueError):
        raise ValueError(f'"{key}" must contain numeric "lat" and "lon" values.')

    return lon, lat

# Define a route for the homepage
@app.route('/')
def home():
    """Render the Leaflet map view."""
    return render_template('map.html')


@app.route('/admin')
def admin_dashboard():
    """Render the data management console."""
    return render_template('admin.html')

# Define another route for a different page
@app.route('/about')
def about():
    """This function runs when someone visits the /about page."""
    return '<h2>This is the About Page!</h2>'


def _route_result_to_feature(route: RouteResult) -> dict:
    coords = list(route.coordinates)
    if len(coords) == 1:
        coords = coords * 2

    return {
        "type": "Feature",
        "geometry": {
            "type": "LineString",
            "coordinates": coords,
        },
        "properties": {
            "start_node": route.start.id,
            "end_node": route.end.id,
            "total_cost": route.total_cost,
            "segment_count": len(route.segments),
        },
    }


@app.route('/api/routes/shortest', methods=['POST'])
def api_shortest_route():
    payload = request.get_json(silent=True)
    if payload is None:
        abort(400, description='Expected JSON body with "start" and "end" points.')

    try:
        start_lon, start_lat = _parse_point_payload(payload, 'start')
        end_lon, end_lat = _parse_point_payload(payload, 'end')
    except ValueError as exc:
        abort(400, description=str(exc))

    with DATABASE.connection() as conn:
        try:
            route = compute_route_between_points(
                conn,
                start_lon,
                start_lat,
                end_lon,
                end_lat,
            )
        except ValueError as exc:
            abort(400, description=str(exc))
        except Exception as exc:  # pragma: no cover - defensive fallback
            app.logger.exception("Error computing route", exc_info=exc)
            abort(500, description='Unable to compute route at this time.')

    feature = _route_result_to_feature(route)

    return jsonify(
        {
            "route": feature,
            "start": {"id": route.start.id, "lon": route.start.lon, "lat": route.start.lat},
            "end": {"id": route.end.id, "lon": route.end.lon, "lat": route.end.lat},
        }
    )


def _to_float(value: Decimal | float | None) -> float | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    return float(value)


def _normalize_brand_name(value: str | None) -> str | None:
    if not value:
        return None

    normalized = unicodedata.normalize("NFKD", value)
    normalized = normalized.encode("ascii", "ignore").decode("ascii")
    normalized = re.sub(r"[^a-zA-Z0-9]", "", normalized).lower()
    return normalized or None


def _brand_logo_url(marca: str | None) -> str | None:
    normalized = _normalize_brand_name(marca)
    if not normalized:
        return None

    for key, filename in BRAND_LOGOS.items():
        if normalized.startswith(key):
            return url_for("static", filename=filename)
    return None


def _tail_text(value: str | None, limit: int = OUTPUT_CHAR_LIMIT) -> str:
    if not value:
        return ""
    if len(value) <= limit:
        return value
    return f"...(truncated)...\n{value[-limit:]}"


def _run_management_script(step: dict, *, extra_args: Sequence[str] | None = None) -> dict:
    script_path = PROJECT_ROOT / step["path"]
    command = [sys.executable, str(script_path)]
    if extra_args:
        command.extend(extra_args)

    started_at = time.monotonic()
    result: dict[str, object] = {
        "label": step.get("label"),
        "command": command,
        "success": False,
        "stdout": "",
        "stderr": "",
        "returncode": None,
        "duration_seconds": None,
    }

    if not script_path.exists():
        result["stderr"] = f"Script not found: {script_path}"
        return result

    env = os.environ.copy()
    env.setdefault("PYTHONIOENCODING", "utf-8")

    try:
        completed = subprocess.run(
            command,
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            env=env,
            timeout=SCRIPT_TIMEOUT_SECONDS,
            check=False,
        )
        result["returncode"] = completed.returncode
        result["stdout"] = _tail_text(completed.stdout)
        result["stderr"] = _tail_text(completed.stderr)
        result["success"] = completed.returncode == 0
    except subprocess.TimeoutExpired as exc:
        result["stderr"] = _tail_text(
            f"Timed out after {SCRIPT_TIMEOUT_SECONDS}s. Partial output:\n{exc.stderr or ''}"
        )
        result["stdout"] = _tail_text(exc.stdout)
        result["returncode"] = None
    except Exception as exc:  # pragma: no cover - defensive fallback
        result["stderr"] = f"Unexpected error: {exc}"
        result["returncode"] = None

    result["duration_seconds"] = round(time.monotonic() - started_at, 2)
    return result


def _normalize_dataset(value: str | None) -> str | None:
    if not value:
        return None
    return _DATASET_ALIASES.get(value.strip().lower())


def _parse_bbox_args() -> Tuple[float, float, float, float] | None:
    north = request.args.get("north")
    south = request.args.get("south")
    east = request.args.get("east")
    west = request.args.get("west")

    if not any([north, south, east, west]):
        return None

    if not all([north, south, east, west]):
        abort(400, description="If using bounding box filters you must provide north, south, east, and west.")

    try:
        north_f = float(north)  # type: ignore[arg-type]
        south_f = float(south)  # type: ignore[arg-type]
        east_f = float(east)  # type: ignore[arg-type]
        west_f = float(west)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        abort(400, description="Bounding box values must be numeric.")

    if south_f > north_f:
        abort(400, description="south must be less than or equal to north.")

    return north_f, south_f, east_f, west_f


@app.route("/api/stations", methods=["GET"])
def api_stations() -> tuple:
    bbox = _parse_bbox_args()

    fuel_type = request.args.get("fuel_type")
    allowed_fuels = {"93", "95", "97", "DI"}
    if fuel_type:
        fuel_type = fuel_type.upper()
        if fuel_type not in allowed_fuels:
            abort(400, description="Invalid fuel type. Use 93, 95, 97, or DI.")

    try:
        limit = int(request.args.get("limit", 150))
    except ValueError:
        abort(400, description="limit must be an integer.")

    if limit <= 0:
        limit = 1
    if limit > 500:
        limit = 500

    with DATABASE.connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            params = []
            filters = ["e.lat IS NOT NULL", "e.lng IS NOT NULL"]

            if fuel_type:
                filters.append("pa.tipo_combustible = %s")
                params.append(fuel_type)

            if bbox:
                north, south, east, west = bbox
                filters.append("e.lat BETWEEN %s AND %s")
                params.extend([south, north])
                if east >= west:
                    filters.append("e.lng BETWEEN %s AND %s")
                    params.extend([west, east])
                else:
                    # Bounding box crosses the antimeridian; split into OR conditions
                    filters.append("(e.lng >= %s OR e.lng <= %s)")
                    params.extend([west, east])

            sql = f"""
                SELECT
                    e.codigo,
                    e.marca,
                    e.direccion,
                    e.comuna,
                    e.region,
                    e.lat,
                    e.lng,
                    pa.tipo_combustible,
                    pa.precio,
                    pa.fecha,
                    pa.hora
                FROM metadata.precios_actuales pa
                JOIN metadata.estaciones_cne e ON e.id = pa.estacion_id
                WHERE {' AND '.join(filters)}
                ORDER BY pa.precio ASC NULLS LAST
                LIMIT %s
            """

            params.append(limit * 4)
            cur.execute(sql, params)
            rows = cur.fetchall()

    stations: Dict[str, dict] = {}
    for row in rows:
        codigo = row["codigo"]
        if not codigo:
            continue

        lat = row.get("lat")
        lng = row.get("lng")
        if lat is None or lng is None:
            continue

        station = stations.setdefault(
            codigo,
            {
                "codigo": codigo,
                "marca": row.get("marca"),
                "logo_url": _brand_logo_url(row.get("marca")),
                "direccion": row.get("direccion"),
                "comuna": row.get("comuna"),
                "region": row.get("region"),
                "lat": lat,
                "lng": lng,
                "precios": {},
                "last_update": None,
            },
        )

        price_value = row.get("precio")
        fuel = row.get("tipo_combustible")
        if fuel:
            station["precios"][fuel] = _to_float(price_value)

        fecha = row.get("fecha")
        hora = row.get("hora")
        if fecha:
            if isinstance(fecha, datetime):
                timestamp = fecha
            else:
                if hora:
                    timestamp = datetime.combine(fecha, hora)
                else:
                    timestamp = datetime(fecha.year, fecha.month, fecha.day)
            station["last_update"] = timestamp.isoformat()

    station_list = list(stations.values())

    if fuel_type:
        station_list.sort(key=lambda s: (s["precios"].get(fuel_type) is None, s["precios"].get(fuel_type, float("inf"))))
    else:
        station_list.sort(key=lambda s: s.get("marca") or "")

    station_list = station_list[:limit]

    return jsonify({
        "count": len(station_list),
        "stations": station_list,
    }), 200


@app.route("/api/admin/refresh", methods=["POST"])
def api_admin_refresh():
    payload = request.get_json(silent=True) or {}
    dataset_key = payload.get("dataset") or payload.get("target") or payload.get("source")
    dataset = _normalize_dataset(dataset_key)
    if dataset is None:
        abort(400, description='Invalid dataset. Use "cne" or "promotions".')

    tasks = DATA_TASKS.get(dataset)
    if not tasks:
        abort(400, description=f"No tasks configured for dataset '{dataset}'.")

    requested_steps = payload.get("steps")
    if requested_steps and not isinstance(requested_steps, (list, tuple, set)):
        abort(400, description='"steps" must be a list of step labels if provided.')
    step_filter = {step_name for step_name in requested_steps} if requested_steps else None

    if not _management_lock.acquire(blocking=False):
        abort(409, description="Another data management task is currently running. Try again later.")

    results = []
    overall_success = True

    try:
        for step in tasks:
            label = step.get("label")
            if step_filter and label not in step_filter:
                continue

            extra_args: list[str] = []
            if label == "import_cne" and payload.get("truncate_prices"):
                extra_args.append("--truncate-prices")
            if label == "import_promos" and payload.get("truncate"):
                extra_args.append("--truncate")

            step_result = _run_management_script(step, extra_args=extra_args)
            results.append(step_result)
            if not step_result.get("success"):
                overall_success = False
                if not payload.get("continue_on_error", False):
                    break
    finally:
        _management_lock.release()

    status_code = 200 if overall_success else 500
    return jsonify(
        {
            "dataset": dataset,
            "success": overall_success,
            "steps": results,
        }
    ), status_code


@app.route("/api/matriculas/<string:patente>", methods=["GET"])
def api_matricula_lookup(patente: str):
    normalized = patente.strip().upper()
    if not normalized:
        abort(400, description="La matrícula es obligatoria.")

    with DATABASE.connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT
                    patente,
                    marca,
                    modelo,
                    año,
                    tipo_combustible,
                    rendimiento_ciudad,
                    rendimiento_carretera,
                    rendimiento_mixto,
                    fuente,
                    created_at
                FROM metadata.consumo_vehicular
                WHERE UPPER(patente) = %s
                ORDER BY created_at DESC NULLS LAST
                LIMIT 1
                """,
                (normalized,),
            )
            row = cur.fetchone()

    if not row:
        abort(404, description="No se encontraron datos para la matrícula especificada.")

    created_at = row.get("created_at")
    response = {
        "patente": row.get("patente"),
        "marca": row.get("marca"),
        "modelo": row.get("modelo"),
        "anio": row.get("año"),
        "tipo_combustible": row.get("tipo_combustible"),
        "rendimiento": {
            "ciudad": _to_float(row.get("rendimiento_ciudad")),
            "carretera": _to_float(row.get("rendimiento_carretera")),
            "mixto": _to_float(row.get("rendimiento_mixto")),
        },
        "fuente": row.get("fuente"),
        "actualizado_en": created_at.isoformat() if isinstance(created_at, datetime) else None,
    }

    return jsonify(response)

# This block allows you to run the app directly from the script
if __name__ == '__main__':
    app.run(debug=True)