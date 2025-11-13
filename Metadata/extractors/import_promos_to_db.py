"""Import fuel promotions into PostgreSQL using the data-access layer.

This script consumes the JSON outputs from:
    * extract_promos.py        -> aliados promotions
    * extract_promos2.py       -> static promotions
    * extract_promos3.py       -> descuentosrata recurring promotions

Records are upserted (by fuente_tipo + external_id) and linked to brands via
the shared data_access metadata repositories.

Usage (inside the repo virtualenv):

    python Metadata/extractors/import_promos_to_db.py
    python Metadata/extractors/import_promos_to_db.py --aliados-only
    python Metadata/extractors/import_promos_to_db.py --estatico-only
    python Metadata/extractors/import_promos_to_db.py --dsn postgresql://user:pass@localhost:5432/rutasdb

You can also rely on standard libpq environment variables (PGHOST, PGPORT,
PGUSER, PGPASSWORD, PGDATABASE) or a DATABASE_URL entry.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import pathlib
import sys
from datetime import date, datetime
from typing import Dict, List, Optional, Sequence

try:
    import psycopg
    from psycopg import Connection, Cursor
except ImportError as exc:
    raise SystemExit(
        "psycopg (v3) is required. Install it with `pip install psycopg[binary]`."
    ) from exc

BASE_DIR = pathlib.Path(__file__).resolve().parent
OUTPUTS_DIR = BASE_DIR.parent / "outputs" / "promos"
DEFAULT_ALIADOS = OUTPUTS_DIR / "promos_aliados.json"
DEFAULT_ESTATICO = OUTPUTS_DIR / "promos_estatico.json"
DEFAULT_RATA = OUTPUTS_DIR / "promos_rata.json"

LOG = logging.getLogger("import_promos")

ROOT_DIR = pathlib.Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from db.data_access.metadata_repositories import (  # noqa: E402
    auto_link_promocion_to_marcas,
    create_scrape_run,
    delete_promociones_by_fuente,
    insert_promocion,
)

DAY_CODE_LABELS = {
    "lu": "Lunes",
    "ma": "Martes",
    "mi": "Miércoles",
    "ju": "Jueves",
    "vi": "Viernes",
    "sa": "Sábado",
    "do": "Domingo",
}


def resolve_conninfo(dsn: Optional[str]) -> str:
    """Build connection string from DSN or environment variables."""
    if dsn:
        return dsn

    if url := os.getenv("DATABASE_URL"):
        return url

    params = {
        "host": os.getenv("PGHOST", "localhost"),
        "port": os.getenv("PGPORT", "5432"),
        "user": os.getenv("PGUSER", "rutas_user"),
        "password": os.getenv("PGPASSWORD", "supersecretpassword"),
        "dbname": os.getenv("PGDATABASE", "rutasdb"),
    }
    parts = [
        f"host={params['host']}",
        f"port={params['port']}",
        f"dbname={params['dbname']}",
        f"user={params['user']}" if params["user"] else None,
        f"password={params['password']}" if params["password"] else None,
    ]
    return " ".join(p for p in parts if p)


def insert_promociones(
    conn: Connection,
    promociones: List[Dict],
    fuente_tipo: str,
    scrape_run_id: int,
    *,
    truncate: bool = False
) -> int:
    """
    Insert or update promotions using the data-access layer.
    Returns count of promotions processed.
    """
    if truncate:
        deleted = delete_promociones_by_fuente(conn, fuente_tipo)
        LOG.info("🗑️  Removed %s existing '%s' promotions", deleted, fuente_tipo)
    
    LOG.info("Inserting %s %s promotions", len(promociones), fuente_tipo)
    
    processed_count = 0
    
    for promo in promociones:
        marca_ids = _coerce_marca_ids(promo.get("marca_ids"))
        fecha_inicio = coerce_date(promo.get("fecha_inicio"))
        fecha_fin = coerce_date(promo.get("fecha_fin"))
        
        promocion_id = insert_promocion(
            conn,
            titulo=promo.get("titulo", ""),
            banco=promo.get("banco"),
            descuento=promo.get("descuento"),
            vigencia=promo.get("vigencia"),
            fuente_url=promo.get("fuente") or promo.get("fuente_url"),
            fuente_tipo=fuente_tipo,
            marca_ids=marca_ids,
            scrape_run_id=scrape_run_id,
            external_id=promo.get("external_id"),
            fecha_inicio=fecha_inicio,
            fecha_fin=fecha_fin,
            activo=promo.get("activo", True),
        )
        
        if not marca_ids:
            auto_link_promocion_to_marcas(conn, promocion_id)
        
        processed_count += 1
    
    LOG.info("✅ Upserted %s promotions", processed_count)
    
    return processed_count


def _format_day_labels(days: List[str]) -> str:
    if not days:
        return ""
    labels = []
    for code in days:
        labels.append(DAY_CODE_LABELS.get(code.lower(), code))
    return ", ".join(dict.fromkeys(labels))


def _parse_date(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    cleaned = value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(cleaned).date()
    except ValueError:
        return None


def normalize_rata_promos(promos: List[Dict]) -> List[Dict]:
    """Convert descuentosrata payload into the legacy import schema."""
    normalized: List[Dict] = []
    for item in promos:
        title = item.get("title") or item.get("nombre") or ""
        partner_info = item.get("partner") or {}
        partner = partner_info.get("name") or partner_info.get("slug") or ""

        brand_info = item.get("brand") or {}
        brand_id = brand_info.get("brand_id")
        discount = item.get("discount_amount") or ""

        day_block = _format_day_labels(item.get("days") or [])
        vigencia = day_block

        normalized.append(
            {
                "titulo": title,
                "banco": partner,
                "descuento": discount,
                "vigencia": vigencia,
                "fuente": item.get("source_url") or item.get("url_rateada") or "",
                "external_id": str(item.get("id")) if item.get("id") is not None else None,
                "fecha_inicio": _parse_date(item.get("valid_from")),
                "fecha_fin": _parse_date(item.get("valid_to")),
                "marca_ids": [brand_id] if isinstance(brand_id, int) else None,
                "activo": bool(item.get("activo", True)),
            }
        )
    return normalized


def coerce_date(value: Optional[object]) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        return _parse_date(value)
    return None


def _coerce_marca_ids(value: Optional[object]) -> Optional[List[int]]:
    if value is None:
        return None
    if isinstance(value, int):
        return [value]
    if isinstance(value, (list, tuple)):
        ids = []
        for element in value:
            if element is None:
                continue
            try:
                ids.append(int(element))
            except (TypeError, ValueError):
                continue
        return ids or None
    try:
        return [int(value)]
    except (TypeError, ValueError):
        return None


def show_summary(cur: Cursor) -> None:
    """Display summary statistics after import."""
    LOG.info("=" * 60)
    LOG.info("IMPORT SUMMARY")
    LOG.info("=" * 60)
    
    # Count active promotions
    cur.execute("SELECT COUNT(*) FROM metadata.promociones WHERE activo = TRUE")
    promo_count = cur.fetchone()[0]
    LOG.info("🎁 Active Promotions: %s", promo_count)
    
    # Count by source type
    LOG.info("")
    LOG.info("📊 Promotions by Source:")
    cur.execute("""
        SELECT fuente_tipo, COUNT(*) as count
        FROM metadata.promociones
        WHERE activo = TRUE
        GROUP BY fuente_tipo
        ORDER BY count DESC
    """)
    for row in cur.fetchall():
        LOG.info("   %s: %s", row[0] or 'unknown', row[1])
    
    # Count promotion-brand links
    cur.execute("SELECT COUNT(*) FROM metadata.promociones_marcas")
    link_count = cur.fetchone()[0]
    LOG.info("")
    LOG.info("🔗 Promotion-Brand Links: %s", link_count)
    
    # Show promotions per brand
    LOG.info("")
    LOG.info("🏷️  Promotions per Brand:")
    cur.execute("""
        SELECT m.nombre_display, COUNT(pm.promocion_id) as promo_count
        FROM metadata.marcas m
        LEFT JOIN metadata.promociones_marcas pm ON m.id = pm.marca_id
        LEFT JOIN metadata.promociones p ON pm.promocion_id = p.id AND p.activo = TRUE
        GROUP BY m.nombre_display
        HAVING COUNT(pm.promocion_id) > 0
        ORDER BY promo_count DESC
    """)
    for row in cur.fetchall():
        LOG.info("   %s: %s promotions", row[0], row[1])
    
    # Show sample promotions
    LOG.info("")
    LOG.info("📋 Sample Promotions:")
    cur.execute("""
        SELECT titulo, banco, descuento, vigencia
        FROM metadata.promociones
        WHERE activo = TRUE
        ORDER BY created_at DESC
        LIMIT 5
    """)
    for i, row in enumerate(cur.fetchall(), 1):
        LOG.info("   %d. %s", i, row[0])
        if row[1]:
            LOG.info("      Bank: %s", row[1])
        if row[2]:
            LOG.info("      Discount: %s", row[2])
        if row[3]:
            LOG.info("      Valid: %s", row[3])


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Main import routine."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", help="PostgreSQL connection string")
    parser.add_argument(
        "--aliados",
        type=pathlib.Path,
        default=DEFAULT_ALIADOS,
        help="Path to aliados JSON file (default: %(default)s)"
    )
    parser.add_argument(
        "--estatico",
        type=pathlib.Path,
        default=DEFAULT_ESTATICO,
        help="Path to estatico JSON file (default: %(default)s)"
    )
    parser.add_argument(
        "--rata",
        type=pathlib.Path,
        default=DEFAULT_RATA,
        help="Path to descuentosrata JSON file (default: %(default)s)"
    )
    parser.add_argument(
        "--aliados-only",
        action="store_true",
        help="Import only aliados promotions"
    )
    parser.add_argument(
        "--estatico-only",
        action="store_true",
        help="Import only estatico promotions"
    )
    parser.add_argument(
        "--rata-only",
        action="store_true",
        help="Import only descuentosrata promotions"
    )
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="Delete existing promotions before importing"
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level (default: INFO)"
    )

    args = parser.parse_args(list(argv) if argv is not None else None)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(levelname)s %(message)s"
    )

    # Determine which files to process
    files_to_process = []
    
    if args.rata_only:
        if args.rata.exists():
            files_to_process.append(('rata', args.rata))
        else:
            LOG.warning("Descuentosrata file not found: %s", args.rata)
    else:
        if not args.estatico_only and not args.rata_only:
            if args.aliados.exists():
                files_to_process.append(('aliados', args.aliados))
            else:
                LOG.warning("Aliados file not found: %s", args.aliados)
        
        if not args.aliados_only and not args.rata_only:
            if args.estatico.exists():
                files_to_process.append(('estatico', args.estatico))
            else:
                LOG.warning("Estatico file not found: %s", args.estatico)
        
        if not args.aliados_only and not args.estatico_only:
            if args.rata.exists():
                files_to_process.append(('rata', args.rata))
            else:
                LOG.warning("Descuentosrata file not found: %s", args.rata)

    if not files_to_process:
        LOG.error("No promotion files found to import")
        LOG.error("Run extract_promos.py, extract_promos2.py and/or extract_promos3.py first")
        return 1

    conninfo = resolve_conninfo(args.dsn)
    LOG.info("Connecting to PostgreSQL...")

    try:
        with psycopg.connect(conninfo) as conn:
            total_imported = 0

            for fuente_tipo, file_path in files_to_process:
                LOG.info("")
                LOG.info("=" * 60)
                LOG.info("Processing %s promotions from %s", fuente_tipo, file_path)
                LOG.info("=" * 60)

                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        promociones = json.load(f)
                except json.JSONDecodeError as e:
                    LOG.error("Failed to parse %s: %s", file_path, e)
                    continue

                if not isinstance(promociones, list):
                    LOG.error("Expected a list in %s, got %s", file_path, type(promociones))
                    continue

                if not promociones:
                    LOG.warning("No promotions found in %s", file_path)
                    continue

                if fuente_tipo == 'rata':
                    promociones = normalize_rata_promos(promociones)

                LOG.info("Loaded %s promotions", len(promociones))

                scrape_run_id = create_scrape_run(
                    conn,
                    source_type=f'promos_{fuente_tipo}',
                    record_count=len(promociones),
                )

                count = insert_promociones(
                    conn,
                    promociones,
                    fuente_tipo,
                    scrape_run_id,
                    truncate=args.truncate,
                )
                total_imported += count

            LOG.info("")
            with conn.cursor() as cur:
                show_summary(cur)

            LOG.info("")
            LOG.info("=" * 60)
            LOG.info("✅ Import complete! Imported %s promotions total", total_imported)
            LOG.info("=" * 60)
            return 0

    except psycopg.Error as e:
        LOG.error("Database error: %s", e)
        return 1
    except Exception as e:
        LOG.error("Unexpected error: %s", e)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())

