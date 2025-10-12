"""Import fuel promotions from banks and payment methods into PostgreSQL.

This script reads the JSON outputs from extract_promos.py and extract_promos2.py
and persists them into the metadata schema. It automatically links promotions
to gas station brands based on text matching.

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
from typing import Dict, List, Optional, Sequence

try:
    import psycopg
    from psycopg import Cursor
except ImportError as exc:
    raise SystemExit(
        "psycopg (v3) is required. Install it with `pip install psycopg[binary]`."
    ) from exc

BASE_DIR = pathlib.Path(__file__).resolve().parent
OUTPUTS_DIR = BASE_DIR.parent / "outputs" / "promos"
DEFAULT_ALIADOS = OUTPUTS_DIR / "promos_aliados.json"
DEFAULT_ESTATICO = OUTPUTS_DIR / "promos_estatico.json"

LOG = logging.getLogger("import_promos")


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


def create_scrape_run(cur: Cursor, source_type: str, record_count: int) -> int:
    """Create a scrape run record and return its ID."""
    cur.execute(
        """
        INSERT INTO metadata.scrape_runs (source_type, record_count, success)
        VALUES (%s, %s, %s)
        RETURNING id
        """,
        (source_type, record_count, True)
    )
    scrape_run_id = cur.fetchone()[0]
    LOG.info("Created scrape run id=%s for %s", scrape_run_id, source_type)
    return scrape_run_id


def extract_marca_ids_from_promo(cur: Cursor, promo_text: str) -> List[int]:
    """
    Find brand IDs mentioned in promotion text.
    Returns list of marca_ids that match.
    """
    cur.execute("""
        SELECT id, nombre, nombre_display 
        FROM metadata.marcas
        WHERE activo = TRUE
        ORDER BY LENGTH(nombre) DESC
    """)
    
    marca_ids = []
    for row in cur.fetchall():
        marca_id, nombre, nombre_display = row
        if nombre.upper() in promo_text.upper() or (nombre_display and nombre_display.upper() in promo_text.upper()):
            marca_ids.append(marca_id)
    
    return marca_ids


def insert_promociones(
    cur: Cursor, 
    promociones: List[Dict], 
    fuente_tipo: str,
    scrape_run_id: int,
    *,
    truncate: bool = False
) -> int:
    """
    Insert promotions and auto-link to brands.
    Returns count of promotions inserted.
    """
    if truncate:
        LOG.info("Truncating existing %s promotions", fuente_tipo)
        cur.execute(
            "DELETE FROM metadata.promociones WHERE fuente_tipo = %s",
            (fuente_tipo,)
        )
    
    LOG.info("Inserting %s %s promotions", len(promociones), fuente_tipo)
    
    inserted_count = 0
    linked_count = 0
    
    for promo in promociones:
        titulo = promo.get('titulo', '')
        banco = promo.get('banco', '')
        descuento = promo.get('descuento', '')
        vigencia = promo.get('vigencia', '')
        fuente_url = promo.get('fuente', '')
        
        # Insert promotion
        cur.execute(
            """
            INSERT INTO metadata.promociones
                (titulo, banco, descuento, vigencia, fuente_url, fuente_tipo, scrape_run_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (titulo, banco, descuento, vigencia, fuente_url, fuente_tipo, scrape_run_id)
        )
        promocion_id = cur.fetchone()[0]
        inserted_count += 1
        
        # Auto-link to brands based on text
        search_text = f"{titulo} {banco} {descuento}"
        marca_ids = extract_marca_ids_from_promo(cur, search_text)
        
        for marca_id in marca_ids:
            cur.execute(
                """
                INSERT INTO metadata.promociones_marcas (promocion_id, marca_id)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
                """,
                (promocion_id, marca_id)
            )
            linked_count += 1
    
    LOG.info("✅ Inserted %s promotions", inserted_count)
    LOG.info("🔗 Created %s promotion-brand links", linked_count)
    
    return inserted_count


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
    
    if not args.estatico_only:
        if args.aliados.exists():
            files_to_process.append(('aliados', args.aliados))
        else:
            LOG.warning("Aliados file not found: %s", args.aliados)
    
    if not args.aliados_only:
        if args.estatico.exists():
            files_to_process.append(('estatico', args.estatico))
        else:
            LOG.warning("Estatico file not found: %s", args.estatico)
    
    if not files_to_process:
        LOG.error("No promotion files found to import")
        LOG.error("Run extract_promos.py and/or extract_promos2.py first")
        return 1

    conninfo = resolve_conninfo(args.dsn)
    LOG.info("Connecting to PostgreSQL...")

    try:
        with psycopg.connect(conninfo) as conn:
            with conn.cursor() as cur:
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
                    
                    LOG.info("Loaded %s promotions", len(promociones))
                    
                    # Create scrape run
                    scrape_run_id = create_scrape_run(
                        cur,
                        f'promos_{fuente_tipo}',
                        len(promociones)
                    )
                    
                    # Import promotions
                    count = insert_promociones(
                        cur,
                        promociones,
                        fuente_tipo,
                        scrape_run_id,
                        truncate=args.truncate
                    )
                    total_imported += count
                
                # Show summary
                LOG.info("")
                show_summary(cur)
                
            conn.commit()
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

