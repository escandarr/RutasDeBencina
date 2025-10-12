"""Import CNE gas stations and fuel prices into PostgreSQL.

This script reads the JSON output from extract_cne.py and persists it into the
metadata schema defined in db/schema.sql. It handles stations, prices, and
automatic brand linking.

Usage (inside the repo virtualenv):

    python Metadata/extractors/import_cne_to_db.py
    python Metadata/extractors/import_cne_to_db.py --input ../outputs/cne/cne_estaciones_normalizado.json
    python Metadata/extractors/import_cne_to_db.py --dsn postgresql://user:pass@localhost:5432/rutasdb

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
from datetime import datetime, date, time as dt_time
from decimal import Decimal
from typing import Dict, List, Optional, Sequence

try:
    import psycopg
    from psycopg import Cursor
    from psycopg.types.json import Json
except ImportError as exc:
    raise SystemExit(
        "psycopg (v3) is required. Install it with `pip install psycopg[binary]`."
    ) from exc

BASE_DIR = pathlib.Path(__file__).resolve().parent
DEFAULT_INPUT = BASE_DIR.parent / "outputs" / "cne" / "cne_estaciones_normalizado.json"

LOG = logging.getLogger("import_cne")


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


def create_scrape_run(cur: Cursor, record_count: int, source_url: str = "https://api.cne.cl/api/v4/estaciones") -> int:
    """Create a scrape run record and return its ID."""
    cur.execute(
        """
        INSERT INTO metadata.scrape_runs (source_type, source_url, record_count, success)
        VALUES (%s, %s, %s, %s)
        RETURNING id
        """,
        ('cne', source_url, record_count, True)
    )
    scrape_run_id = cur.fetchone()[0]
    LOG.info("Created scrape run id=%s", scrape_run_id)
    return scrape_run_id


def upsert_estaciones(cur: Cursor, estaciones: List[Dict], scrape_run_id: int) -> int:
    """Insert or update gas stations. Returns count of stations processed."""
    LOG.info("Upserting %s stations", len(estaciones))
    
    station_records = []
    for est in estaciones:
        lat = None
        lng = None
        if est.get('lat'):
            try:
                lat = float(est['lat'])
            except (ValueError, TypeError):
                pass
        if est.get('lng'):
            try:
                lng = float(est['lng'])
            except (ValueError, TypeError):
                pass
        
        station_records.append((
            est.get('codigo'),
            est.get('marca'),
            est.get('razon_social'),
            est.get('direccion'),
            est.get('region'),
            est.get('cod_region'),
            est.get('comuna'),
            est.get('cod_comuna'),
            lat,
            lng,
            scrape_run_id
        ))
    
    cur.executemany(
        """
        INSERT INTO metadata.estaciones_cne 
            (codigo, marca, razon_social, direccion, region, cod_region, 
             comuna, cod_comuna, lat, lng, scrape_run_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (codigo) DO UPDATE SET
            marca = EXCLUDED.marca,
            razon_social = EXCLUDED.razon_social,
            direccion = EXCLUDED.direccion,
            region = EXCLUDED.region,
            cod_region = EXCLUDED.cod_region,
            comuna = EXCLUDED.comuna,
            cod_comuna = EXCLUDED.cod_comuna,
            lat = EXCLUDED.lat,
            lng = EXCLUDED.lng,
            scrape_run_id = EXCLUDED.scrape_run_id,
            updated_at = NOW()
        """,
        station_records
    )
    
    LOG.info("✅ Upserted %s stations", len(station_records))
    return len(station_records)


def insert_precios(cur: Cursor, estaciones: List[Dict], scrape_run_id: int) -> int:
    """Insert fuel prices from station data. Returns count of prices inserted."""
    LOG.info("Inserting fuel prices")
    
    # Build mapping of codigo -> estacion_id
    cur.execute("SELECT id, codigo FROM metadata.estaciones_cne")
    codigo_to_id = {row[1]: row[0] for row in cur.fetchall()}
    
    price_records = []
    
    for est in estaciones:
        codigo = est.get('codigo')
        if not codigo or codigo not in codigo_to_id:
            continue
        
        estacion_id = codigo_to_id[codigo]
        
        # Process each fuel type
        for fuel_key in ['precio_93', 'precio_95', 'precio_97', 'precio_DI']:
            precio_data = est.get(fuel_key)
            if not precio_data or not isinstance(precio_data, dict):
                continue
            
            precio_val = precio_data.get('precio')
            if precio_val is None:
                continue
            
            # Convert price (CNE sends in larger units, divide by 1000)
            try:
                precio_decimal = Decimal(str(precio_val)) / 1000
            except (ValueError, TypeError, ArithmeticError):
                continue
            
            tipo_combustible = fuel_key.replace('precio_', '')
            
            # Parse date and time
            fecha = None
            hora = None
            
            fecha_str = precio_data.get('fecha')
            if fecha_str:
                try:
                    fecha = datetime.strptime(fecha_str, '%Y-%m-%d').date()
                except (ValueError, TypeError):
                    pass
            
            hora_str = precio_data.get('hora')
            if hora_str:
                try:
                    hora = datetime.strptime(hora_str, '%H:%M:%S').time()
                except (ValueError, TypeError):
                    pass
            
            price_records.append((
                estacion_id,
                tipo_combustible,
                precio_decimal,
                precio_data.get('unidad'),
                fecha,
                hora,
                precio_data.get('tipo_atencion'),
                scrape_run_id
            ))
    
    if price_records:
        cur.executemany(
            """
            INSERT INTO metadata.precios_combustible
                (estacion_id, tipo_combustible, precio, unidad, fecha, hora, tipo_atencion, scrape_run_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            price_records
        )
        LOG.info("✅ Inserted %s price records", len(price_records))
    else:
        LOG.warning("No valid price records found")
    
    return len(price_records)


def show_summary(cur: Cursor) -> None:
    """Display summary statistics after import."""
    LOG.info("=" * 60)
    LOG.info("IMPORT SUMMARY")
    LOG.info("=" * 60)
    
    # Count stations
    cur.execute("SELECT COUNT(*) FROM metadata.estaciones_cne")
    station_count = cur.fetchone()[0]
    LOG.info("⛽ Total Gas Stations: %s", station_count)
    
    # Count prices
    cur.execute("SELECT COUNT(*) FROM metadata.precios_combustible")
    price_count = cur.fetchone()[0]
    LOG.info("💰 Total Price Records: %s", price_count)
    
    # Count stations by brand
    LOG.info("")
    LOG.info("📍 Stations per Brand:")
    cur.execute("""
        SELECT m.nombre_display, COUNT(e.id) as count
        FROM metadata.marcas m
        LEFT JOIN metadata.estaciones_cne e ON m.id = e.marca_id
        GROUP BY m.nombre_display
        HAVING COUNT(e.id) > 0
        ORDER BY count DESC
    """)
    for row in cur.fetchall():
        LOG.info("   %s: %s stations", row[0], row[1])
    
    # Show average prices by fuel type
    LOG.info("")
    LOG.info("💵 Latest Average Prices:")
    cur.execute("""
        SELECT tipo_combustible, 
               ROUND(AVG(precio)::numeric, 2) as avg_price,
               COUNT(DISTINCT estacion_id) as station_count
        FROM metadata.precios_actuales
        WHERE precio IS NOT NULL
        GROUP BY tipo_combustible
        ORDER BY tipo_combustible
    """)
    for row in cur.fetchall():
        LOG.info("   %s: $%s/L (from %s stations)", row[0], row[1], row[2])


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Main import routine."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", help="PostgreSQL connection string")
    parser.add_argument(
        "--input", 
        type=pathlib.Path, 
        default=DEFAULT_INPUT,
        help="Path to CNE JSON file (default: %(default)s)"
    )
    parser.add_argument(
        "--truncate-prices",
        action="store_true",
        help="Truncate price table before importing (keeps history by default)"
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

    if not args.input.exists():
        LOG.error("Input file not found: %s", args.input)
        LOG.error("Run extract_cne.py first to generate the data file")
        return 1

    LOG.info("Loading CNE data from %s", args.input)
    try:
        with open(args.input, 'r', encoding='utf-8') as f:
            estaciones = json.load(f)
    except json.JSONDecodeError as e:
        LOG.error("Failed to parse JSON: %s", e)
        return 1

    if not isinstance(estaciones, list):
        LOG.error("Expected a list of stations, got %s", type(estaciones))
        return 1

    if not estaciones:
        LOG.warning("No stations found in input file")
        return 0

    LOG.info("Loaded %s stations", len(estaciones))

    conninfo = resolve_conninfo(args.dsn)
    LOG.info("Connecting to PostgreSQL...")

    try:
        with psycopg.connect(conninfo) as conn:
            with conn.cursor() as cur:
                # Create scrape run
                scrape_run_id = create_scrape_run(cur, len(estaciones))
                
                # Truncate prices if requested
                if args.truncate_prices:
                    LOG.info("Truncating precios_combustible table")
                    cur.execute("TRUNCATE TABLE metadata.precios_combustible RESTART IDENTITY")
                
                # Import stations
                station_count = upsert_estaciones(cur, estaciones, scrape_run_id)
                
                # Import prices
                price_count = insert_precios(cur, estaciones, scrape_run_id)
                
                # Show summary
                show_summary(cur)
                
            conn.commit()
            LOG.info("=" * 60)
            LOG.info("✅ Import complete!")
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

