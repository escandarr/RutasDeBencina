#!/usr/bin/env python3
"""Import scraped metadata (CNE stations, prices, promotions) into the database."""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict, List

from connection import Database
from metadata_repositories import (
    bulk_import_estaciones_from_cne,
    bulk_import_precios_from_cne,
    bulk_import_promociones,
    create_scrape_run,
)

# Default paths to metadata outputs
METADATA_DIR = Path(__file__).parent.parent.parent / "Metadata" / "outputs"
CNE_FILE = METADATA_DIR / "cne" / "cne_estaciones_normalizado.json"
PROMOS_ALIADOS_FILE = METADATA_DIR / "promos" / "promos_aliados.json"
PROMOS_ESTATICO_FILE = METADATA_DIR / "promos" / "promos_estatico.json"


def load_json(file_path: Path) -> List[Dict[str, Any]]:
    """Load JSON data from a file."""
    if not file_path.exists():
        print(f"⚠️  File not found: {file_path}")
        return []
    
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
        if not isinstance(data, list):
            return []
        return data


def import_cne_data(db: Database) -> None:
    """Import CNE gas stations and prices."""
    print(f"📥 Loading CNE data from {CNE_FILE}")
    estaciones_data = load_json(CNE_FILE)
    
    if not estaciones_data:
        print("❌ No CNE data to import")
        return
    
    print(f"✅ Loaded {len(estaciones_data)} stations")
    
    with db.connection() as conn:
        # Create scrape run record
        scrape_run_id = create_scrape_run(
            conn,
            source_type='cne',
            source_url='https://api.cne.cl/api/v4/estaciones',
            record_count=len(estaciones_data),
            success=True
        )
        print(f"📝 Created scrape run #{scrape_run_id}")
        
        # Import stations
        print("🏢 Importing stations...")
        station_count = bulk_import_estaciones_from_cne(conn, estaciones_data, scrape_run_id)
        print(f"✅ Imported {station_count} stations")
        
        # Import prices
        print("💰 Importing prices...")
        price_count = bulk_import_precios_from_cne(conn, estaciones_data, scrape_run_id)
        print(f"✅ Imported {price_count} price records")


def import_promociones_data(db: Database) -> None:
    """Import promotions from both aliados and estatico sources."""
    
    # Import aliados promotions
    print(f"📥 Loading aliados promotions from {PROMOS_ALIADOS_FILE}")
    aliados_data = load_json(PROMOS_ALIADOS_FILE)
    
    if aliados_data:
        print(f"✅ Loaded {len(aliados_data)} aliados promotions")
        with db.connection() as conn:
            scrape_run_id = create_scrape_run(
                conn,
                source_type='promos_aliados',
                record_count=len(aliados_data),
                success=True
            )
            count = bulk_import_promociones(conn, aliados_data, 'aliados', scrape_run_id)
            print(f"✅ Imported {count} aliados promotions")
    
    # Import estatico promotions
    print(f"📥 Loading estatico promotions from {PROMOS_ESTATICO_FILE}")
    estatico_data = load_json(PROMOS_ESTATICO_FILE)
    
    if estatico_data:
        print(f"✅ Loaded {len(estatico_data)} estatico promotions")
        with db.connection() as conn:
            scrape_run_id = create_scrape_run(
                conn,
                source_type='promos_estatico',
                record_count=len(estatico_data),
                success=True
            )
            count = bulk_import_promociones(conn, estatico_data, 'estatico', scrape_run_id)
            print(f"✅ Imported {count} estatico promotions")


def show_summary(db: Database) -> None:
    """Show summary statistics after import."""
    print("\n" + "="*60)
    print("📊 IMPORT SUMMARY")
    print("="*60)
    
    with db.connection() as conn:
        with conn.cursor() as cur:
            # Count brands
            cur.execute("SELECT COUNT(*) FROM metadata.marcas WHERE activo = TRUE")
            marca_count = cur.fetchone()[0]
            print(f"🏷️  Brands (marcas): {marca_count}")
            
            # Count stations
            cur.execute("SELECT COUNT(*) FROM metadata.estaciones_cne")
            station_count = cur.fetchone()[0]
            print(f"⛽ Gas Stations: {station_count}")
            
            # Count prices
            cur.execute("SELECT COUNT(*) FROM metadata.precios_combustible")
            price_count = cur.fetchone()[0]
            print(f"💰 Price Records: {price_count}")
            
            # Count promotions
            cur.execute("SELECT COUNT(*) FROM metadata.promociones WHERE activo = TRUE")
            promo_count = cur.fetchone()[0]
            print(f"🎁 Active Promotions: {promo_count}")
            
            # Count promotion-brand links
            cur.execute("SELECT COUNT(*) FROM metadata.promociones_marcas")
            link_count = cur.fetchone()[0]
            print(f"🔗 Promotion-Brand Links: {link_count}")
            
            # Show brands with station counts
            print("\n📍 Stations per Brand:")
            cur.execute("""
                SELECT m.nombre_display, COUNT(e.id) as count
                FROM metadata.marcas m
                LEFT JOIN metadata.estaciones_cne e ON m.id = e.marca_id
                GROUP BY m.nombre_display
                ORDER BY count DESC
            """)
            for row in cur.fetchall():
                print(f"   {row[0]}: {row[1]} stations")
            
            # Show average prices by fuel type
            print("\n💵 Average Prices by Fuel Type:")
            cur.execute("""
                SELECT tipo_combustible, 
                       ROUND(AVG(precio)::numeric, 2) as avg_price,
                       COUNT(*) as count
                FROM metadata.precios_actuales
                WHERE precio IS NOT NULL
                GROUP BY tipo_combustible
                ORDER BY tipo_combustible
            """)
            for row in cur.fetchall():
                print(f"   {row[0]}: ${row[1]} (from {row[2]} stations)")


def main() -> int:
    """Main import routine."""
    print("🚀 RutasDeBencina Metadata Import")
    print("="*60)
    
    # Check if files exist
    if not CNE_FILE.exists():
        print(f"❌ CNE data file not found: {CNE_FILE}")
        print("   Run Metadata/extractors/extract_cne.py first!")
        return 1
    
    try:
        db = Database()
        
        # Import CNE data (stations + prices)
        import_cne_data(db)
        
        # Import promotions
        import_promociones_data(db)
        
        # Show summary
        show_summary(db)
        
        print("\n✅ Import completed successfully!")
        return 0
        
    except Exception as e:
        print(f"\n❌ Import failed: {e}")
        import traceback
        traceback.print_exc()
        return 1
    finally:
        try:
            db.close()
        except:
            pass


if __name__ == "__main__":
    sys.exit(main())

