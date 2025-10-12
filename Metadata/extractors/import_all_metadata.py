"""Import all metadata (CNE stations, prices, and promotions) into PostgreSQL.

This is a convenience script that runs both import_cne_to_db.py and 
import_promos_to_db.py in sequence. It's the equivalent of running both
scripts individually.

Usage (inside the repo virtualenv):

    python Metadata/extractors/import_all_metadata.py
    python Metadata/extractors/import_all_metadata.py --dsn postgresql://user:pass@localhost:5432/rutasdb
    python Metadata/extractors/import_all_metadata.py --truncate-all

You can also rely on standard libpq environment variables (PGHOST, PGPORT,
PGUSER, PGPASSWORD, PGDATABASE) or a DATABASE_URL entry.
"""
from __future__ import annotations

import argparse
import logging
import sys
from typing import Optional, Sequence

# Import the individual import modules
import import_cne_to_db
import import_promos_to_db

LOG = logging.getLogger("import_all_metadata")


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Main import routine that orchestrates all metadata imports."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", help="PostgreSQL connection string")
    parser.add_argument(
        "--truncate-all",
        action="store_true",
        help="Truncate tables before importing (removes history)"
    )
    parser.add_argument(
        "--skip-cne",
        action="store_true",
        help="Skip CNE import (stations and prices)"
    )
    parser.add_argument(
        "--skip-promos",
        action="store_true",
        help="Skip promotions import"
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level (default: INFO)"
    )

    args = parser.parse_args(list(argv) if argv is not None else None)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(levelname)s [%(name)s] %(message)s"
    )

    LOG.info("=" * 70)
    LOG.info("🚀 RutasDeBencina - Complete Metadata Import")
    LOG.info("=" * 70)
    
    errors = []
    
    # Import CNE data (stations + prices)
    if not args.skip_cne:
        LOG.info("")
        LOG.info("📍 STEP 1: Importing CNE Stations and Prices")
        LOG.info("-" * 70)
        
        cne_args = ["--log-level", args.log_level]
        if args.dsn:
            cne_args.extend(["--dsn", args.dsn])
        if args.truncate_all:
            cne_args.append("--truncate-prices")
        
        try:
            exit_code = import_cne_to_db.main(cne_args)
            if exit_code != 0:
                errors.append("CNE import failed")
                LOG.error("❌ CNE import failed with exit code %s", exit_code)
            else:
                LOG.info("✅ CNE import completed successfully")
        except Exception as e:
            errors.append(f"CNE import exception: {e}")
            LOG.error("❌ CNE import raised exception: %s", e)
    else:
        LOG.info("⏭️  Skipping CNE import")
    
    # Import promotions
    if not args.skip_promos:
        LOG.info("")
        LOG.info("🎁 STEP 2: Importing Promotions")
        LOG.info("-" * 70)
        
        promos_args = ["--log-level", args.log_level]
        if args.dsn:
            promos_args.extend(["--dsn", args.dsn])
        if args.truncate_all:
            promos_args.append("--truncate")
        
        try:
            exit_code = import_promos_to_db.main(promos_args)
            if exit_code != 0:
                errors.append("Promotions import failed")
                LOG.error("❌ Promotions import failed with exit code %s", exit_code)
            else:
                LOG.info("✅ Promotions import completed successfully")
        except Exception as e:
            errors.append(f"Promotions import exception: {e}")
            LOG.error("❌ Promotions import raised exception: %s", e)
    else:
        LOG.info("⏭️  Skipping promotions import")
    
    # Final summary
    LOG.info("")
    LOG.info("=" * 70)
    if errors:
        LOG.error("❌ Import completed with %s error(s):", len(errors))
        for error in errors:
            LOG.error("   - %s", error)
        LOG.info("=" * 70)
        return 1
    else:
        LOG.info("✅ All metadata imported successfully!")
        LOG.info("=" * 70)
        LOG.info("")
        LOG.info("💡 Next steps:")
        LOG.info("   - Check the database for imported data")
        LOG.info("   - Use the data_access module to query the metadata")
        LOG.info("   - Build routes with fuel cost calculations")
        LOG.info("")
        return 0


if __name__ == "__main__":
    sys.exit(main())

