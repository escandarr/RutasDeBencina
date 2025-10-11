"""CLI helper to test database connectivity."""
from __future__ import annotations

import argparse
import logging

from .connection import Database
from .repositories import iter_edges, iter_nodes


def main() -> None:
    parser = argparse.ArgumentParser(description="Quick checks against the osm schema")
    parser.add_argument("--dsn", help="Override the default connection string")
    parser.add_argument("--sample", type=int, default=5, help="Rows to fetch from each entity (default: 5)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    with Database(dsn=args.dsn) as db:
        with db.connection() as conn:
            logging.info("Nodes sample:")
            for node in iter_nodes(conn, limit=args.sample):
                logging.info("node %s @ (%s, %s)", node.id, node.lon, node.lat)

            logging.info("Edges sample:")
            for edge in iter_edges(conn, limit=args.sample):
                logging.info(
                    "edge %s way=%s %s→%s length=%.2fm",
                    edge.id,
                    edge.osm_way_id,
                    edge.source,
                    edge.target,
                    edge.length_m,
                )


if __name__ == "__main__":
    main()
