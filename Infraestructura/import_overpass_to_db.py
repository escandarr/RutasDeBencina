"""Import OpenStreetMap road network from Overpass API into PostGIS/pgRouting.

This script downloads (or reads from disk) the same dataset produced by
``extract_infra.py`` and persists it into the Postgres schema defined in
``db/schema.sql``. It takes care of inserting nodes, ways, way_nodes and
road_edges, recording an import run, and refreshing pgRouting topology.

Usage (inside the repo virtualenv):

    python Infraestructura/import_overpass_to_db.py --dsn postgresql://user:pass@localhost:5432/rutasdb

You can also rely on standard libpq environment variables (PGHOST, PGPORT,
PGUSER, PGPASSWORD, PGDATABASE) or a ``DATABASE_URL`` entry.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import pathlib
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import requests

try:
    import psycopg
    from psycopg import Cursor
    from psycopg.types.json import Json
except ImportError as exc:
    raise SystemExit(
        "psycopg (v3) is required. Install it with `pip install psycopg[binary]`."
    ) from exc

BASE_DIR = pathlib.Path(__file__).resolve().parent
DEFAULT_JSON = BASE_DIR / "infra_raw.json"
DEFAULT_GEOJSON = BASE_DIR / "infra.geojson"

DEFAULT_BBOX = (-41.9, -74.0, -33.0, -70.2)
OVERPASS_URL = "https://overpass-api.de/api/interpreter"
DEFAULT_TIMEOUT = 180

LOG = logging.getLogger("import_overpass")


@dataclass
class BBox:
    lat_min: float
    lon_min: float
    lat_max: float
    lon_max: float

    @classmethod
    def from_arg(cls, raw: Sequence[str]) -> "BBox":
        if len(raw) != 4:
            raise ValueError("Bounding box must provide exactly four numbers")
        lat_min, lon_min, lat_max, lon_max = map(float, raw)
        if lat_min >= lat_max or lon_min >= lon_max:
            raise ValueError("Bounding box values must be ordered: lat_min < lat_max and lon_min < lon_max")
        return cls(lat_min, lon_min, lat_max, lon_max)

    @property
    def tuple(self) -> Tuple[float, float, float, float]:
        return self.lat_min, self.lon_min, self.lat_max, self.lon_max

    def to_wkt(self) -> str:
        points = [
            (self.lon_min, self.lat_min),
            (self.lon_max, self.lat_min),
            (self.lon_max, self.lat_max),
            (self.lon_min, self.lat_max),
            (self.lon_min, self.lat_min),
        ]
        coords = ", ".join(f"{lon} {lat}" for lon, lat in points)
        return f"POLYGON(({coords}))"

    def __str__(self) -> str:
        return f"{self.lat_min},{self.lon_min},{self.lat_max},{self.lon_max}"


def build_overpass_query(bbox: BBox) -> str:
    lat_min, lon_min, lat_max, lon_max = bbox.tuple
    return (
        f"[out:json][timeout:{DEFAULT_TIMEOUT}];\n"
        f"way[\"highway\"~\"motorway|trunk|primary|secondary|tertiary\"]"
        f"({lat_min},{lon_min},{lat_max},{lon_max});\n"
        "(._;>;);\n"
        "out geom;\n"
    )


def resolve_conninfo(dsn: Optional[str]) -> str:
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


def fetch_overpass_data(bbox: BBox, *, session: Optional[requests.Session] = None, save_json: Optional[pathlib.Path] = None) -> Dict:
    query = build_overpass_query(bbox)
    sess = session or requests.Session()
    LOG.info("Requesting Overpass data for bbox=%s", bbox)
    response = sess.post(OVERPASS_URL, data=query, timeout=DEFAULT_TIMEOUT + 60)
    response.raise_for_status()
    data = response.json()
    if save_json:
        save_json.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        LOG.info("Saved raw Overpass response to %s", save_json)
    return data


def parse_maxspeed(raw: Optional[str]) -> Optional[int]:
    if not raw:
        return None
    match = re.search(r"(\d+)", raw)
    if match:
        return int(match.group(1))
    return None


def insert_osm_data(
    cur: Cursor,
    elements: Sequence[Dict],
    bbox: BBox,
    *,
    truncate: bool,
    default_speed: int,
    import_source: str,
    query: str,
    refresh_topology: bool,
) -> None:
    nodes: Dict[int, Dict] = {}
    ways: List[Dict] = []

    for el in elements:
        if el.get("type") == "node":
            nodes[el["id"]] = el
        elif el.get("type") == "way":
            if not el.get("geometry"):
                continue
            ways.append(el)

    LOG.info("Preparing to import %s nodes and %s ways", len(nodes), len(ways))

    if truncate:
        LOG.info("Truncating existing osm tables")
        cur.execute("TRUNCATE TABLE osm.road_edges, osm.way_nodes, osm.ways, osm.nodes RESTART IDENTITY CASCADE")

    cur.execute(
        """
        INSERT INTO osm.import_runs (source, query, bbox, element_total, notes)
        VALUES (%s, %s, ST_GeomFromText(%s, 4326), %s, %s)
        RETURNING id
        """,
        (
            import_source,
            query,
            bbox.to_wkt(),
            len(elements),
            f"{len(ways)} ways, {len(nodes)} nodes",
        ),
    )
    import_run_id = cur.fetchone()[0]
    LOG.info("Recorded import run id=%s", import_run_id)

    node_records = [
        (
            node_id,
            el.get("version"),
            Json(el.get("tags") or {}),
            el["lon"],
            el["lat"],
        )
        for node_id, el in nodes.items()
    ]
    LOG.debug("Inserting %s nodes", len(node_records))
    cur.executemany(
        """
        INSERT INTO osm.nodes (id, version, tags, geom, created_at, updated_at)
        VALUES (%s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326), NOW(), NOW())
        ON CONFLICT (id) DO UPDATE SET
            version = EXCLUDED.version,
            tags = EXCLUDED.tags,
            geom = EXCLUDED.geom,
            updated_at = NOW()
        """,
        node_records,
    )

    way_records = []
    way_node_records: List[Tuple[int, int, int, float, float]] = []
    road_edge_records: List[Tuple[int, int, bool, Optional[int], Optional[float], Optional[float], str, Json]] = []

    for way in ways:
        way_id = way["id"]
        tags = way.get("tags") or {}
        geom = way.get("geometry") or []
        way_nodes_seq = way.get("nodes") or []

        if len(geom) < 2 or len(way_nodes_seq) < 2:
            continue

        maxspeed = parse_maxspeed(tags.get("maxspeed"))
        oneway_tag = (tags.get("oneway") or "no").lower()
        directed = oneway_tag in {"yes", "true", "1", "-1", "reverse"}
        reverse_only = oneway_tag in {"-1", "reverse"}

        coords = geom if not reverse_only else list(reversed(geom))
        node_seq = way_nodes_seq if not reverse_only else list(reversed(way_nodes_seq))
        linestring_wkt = "LINESTRING(" + ", ".join(f"{pt['lon']} {pt['lat']}" for pt in coords) + ")"

        way_records.append(
            (
                way_id,
                way.get("version"),
                Json(tags),
                directed,
                maxspeed,
                tags.get("highway"),
                linestring_wkt,
            )
        )

        for seq, node_id in enumerate(node_seq):
            point = coords[seq]
            way_node_records.append((way_id, node_id, seq, point["lon"], point["lat"]))

        for idx in range(len(node_seq) - 1):
            p1 = coords[idx]
            p2 = coords[idx + 1]
            segment_wkt = f"LINESTRING({p1['lon']} {p1['lat']}, {p2['lon']} {p2['lat']})"
            road_edge_records.append(
                (
                    way_id,
                    import_run_id,
                    directed,
                    maxspeed,
                    None,
                    None,
                    segment_wkt,
                    Json(tags),
                )
            )

    LOG.debug("Inserting %s ways", len(way_records))
    cur.executemany(
        """
        INSERT INTO osm.ways (id, version, tags, oneway, maxspeed_kph, road_class, geom, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, ST_GeomFromText(%s, 4326), NOW(), NOW())
        ON CONFLICT (id) DO UPDATE SET
            version = EXCLUDED.version,
            tags = EXCLUDED.tags,
            oneway = EXCLUDED.oneway,
            maxspeed_kph = EXCLUDED.maxspeed_kph,
            road_class = EXCLUDED.road_class,
            geom = EXCLUDED.geom,
            updated_at = NOW()
        """,
        way_records,
    )

    LOG.debug("Inserting %s way_nodes", len(way_node_records))
    cur.executemany(
        """
        INSERT INTO osm.way_nodes (way_id, node_id, seq, geom)
        VALUES (%s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326))
        ON CONFLICT (way_id, seq) DO UPDATE SET
            node_id = EXCLUDED.node_id,
            geom = EXCLUDED.geom
        """,
        way_node_records,
    )

    LOG.debug("Inserting %s road segments", len(road_edge_records))
    cur.executemany(
        """
        INSERT INTO osm.road_edges (osm_way_id, import_run_id, directed, speed_kph, cost, reverse_cost, geom, tags)
        VALUES (%s, %s, %s, %s, %s, %s, ST_GeomFromText(%s, 4326), %s)
        ON CONFLICT (id) DO NOTHING
        """,
        road_edge_records,
    )

    LOG.info("Updating edge costs (%s km/h default)", default_speed)
    cur.execute(
        """
        UPDATE osm.road_edges
        SET
            cost = ST_Length(geom::geography) / NULLIF(COALESCE(speed_kph, %s) * 1000.0 / 3600.0, 0),
            reverse_cost = CASE
                WHEN directed THEN 1e9
                ELSE ST_Length(geom::geography) / NULLIF(COALESCE(speed_kph, %s) * 1000.0 / 3600.0, 0)
            END
        WHERE import_run_id = %s
        """,
        (default_speed, default_speed, import_run_id),
    )

    if refresh_topology:
        LOG.info("Refreshing pgRouting topology")
        cur.execute("SELECT osm.refresh_road_topology(%s)", (0.00001,))
    else:
        LOG.info("Skipping pgRouting topology refresh as requested")


def write_geojson(edges: Iterable[Dict], destination: pathlib.Path) -> None:
    features = []
    for edge in edges:
        coords = [(p["lon"], p["lat"]) for p in edge.get("geometry", [])]
        features.append(
            {
                "type": "Feature",
                "geometry": {"type": "LineString", "coordinates": coords},
                "properties": {
                    key: edge.get(key)
                    for key in ("id", "name", "highway", "oneway", "maxspeed")
                },
            }
        )
    payload = {"type": "FeatureCollection", "features": features}
    destination.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    LOG.info("Saved GeoJSON preview to %s", destination)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", help="PostgreSQL connection string")
    parser.add_argument("--bbox", nargs=4, metavar=("LAT_MIN", "LON_MIN", "LAT_MAX", "LON_MAX"), help="Bounding box to query")
    parser.add_argument("--input", type=pathlib.Path, help="Load Overpass JSON from file instead of downloading")
    parser.add_argument("--save-json", type=pathlib.Path, default=DEFAULT_JSON, help="Where to save raw Overpass JSON (default: %(default)s)")
    parser.add_argument("--geojson", type=pathlib.Path, default=DEFAULT_GEOJSON, help="Where to save preview GeoJSON (default: %(default)s)")
    parser.add_argument("--default-speed", type=int, default=60, help="Fallback speed (km/h) when maxspeed is missing")
    parser.add_argument("--no-truncate", action="store_true", help="Do not truncate osm tables before inserting")
    parser.add_argument("--skip-topology", action="store_true", help="Skip pgRouting topology refresh (advanced)")
    parser.add_argument("--log-level", default="INFO", help="Logging level (default: INFO)")

    args = parser.parse_args(list(argv) if argv is not None else None)

    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO), format="%(levelname)s %(message)s")

    bbox = BBox.from_arg(args.bbox) if args.bbox else BBox(*DEFAULT_BBOX)

    if args.input and not args.input.exists():
        parser.error(f"Input file {args.input} does not exist")

    if args.input:
        LOG.info("Loading data from %s", args.input)
        elements = json.loads(args.input.read_text(encoding="utf-8")).get("elements", [])
        query = "loaded from file"
    else:
        raw = fetch_overpass_data(bbox, save_json=args.save_json)
        elements = raw.get("elements", [])
        query = build_overpass_query(bbox)

    if not elements:
        LOG.warning("No elements returned; nothing to import")
        return 0

    conninfo = resolve_conninfo(args.dsn)
    LOG.info("Connecting to PostgreSQL with %s", conninfo)

    with psycopg.connect(conninfo) as conn:
        with conn.cursor() as cur:
            insert_osm_data(
                cur,
                elements,
                bbox,
                truncate=not args.no_truncate,
                default_speed=args.default_speed,
                import_source=OVERPASS_URL if not args.input else str(args.input),
                query=query,
                refresh_topology=not args.skip_topology,
            )
        conn.commit()

    if args.geojson:
        raw_edges = []
        for el in elements:
            if el.get("type") == "way" and el.get("geometry"):
                tags = el.get("tags") or {}
                raw_edges.append(
                    {
                        "id": el["id"],
                        "geometry": el["geometry"],
                        "name": tags.get("name"),
                        "highway": tags.get("highway"),
                        "oneway": tags.get("oneway"),
                        "maxspeed": tags.get("maxspeed"),
                    }
                )
        write_geojson(raw_edges, args.geojson)

    LOG.info("Import complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
