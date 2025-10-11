"""Repository functions for reading from the osm schema."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional

from psycopg import Connection
from psycopg.rows import dict_row


@dataclass
class RoadNode:
    id: int
    lon: float
    lat: float


@dataclass
class RoadEdge:
    id: int
    osm_way_id: int
    source: int
    target: int
    length_m: float


def iter_nodes(conn: Connection, limit: Optional[int] = None) -> Iterable[RoadNode]:
    sql = "SELECT id, ST_X(geom) AS lon, ST_Y(geom) AS lat FROM osm.nodes ORDER BY id"
    if limit:
        sql += " LIMIT %s"
        params = (limit,)
    else:
        params = None

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, params)
        for row in cur:
            yield RoadNode(id=row["id"], lon=row["lon"], lat=row["lat"])


def iter_edges(conn: Connection, limit: Optional[int] = None) -> Iterable[RoadEdge]:
    sql = """
        SELECT id, osm_way_id, source, target,
               ST_Length(geom::geography) AS length_m
        FROM osm.road_edges
        ORDER BY id
    """
    if limit:
        sql += " LIMIT %s"
        params = (limit,)
    else:
        params = None

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, params)
        for row in cur:
            yield RoadEdge(
                id=row["id"],
                osm_way_id=row["osm_way_id"],
                source=row["source"],
                target=row["target"],
                length_m=row["length_m"],
            )
