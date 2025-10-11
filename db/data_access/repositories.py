"""Repository functions for reading from the osm schema."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, Optional, Sequence

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
    sql = "SELECT id, ST_X(the_geom) AS lon, ST_Y(the_geom) AS lat FROM osm.road_edges_vertices_pgr ORDER BY id"
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


def find_nearest_node(
    conn: Connection,
    lon: float,
    lat: float,
    *,
    max_distance_m: Optional[float] = None,
) -> Optional[RoadNode]:
    """Return the closest road node to the provided coordinate."""

    query = (
        """
        SELECT id,
               ST_X(the_geom) AS lon,
               ST_Y(the_geom) AS lat,
               ST_Distance(
                   the_geom::geography,
                   ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography
               ) AS distance_m
        FROM osm.road_edges_vertices_pgr
        ORDER BY the_geom <-> ST_SetSRID(ST_MakePoint(%s, %s), 4326)
        LIMIT 1
        """
    )

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(query, (lon, lat, lon, lat))
        row = cur.fetchone()

    if not row:
        return None

    if max_distance_m is not None and row["distance_m"] is not None and row["distance_m"] > max_distance_m:
        return None

    return RoadNode(id=row["id"], lon=row["lon"], lat=row["lat"])


def fetch_nodes_by_ids(conn: Connection, node_ids: Sequence[int]) -> Dict[int, RoadNode]:
    """Fetch nodes for the given identifiers as a mapping."""

    if not node_ids:
        return {}

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT id, ST_X(the_geom) AS lon, ST_Y(the_geom) AS lat
            FROM osm.road_edges_vertices_pgr
            WHERE id = ANY(%s)
            """,
            (list(node_ids),),
        )
        rows = cur.fetchall()

    return {row["id"]: RoadNode(id=row["id"], lon=row["lon"], lat=row["lat"]) for row in rows}
