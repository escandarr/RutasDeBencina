"""Convenience wrappers around pgRouting stored procedures."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional, Sequence

from psycopg import Connection
from psycopg.rows import tuple_row


@dataclass
class RouteSegment:
    seq: int
    path_id: int
    edge_id: int
    cost: float
    agg_cost: float


def shortest_path(
    conn: Connection,
    start_vertex: int,
    end_vertex: int,
    *,
    directed: bool = True,
    edge_table: str = "osm.road_edges_pgr",
    columns: Sequence[str] = ("id", "source", "target", "cost", "reverse_cost"),
) -> Iterable[RouteSegment]:
    """Run pgr_dijkstra between two vertices and yield route segments."""

    sql = """
        SELECT seq, path_id, edge, cost, agg_cost
        FROM pgr_dijkstra(
            %(sql)s,
            %(start)s,
            %(end)s,
            %(directed)s
        )
    """
    subquery = f"SELECT {', '.join(columns)} FROM {edge_table}"
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute(
            sql,
            {
                "sql": subquery,
                "start": start_vertex,
                "end": end_vertex,
                "directed": directed,
            },
        )
        for seq, path_id, edge_id, cost, agg_cost in cur:
            yield RouteSegment(seq=seq, path_id=path_id, edge_id=edge_id, cost=cost, agg_cost=agg_cost)
