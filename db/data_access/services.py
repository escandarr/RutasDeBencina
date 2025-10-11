"""Higher-level services combining repository results and pgRouting."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from psycopg import Connection

from .pgrouting import RouteSegment, shortest_path
from .repositories import iter_nodes


@dataclass
class Route:
    segments: list[RouteSegment]

    @property
    def total_cost(self) -> float:
        return self.segments[-1].agg_cost if self.segments else 0.0


def compute_route(conn: Connection, start_vertex: int, end_vertex: int) -> Route:
    segments = list(shortest_path(conn, start_vertex, end_vertex))
    return Route(segments=segments)


def count_nodes(conn: Connection) -> int:
    return sum(1 for _ in iter_nodes(conn))
