"""Higher-level services combining repository results and pgRouting."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Sequence, Tuple

from psycopg import Connection

from .pgrouting import RouteSegment, shortest_path
from .repositories import (
    RoadNode,
    fetch_nodes_by_ids,
    find_nearest_node,
    iter_nodes,
)


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


@dataclass
class RouteResult:
    start: RoadNode
    end: RoadNode
    coordinates: List[Tuple[float, float]]
    total_cost: float
    segments: Sequence[RouteSegment]


def _snap_point(
    conn: Connection,
    lon: float,
    lat: float,
    *,
    distances_m: Iterable[float],
) -> RoadNode | None:
    for distance in distances_m:
        node = find_nearest_node(conn, lon, lat, max_distance_m=distance)
        if node is not None:
            return node
    return None


def compute_route_between_points(
    conn: Connection,
    start_lon: float,
    start_lat: float,
    end_lon: float,
    end_lat: float,
    *,
    snap_distance_m: float = 1000.0,
    fallback_snap_distances_m: Sequence[float] = (2500.0, 5000.0),
) -> RouteResult:
    """Compute the shortest path between two coordinates.

    The coordinates are snapped to the nearest OSM nodes. Raises ValueError when
    either coordinate is too far away or no path exists between snapped nodes.
    """

    snap_candidates: List[float] = [snap_distance_m, *fallback_snap_distances_m]

    start_node = _snap_point(conn, start_lon, start_lat, distances_m=snap_candidates)
    if start_node is None:
        raise ValueError("Could not locate a road vertex near the start point.")

    end_node = _snap_point(conn, end_lon, end_lat, distances_m=snap_candidates)
    if end_node is None:
        raise ValueError("Could not locate a road vertex near the end point.")

    if start_node.id == end_node.id:
        return RouteResult(
            start=start_node,
            end=end_node,
            coordinates=[(start_node.lon, start_node.lat)],
            total_cost=0.0,
            segments=[],
        )

    route = compute_route(conn, start_node.id, end_node.id)
    if not route.segments:
        raise ValueError("No path found between the selected vertices.")

    node_ids = [segment.node_id for segment in route.segments]
    nodes = fetch_nodes_by_ids(conn, node_ids)

    coordinates: List[Tuple[float, float]] = []
    for segment in route.segments:
        node = nodes.get(segment.node_id)
        if node:
            coordinates.append((node.lon, node.lat))

    if not coordinates:
        raise ValueError("Route segments resolved without coordinate data.")

    return RouteResult(
        start=start_node,
        end=end_node,
        coordinates=coordinates,
        total_cost=route.total_cost,
        segments=route.segments,
    )
