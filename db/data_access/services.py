"""Higher-level services combining repository results and pgRouting."""
from __future__ import annotations

from dataclasses import dataclass
from math import atan2, ceil, cos, radians, sin, sqrt
from typing import Iterable, List, Sequence, Tuple

from psycopg import Connection

from .metadata_services import StationPricePoint, get_cheapest_stations_in_bbox
from .pgrouting import RouteSegment, shortest_path
from .repositories import (
    RoadNode,
    fetch_nodes_by_ids,
    find_nearest_node,
    iter_nodes,
)

REFUEL_THRESHOLD_RATIO = 0.4  # Allow refueling once tank is 40% or less
MIN_PROGRESS_KM = 5.0
MIN_SEARCH_RANGE_KM = 15.0
MAX_SEARCH_RANGE_KM = 120.0


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


@dataclass
class VehicleSettings:
    fuel_type: str = "95"
    consumption_km_per_l: float = 12.0
    tank_capacity_l: float | None = None
    tank_level_percent: float | None = None


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


def _haversine_distance_km(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    radius_km = 6371.0
    dlon = radians(lon2 - lon1)
    dlat = radians(lat2 - lat1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return radius_km * c


def _route_distance_km(coordinates: Sequence[Tuple[float, float]]) -> float:
    if len(coordinates) < 2:
        return 0.0
    total = 0.0
    for (lon1, lat1), (lon2, lat2) in zip(coordinates, coordinates[1:]):
        total += _haversine_distance_km(lon1, lat1, lon2, lat2)
    return total


def _interpolate_point_along_route(
    coordinates: Sequence[Tuple[float, float]],
    distance_km: float,
) -> Tuple[float, float] | None:
    if not coordinates:
        return None
    if len(coordinates) == 1 or distance_km <= 0:
        return coordinates[0]

    traversed = 0.0
    for (lon1, lat1), (lon2, lat2) in zip(coordinates, coordinates[1:]):
        seg_dist = _haversine_distance_km(lon1, lat1, lon2, lat2)
        if traversed + seg_dist >= distance_km:
            ratio = 0.0 if seg_dist == 0 else (distance_km - traversed) / seg_dist
            lon = lon1 + (lon2 - lon1) * ratio
            lat = lat1 + (lat2 - lat1) * ratio
            return lon, lat
        traversed += seg_dist

    return coordinates[-1]


def _search_stations_ahead(
    conn: Connection,
    current_lon: float,
    current_lat: float,
    end_lon: float,
    end_lat: float,
    *,
    fuel_type: str,
    range_km: float,
    limit: int = 40,
) -> list[StationPricePoint]:
    lat_padding = max(MIN_SEARCH_RANGE_KM, min(range_km, MAX_SEARCH_RANGE_KM)) / 111.0
    lon_padding = lat_padding / max(0.1, cos(radians(current_lat)))

    min_lat = min(current_lat, end_lat) - lat_padding
    max_lat = max(current_lat, end_lat) + lat_padding
    min_lng = min(current_lon, end_lon) - lon_padding
    max_lng = max(current_lon, end_lon) + lon_padding

    stations = get_cheapest_stations_in_bbox(
        conn,
        fuel_type=fuel_type,
        min_lat=min_lat,
        max_lat=max_lat,
        min_lng=min_lng,
        max_lng=max_lng,
        limit=limit,
    )
    return stations


def _estimate_trip_cost(
    distance_km: float,
    vehicle: VehicleSettings,
    price_per_liter: float | None,
) -> tuple[float | None, float, float]:
    consumption = vehicle.consumption_km_per_l or 12.0
    if consumption <= 0:
        consumption = 12.0
    liters_needed = distance_km / consumption

    tank_capacity = vehicle.tank_capacity_l or 0.0
    level_percent = vehicle.tank_level_percent
    if level_percent is None:
        level_percent = 50.0
    level_percent = max(0.0, min(100.0, level_percent))
    liters_available = tank_capacity * (level_percent / 100.0)

    liters_to_buy = max(0.0, liters_needed - liters_available)
    if price_per_liter is None:
        return None, liters_needed, liters_to_buy

    estimated_cost = liters_to_buy * price_per_liter
    return estimated_cost, liters_needed, liters_to_buy


def _merge_route_results(first: RouteResult, second: RouteResult) -> RouteResult:
    combined_coords: List[Tuple[float, float]] = list(first.coordinates)
    if second.coordinates:
        combined_coords.extend(second.coordinates[1:])
    combined_segments = list(first.segments) + list(second.segments)
    return RouteResult(
        start=first.start,
        end=second.end,
        coordinates=combined_coords,
        total_cost=first.total_cost + second.total_cost,
        segments=combined_segments,
    )


def _merge_route_sequence(routes: Sequence[RouteResult]) -> RouteResult:
    if not routes:
        raise ValueError("No routes to merge")
    merged = routes[0]
    for route in routes[1:]:
        merged = _merge_route_results(merged, route)
    return merged


def _choose_next_station(
    current_lon: float,
    current_lat: float,
    end_lon: float,
    end_lat: float,
    *,
    candidates: Sequence[StationPricePoint],
    visited: set[str],
    max_range_km: float,
    attempt: int = 0,
) -> StationPricePoint | None:
    dist_current_to_end = _haversine_distance_km(current_lon, current_lat, end_lon, end_lat)
    best_candidate: StationPricePoint | None = None
    best_price = float("inf")
    vec_to_end_lon = end_lon - current_lon
    vec_to_end_lat = end_lat - current_lat

    for station in candidates:
        if station.codigo in visited:
            continue
        distance_to_station = _haversine_distance_km(current_lon, current_lat, station.lng, station.lat)
        if attempt == 0 and distance_to_station > max_range_km * 0.85:
            continue
        if attempt <= 1 and distance_to_station > max_range_km * 0.95:
            continue
        if distance_to_station < 2.0:
            continue
        dist_station_to_end = _haversine_distance_km(station.lng, station.lat, end_lon, end_lat)
        progress = dist_current_to_end - dist_station_to_end
        required_progress = max(5.0, 0.25 * max_range_km, dist_current_to_end * 0.1)
        if attempt == 0 and progress < required_progress:
            continue
        if attempt == 1 and progress < required_progress * 0.5:
            continue
        vec_station_lon = station.lng - current_lon
        vec_station_lat = station.lat - current_lat
        dot = vec_station_lon * vec_to_end_lon + vec_station_lat * vec_to_end_lat
        if attempt == 0 and dot <= 0:
            continue
        price = float(station.precio)
        if price < best_price:
            best_candidate = station
            best_price = price

    return best_candidate


def _build_detour_route(
    conn: Connection,
    start_lon: float,
    start_lat: float,
    station: StationPricePoint,
    end_lon: float,
    end_lat: float,
) -> tuple[RouteResult, float]:
    """Compute route passing through a specific station."""
    to_station = compute_route_between_points(
        conn,
        start_lon,
        start_lat,
        station.lng,
        station.lat,
    )
    station_to_end = compute_route_between_points(
        conn,
        station.lng,
        station.lat,
        end_lon,
        end_lat,
    )
    merged_route = _merge_route_results(to_station, station_to_end)
    return merged_route, _route_distance_km(merged_route.coordinates)


def compute_cheapest_route_between_points(
    conn: Connection,
    start_lon: float,
    start_lat: float,
    end_lon: float,
    end_lat: float,
    *,
    vehicle: VehicleSettings,
    candidate_limit: int = 5,
) -> tuple[RouteResult, dict]:
    """Compute a cost-focused route by testing detours through cheap stations."""
    base_route = compute_route_between_points(
        conn,
        start_lon,
        start_lat,
        end_lon,
        end_lat,
    )
    km_per_l = vehicle.consumption_km_per_l or 12.0
    tank_capacity = vehicle.tank_capacity_l or 50.0
    level_percent = vehicle.tank_level_percent
    if level_percent is None:
        level_percent = 50.0
    level_percent = max(0.0, min(100.0, level_percent))
    fuel_liters = tank_capacity * (level_percent / 100.0)

    routes_sequence: list[RouteResult] = []
    visited_codes: set[str] = set()
    station_costs: list[tuple[StationPricePoint, float]] = []

    current_lon = start_lon
    current_lat = start_lat

    fuel_type = (vehicle.fuel_type or "95").upper()

    start_lats = [start_lat, end_lat]
    start_lons = [start_lon, end_lon]
    bbox_padding = max(0.5, abs(max(start_lats) - min(start_lats)), abs(max(start_lons) - min(start_lons))) + 0.5
    global_candidates = get_cheapest_stations_in_bbox(
        conn,
        fuel_type=fuel_type,
        min_lat=min(start_lats) - bbox_padding,
        max_lat=max(start_lats) + bbox_padding,
        min_lng=min(start_lons) - bbox_padding,
        max_lng=max(start_lons) + bbox_padding,
        limit=300,
    )

    total_cost = 0.0
    total_distance_km = 0.0
    reached_end = False

    maximum_iterations = 80
    for _ in range(maximum_iterations):
        try:
            route_to_end = compute_route_between_points(
                conn,
                current_lon,
                current_lat,
                end_lon,
                end_lat,
            )
        except ValueError:
            break

        distance_to_end = _route_distance_km(route_to_end.coordinates)
        range_km = fuel_liters * km_per_l

        if range_km >= distance_to_end:
            routes_sequence.append(route_to_end)
            total_distance_km += distance_to_end
            fuel_liters -= distance_to_end / km_per_l
            reached_end = True
            break

        threshold_liters = tank_capacity * REFUEL_THRESHOLD_RATIO
        excess_liters = max(0.0, fuel_liters - threshold_liters)
        drove_partial = False
        if excess_liters > 0:
            drive_km = min(distance_to_end - 0.25, excess_liters * km_per_l)
            if drive_km > 1.0:
                midpoint = _interpolate_point_along_route(route_to_end.coordinates, drive_km)
                if midpoint:
                    mid_lon, mid_lat = midpoint
                    try:
                        leg = compute_route_between_points(
                            conn,
                            current_lon,
                            current_lat,
                            mid_lon,
                            mid_lat,
                        )
                    except ValueError:
                        leg = None
                    if leg:
                        leg_distance = _route_distance_km(leg.coordinates)
                        routes_sequence.append(leg)
                        total_distance_km += leg_distance
                        fuel_liters = max(0.0, fuel_liters - leg_distance / km_per_l)
                        current_lon, current_lat = mid_lon, mid_lat
                        drove_partial = True
        if drove_partial:
            continue

        candidate_stations = _search_stations_ahead(
            conn,
            current_lon,
            current_lat,
            end_lon,
            end_lat,
            fuel_type=fuel_type,
            range_km=range_km,
            limit=max(candidate_limit, 50),
        )
        merged_candidates: dict[str, StationPricePoint] = {station.codigo: station for station in candidate_stations}
        for station in global_candidates:
            merged_candidates.setdefault(station.codigo, station)
        candidate_stations = list(merged_candidates.values())

        best_station: StationPricePoint | None = None
        best_leg: RouteResult | None = None
        best_leg_distance = 0.0
        best_price = float("inf")

        for station in candidate_stations:
            if station.codigo in visited_codes:
                continue
            to_station_distance = _haversine_distance_km(current_lon, current_lat, station.lng, station.lat)
            if to_station_distance > range_km * 0.95:
                continue
            try:
                leg = compute_route_between_points(
                    conn,
                    current_lon,
                    current_lat,
                    station.lng,
                    station.lat,
                )
            except ValueError:
                visited_codes.add(station.codigo)
                continue

            leg_distance = _route_distance_km(leg.coordinates)
            if leg_distance > range_km * 0.98:
                visited_codes.add(station.codigo)
                continue

            price = float(station.precio)
            if price < best_price:
                best_price = price
                best_station = station
                best_leg = leg
                best_leg_distance = leg_distance

        if not best_station or not best_leg:
            routes_sequence.append(route_to_end)
            total_distance_km += distance_to_end
            fuel_liters -= distance_to_end / km_per_l
            reached_end = True
            break

        routes_sequence.append(best_leg)
        total_distance_km += best_leg_distance
        fuel_liters = max(0.0, fuel_liters - best_leg_distance / km_per_l)
        liters_to_buy = tank_capacity - fuel_liters
        liters_to_buy = max(0.0, liters_to_buy)
        total_cost += liters_to_buy * best_price
        station_costs.append((best_station, liters_to_buy, best_price))
        fuel_liters = tank_capacity
        current_lon = best_station.lng
        current_lat = best_station.lat
        visited_codes.add(best_station.codigo)

    if not reached_end:
        try:
            final_leg = compute_route_between_points(
                conn,
                current_lon,
                current_lat,
                end_lon,
                end_lat,
            )
            routes_sequence.append(final_leg)
            total_distance_km += _route_distance_km(final_leg.coordinates)
        except ValueError:
            if not routes_sequence:
                routes_sequence.append(base_route)
                total_distance_km = _route_distance_km(base_route.coordinates)

    if not routes_sequence:
        routes_sequence.append(base_route)
        total_distance_km = _route_distance_km(base_route.coordinates)

    best_route = _merge_route_sequence(routes_sequence)
    best_distance = _route_distance_km(best_route.coordinates)
    liters_needed = best_distance / km_per_l
    liters_to_buy = max(0.0, liters_needed - tank_capacity)

    cost_summary = {
        "mode": "cheapest",
        "distance_km": round(best_distance, 3),
        "liters_needed": round(liters_needed, 3),
        "liters_to_buy": round(liters_to_buy, 3),
        "price_per_liter": None,
        "estimated_cost_clp": round(total_cost, 2),
        "stations": [],
    }

    for station, liters_here, price in station_costs:
        cost_summary["stations"].append(
            {
                "codigo": station.codigo,
                "marca": station.marca,
                "direccion": station.direccion,
                "precio": float(price),
                "lat": station.lat,
                "lng": station.lng,
                "liters_planned": round(liters_here, 3),
            }
        )

    return best_route, cost_summary
