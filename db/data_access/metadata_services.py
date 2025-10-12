"""Higher-level services combining routing with fuel metadata."""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import List, Optional, Tuple

from psycopg import Connection
from psycopg.rows import dict_row

from .metadata_models import Estacion, PrecioActual, Promocion
from .metadata_repositories import (
    find_nearest_estaciones,
    get_estacion_by_codigo,
    get_precios_actuales,
    get_promociones_estacion,
)


@dataclass
class StationOnRoute:
    """Gas station near a route with price and distance info."""
    estacion_id: int
    codigo: str
    marca: str
    direccion: str
    comuna: str
    precio: Optional[Decimal]
    tiene_promo: bool
    promo_descuento: Optional[str]
    distance_to_route_m: float
    lat: Optional[float] = None
    lng: Optional[float] = None


@dataclass
class RouteWithFuelCost:
    """Route with fuel cost calculation."""
    coordinates: List[Tuple[float, float]]
    distance_km: float
    fuel_type: str
    fuel_consumption_km_per_l: float
    liters_needed: float
    cheapest_station: Optional[StationOnRoute]
    estimated_fuel_cost: Optional[Decimal]
    nearby_stations: List[StationOnRoute]


def find_stations_on_route(
    conn: Connection,
    route_coords: List[Tuple[float, float]],
    *,
    fuel_type: str = 'DI',
    buffer_meters: float = 1000.0,
    day_of_week: Optional[str] = None,
    limit: int = 20
) -> List[StationOnRoute]:
    """
    Find gas stations near a route with current prices and promotions.
    
    Args:
        conn: Database connection
        route_coords: List of (lon, lat) tuples defining the route
        fuel_type: '93', '95', '97', or 'DI'
        buffer_meters: Search radius around route
        day_of_week: Filter promotions by day (e.g., 'Miércoles')
        limit: Maximum stations to return
    
    Returns:
        List of stations sorted by price (cheapest first)
    """
    if not route_coords or len(route_coords) < 2:
        return []
    
    # Build WKT LINESTRING for the route
    coords_wkt = ','.join([f'{lon} {lat}' for lon, lat in route_coords])
    linestring_wkt = f'LINESTRING({coords_wkt})'
    
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT * FROM (
                SELECT 
                    e.id AS estacion_id,
                    e.codigo,
                    e.marca,
                    e.direccion,
                    e.comuna,
                    e.lat,
                    e.lng,
                    data.precio,
                    data.tiene_promo,
                    data.promo_descuento,
                    data.distance_m
                FROM metadata.estaciones_cne e
                JOIN metadata.best_prices_on_route(
                    ST_GeomFromText(%s, 4326),
                    %s,
                    %s,
                    %s
                ) AS data ON data.estacion_id = e.id
            ) AS sub
            ORDER BY precio NULLS LAST, distance_m
            LIMIT %s
            """,
            (linestring_wkt, buffer_meters, fuel_type, day_of_week, limit)
        )

        stations = []
        for row in cur.fetchall():
            stations.append(
                StationOnRoute(
                    estacion_id=row['estacion_id'],
                    codigo=row['codigo'],
                    marca=row['marca'],
                    direccion=row['direccion'],
                    comuna=row['comuna'] or '',
                    precio=row['precio'],
                    tiene_promo=row['tiene_promo'],
                    promo_descuento=row['promo_descuento'],
                    distance_to_route_m=row['distance_m'],
                    lat=row['lat'],
                    lng=row['lng'],
                )
            )

        return stations


def calculate_route_fuel_cost(
    conn: Connection,
    route_coords: List[Tuple[float, float]],
    distance_km: float,
    *,
    fuel_type: str = 'DI',
    fuel_consumption_km_per_l: float = 12.0,
    buffer_meters: float = 1000.0,
    day_of_week: Optional[str] = None
) -> RouteWithFuelCost:
    """
    Calculate fuel cost for a route based on nearby stations.
    
    Args:
        conn: Database connection
        route_coords: List of (lon, lat) tuples
        distance_km: Total route distance in kilometers
        fuel_type: Type of fuel ('93', '95', '97', 'DI')
        fuel_consumption_km_per_l: Vehicle efficiency (km per liter)
        buffer_meters: Search radius around route
        day_of_week: Day for promotion filtering
    
    Returns:
        RouteWithFuelCost with cost estimation and station options
    """
    liters_needed = distance_km / fuel_consumption_km_per_l
    
    nearby_stations = find_stations_on_route(
        conn,
        route_coords,
        fuel_type=fuel_type,
        buffer_meters=buffer_meters,
        day_of_week=day_of_week,
        limit=10
    )
    
    cheapest_station = nearby_stations[0] if nearby_stations else None
    estimated_cost = None
    
    if cheapest_station and cheapest_station.precio:
        estimated_cost = cheapest_station.precio * Decimal(str(liters_needed))
    
    return RouteWithFuelCost(
        coordinates=route_coords,
        distance_km=distance_km,
        fuel_type=fuel_type,
        fuel_consumption_km_per_l=fuel_consumption_km_per_l,
        liters_needed=liters_needed,
        cheapest_station=cheapest_station,
        estimated_fuel_cost=estimated_cost,
        nearby_stations=nearby_stations
    )


def find_cheapest_stations_in_region(
    conn: Connection,
    cod_region: str,
    fuel_type: str = 'DI',
    limit: int = 10
) -> List[PrecioActual]:
    """Find the cheapest stations for a fuel type in a region."""
    precios = get_precios_actuales(
        conn,
        tipo_combustible=fuel_type,
        cod_region=cod_region
    )
    
    # Sort by price and filter out nulls
    valid_precios = [p for p in precios if p.precio is not None]
    valid_precios.sort(key=lambda p: p.precio)
    
    return valid_precios[:limit]


def find_stations_near_point(
    conn: Connection,
    lat: float,
    lng: float,
    *,
    fuel_type: Optional[str] = None,
    max_distance_km: float = 5.0,
    with_promotions: bool = False,
    limit: int = 10
) -> List[Estacion]:
    """
    Find gas stations near a specific point.
    
    Args:
        conn: Database connection
        lat: Latitude
        lng: Longitude
        fuel_type: Optional fuel type filter
        max_distance_km: Maximum search radius in kilometers
        with_promotions: If True, only return stations with active promotions
        limit: Maximum number of stations to return
    
    Returns:
        List of nearest stations
    """
    stations = find_nearest_estaciones(
        conn,
        lat=lat,
        lng=lng,
        fuel_type=fuel_type,
        limit=limit * 2 if with_promotions else limit  # Get more if filtering
    )
    
    # Filter by distance
    max_distance_m = max_distance_km * 1000
    stations = [s for s in stations if s.distance_m is None or s.distance_m <= max_distance_m]
    
    # Filter by promotions if requested
    if with_promotions:
        stations_with_promos = []
        for station in stations:
            promos = get_promociones_estacion(conn, station.codigo)
            if promos:
                stations_with_promos.append(station)
        stations = stations_with_promos
    
    return stations[:limit]


def compare_fuel_costs_across_brands(
    conn: Connection,
    fuel_type: str = 'DI',
    cod_region: Optional[str] = None
) -> dict[str, dict]:
    """
    Compare average fuel prices across different brands.
    
    Returns a dict mapping brand names to their stats:
    {
        'Copec': {
            'avg_price': Decimal('1250.50'),
            'min_price': Decimal('1200.00'),
            'max_price': Decimal('1300.00'),
            'station_count': 45
        },
        ...
    }
    """
    sql = """
        SELECT 
            m.nombre_display as marca,
            AVG(p.precio) as avg_price,
            MIN(p.precio) as min_price,
            MAX(p.precio) as max_price,
            COUNT(DISTINCT p.estacion_id) as station_count
        FROM metadata.precios_actuales p
        JOIN metadata.estaciones_cne e ON e.id = p.estacion_id
        JOIN metadata.marcas m ON e.marca_id = m.id
        WHERE p.tipo_combustible = %s
          AND p.precio IS NOT NULL
    """
    params = [fuel_type]
    
    if cod_region:
        sql += " AND e.cod_region = %s"
        params.append(cod_region)
    
    sql += " GROUP BY m.nombre_display ORDER BY avg_price"
    
    result = {}
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, params)
        for row in cur.fetchall():
            result[row['marca']] = {
                'avg_price': row['avg_price'],
                'min_price': row['min_price'],
                'max_price': row['max_price'],
                'station_count': row['station_count']
            }
    
    return result


def get_promotions_for_day(
    conn: Connection,
    day_of_week: str,
    marca: Optional[str] = None
) -> List[dict]:
    """
    Get all active promotions for a specific day of the week.
    
    Args:
        conn: Database connection
        day_of_week: Spanish day name (e.g., 'Lunes', 'Miércoles')
        marca: Optional brand filter
    
    Returns:
        List of promotion dicts with station counts
    """
    sql = """
        SELECT 
            p.titulo,
            p.banco,
            p.descuento,
            p.vigencia,
            STRING_AGG(DISTINCT m.nombre_display, ', ') as marcas,
            COUNT(DISTINCT e.id) as station_count
        FROM metadata.promociones p
        JOIN metadata.promociones_marcas pm ON p.id = pm.promocion_id
        JOIN metadata.marcas m ON pm.marca_id = m.id
        LEFT JOIN metadata.estaciones_cne e ON e.marca_id = m.id
        WHERE p.activo = TRUE
          AND p.vigencia ILIKE %s
    """
    params = [f'%{day_of_week}%']
    
    if marca:
        sql += " AND m.nombre_display ILIKE %s"
        params.append(f'%{marca}%')
    
    sql += """
        GROUP BY p.id, p.titulo, p.banco, p.descuento, p.vigencia
        ORDER BY p.titulo
    """
    
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, params)
        return [dict(row) for row in cur.fetchall()]


@dataclass
class FuelSavingsReport:
    """Report comparing fuel costs with and without promotions."""
    route_distance_km: float
    liters_needed: float
    base_price_per_liter: Decimal
    base_total_cost: Decimal
    promo_discount_per_liter: Decimal
    promo_total_cost: Decimal
    total_savings: Decimal
    promo_details: str
    station_codigo: str
    station_marca: str


def calculate_savings_with_promotion(
    conn: Connection,
    route_distance_km: float,
    fuel_consumption_km_per_l: float,
    station_codigo: str,
    fuel_type: str = 'DI',
    discount_per_liter: Optional[Decimal] = None
) -> Optional[FuelSavingsReport]:
    """
    Calculate how much money a promotion saves on a route.
    
    Args:
        conn: Database connection
        route_distance_km: Route distance in km
        fuel_consumption_km_per_l: Vehicle efficiency
        station_codigo: CNE station code
        fuel_type: Fuel type
        discount_per_liter: Discount amount (if None, tries to extract from promo text)
    
    Returns:
        FuelSavingsReport or None if station/price not found
    """
    station = get_estacion_by_codigo(conn, station_codigo)
    if not station:
        return None
    
    # Get current price
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT precio FROM metadata.precios_actuales
            WHERE codigo = %s AND tipo_combustible = %s
            """,
            (station_codigo, fuel_type)
        )
        price_row = cur.fetchone()
        if not price_row or not price_row['precio']:
            return None
        
        base_price = price_row['precio']
    
    # Get promotions
    promos = get_promociones_estacion(conn, station_codigo)
    if not promos:
        return None
    
    liters_needed = Decimal(str(route_distance_km / fuel_consumption_km_per_l))
    base_total = base_price * liters_needed
    
    # Use provided discount or try to extract from promo
    if discount_per_liter is None:
        # Try to extract discount from promo text (simple approach)
        discount_per_liter = Decimal('0')
        promo_text = promos[0].descuento or ''
        # This is a simplified extraction - in production you'd want better parsing
        import re
        matches = re.findall(r'\$\s*(\d+)', promo_text)
        if matches:
            discount_per_liter = Decimal(matches[0])
    
    promo_price = base_price - discount_per_liter
    if promo_price < 0:
        promo_price = Decimal('0')
    
    promo_total = promo_price * liters_needed
    savings = base_total - promo_total
    
    return FuelSavingsReport(
        route_distance_km=route_distance_km,
        liters_needed=liters_needed,
        base_price_per_liter=base_price,
        base_total_cost=base_total,
        promo_discount_per_liter=discount_per_liter,
        promo_total_cost=promo_total,
        total_savings=savings,
        promo_details=f"{promos[0].titulo} - {promos[0].descuento}",
        station_codigo=station_codigo,
        station_marca=station.marca or ''
    )

