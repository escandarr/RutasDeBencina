"""Database access package for RutasDeBencina."""

# Core database and routing
from .connection import Database
from .pgrouting import RouteSegment, shortest_path
from .repositories import iter_edges, iter_nodes
from .services import RouteResult, compute_route_between_points

# Metadata models
from .metadata_models import (
	Estacion,
	Marca,
	Precio,
	PrecioActual,
	Promocion,
	PromocionConMarca,
	EstacionConPromociones,
	ConsumoVehicular,
	ScrapeRun,
)

# Metadata repositories
from .metadata_repositories import (
	# Marcas
	get_all_marcas,
	get_marca_by_id,
	get_marca_by_nombre,
	create_marca,
	# Estaciones
	get_all_estaciones,
	get_estacion_by_codigo,
	get_estaciones_by_marca,
	get_estaciones_by_region,
	find_nearest_estaciones,
	upsert_estacion,
	# Precios
	get_precios_actuales,
	get_precio_estacion,
	insert_precio,
	# Promociones
	get_all_promociones,
	get_promociones_con_marcas,
	get_promociones_by_marca,
	get_promociones_by_day,
	insert_promocion,
	link_promocion_to_marca,
	auto_link_promocion_to_marcas,
	delete_promociones_by_fuente,
	# Estaciones con promociones
	get_estaciones_con_promociones,
	get_promociones_estacion,
	# Scrape runs
	create_scrape_run,
	get_latest_scrape_run,
	# Bulk import
	bulk_import_estaciones_from_cne,
	bulk_import_precios_from_cne,
	bulk_import_promociones,
)

# Metadata services
from .metadata_services import (
	StationOnRoute,
	RouteWithFuelCost,
	FuelSavingsReport,
	find_stations_on_route,
	calculate_route_fuel_cost,
	find_cheapest_stations_in_region,
	find_stations_near_point,
	compare_fuel_costs_across_brands,
	get_promotions_for_day,
	calculate_savings_with_promotion,
)

__all__ = [
	# Core
	"Database",
	"RouteSegment",
	"shortest_path",
	"RouteResult",
	"compute_route_between_points",
	"iter_edges",
	"iter_nodes",
	# Models
	"Estacion",
	"Marca",
	"Precio",
	"PrecioActual",
	"Promocion",
	"PromocionConMarca",
	"EstacionConPromociones",
	"ConsumoVehicular",
	"ScrapeRun",
	# Repository functions
	"get_all_marcas",
	"get_marca_by_id",
	"get_marca_by_nombre",
	"create_marca",
	"get_all_estaciones",
	"get_estacion_by_codigo",
	"get_estaciones_by_marca",
	"get_estaciones_by_region",
	"find_nearest_estaciones",
	"upsert_estacion",
	"get_precios_actuales",
	"get_precio_estacion",
	"insert_precio",
	"get_all_promociones",
	"get_promociones_con_marcas",
	"get_promociones_by_marca",
	"get_promociones_by_day",
	"insert_promocion",
	"link_promocion_to_marca",
	"auto_link_promocion_to_marcas",
	"delete_promociones_by_fuente",
	"get_estaciones_con_promociones",
	"get_promociones_estacion",
	"create_scrape_run",
	"get_latest_scrape_run",
	"bulk_import_estaciones_from_cne",
	"bulk_import_precios_from_cne",
	"bulk_import_promociones",
	# Service functions
	"StationOnRoute",
	"RouteWithFuelCost",
	"FuelSavingsReport",
	"find_stations_on_route",
	"calculate_route_fuel_cost",
	"find_cheapest_stations_in_region",
	"find_stations_near_point",
	"compare_fuel_costs_across_brands",
	"get_promotions_for_day",
	"calculate_savings_with_promotion",
]
