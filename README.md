# RutasDeBencina

Utilities and services for working with fuel-station routes over a Chilean road
network derived from OpenStreetMap.

## Overpass → PostGIS importer

`Infraestructura/import_overpass_to_db.py` downloads road data from the Overpass
API and hydrates the pgRouting-ready schema defined in `db/schema.sql`.

### Requirements

```bash
pip install -r requirements.txt
```

A running PostgreSQL/PostGIS instance is required. The included Docker Compose
stack can be started via:

```bash
docker compose -f DockerFolder/docker-compose.yml up -d
```

### Usage

```bash
python Infraestructura/import_overpass_to_db.py \
	--dsn postgresql://rutas_user:supersecretpassword@localhost:5432/rutasdb \
	--bbox -33.5 -70.75 -33.4 -70.6
```

Flags of note:

- `--bbox` (lat_min lon_min lat_max lon_max) controls the query window. Omit it
	for the default south-central Chile bounding box.
- `--input` reads an Overpass JSON dump from disk instead of calling the API.
- `--no-truncate` preserves existing records in `osm.*` tables.
- `--skip-topology` avoids regenerating pgRouting topology for debugging.

Successful runs also persist the raw response to
`Infraestructura/infra_raw.json` and a GeoJSON preview at
`Infraestructura/infra.geojson`.

## Shortest-path API

The Flask app at `web/web.py` exposes a shortest-path endpoint backed by
`pgr_dijkstra`. With the server running (`flask --app web.web run` or equivalent)
you can request the cheapest route between two coordinates:

```bash
curl -X POST http://localhost:5000/api/routes/shortest \
	-H "Content-Type: application/json" \
	-d '{
				"start": {"lat": -33.4369052, "lon": -70.6368676},
				"end": {"lat": -33.4414353, "lon": -70.6451415}
			}'
```

The response includes a GeoJSON `LineString`, identifiers for the snapped graph
vertices, and the aggregated travel cost (in seconds, derived from segment
length/speed). Coordinates are snapped to the nearest vertex within 1 km before
the route is computed; requests outside that range return HTTP 400.

