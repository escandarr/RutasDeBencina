--
-- RutasDeBencina base database schema for Overpass (OSM) imports and pgRouting
--

-- Enable required extensions -------------------------------------------------
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS pgrouting;

-- Dedicated schema to hold OSM-derived data ----------------------------------
CREATE SCHEMA IF NOT EXISTS osm;

COMMENT ON SCHEMA osm IS 'OpenStreetMap-derived data imported from Overpass API.';

SET search_path TO osm, public;

-- Imports audit table ---------------------------------------------------------
CREATE TABLE IF NOT EXISTS import_runs (
	id              BIGSERIAL PRIMARY KEY,
	source          TEXT        NOT NULL,   -- e.g. overpass url or bbox descriptor
	query           TEXT,
	bbox            geometry(Polygon, 4326),
	imported_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	element_total   INTEGER,
	notes           TEXT
);

COMMENT ON TABLE import_runs IS 'Tracks each Overpass API import job.';

-- Core OSM primitives ---------------------------------------------------------

CREATE TABLE IF NOT EXISTS nodes (
	id              BIGINT PRIMARY KEY,     -- OSM node id
	version         INTEGER,
	tags            JSONB      NOT NULL DEFAULT '{}'::JSONB,
	geom            geometry(Point, 4326) NOT NULL,
	created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS nodes_geom_idx ON nodes USING GIST (geom);
CREATE INDEX IF NOT EXISTS nodes_tags_idx ON nodes USING GIN (tags jsonb_path_ops);

CREATE TABLE IF NOT EXISTS ways (
	id              BIGINT PRIMARY KEY,     -- OSM way id
	version         INTEGER,
	tags            JSONB      NOT NULL DEFAULT '{}'::JSONB,
	geom            geometry(LineString, 4326) NOT NULL,
	oneway          BOOLEAN,
	maxspeed_kph    INTEGER,
	road_class      TEXT,
	length_m        DOUBLE PRECISION GENERATED ALWAYS AS (ST_Length(geom::geography)) STORED,
	created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ways_geom_idx ON ways USING GIST (geom);
CREATE INDEX IF NOT EXISTS ways_tags_idx ON ways USING GIN (tags jsonb_path_ops);

CREATE TABLE IF NOT EXISTS way_nodes (
	way_id          BIGINT REFERENCES ways (id) ON DELETE CASCADE,
	node_id         BIGINT REFERENCES nodes (id) ON DELETE CASCADE,
	seq             INTEGER    NOT NULL,
	geom            geometry(Point, 4326),
	PRIMARY KEY (way_id, seq)
);

CREATE INDEX IF NOT EXISTS way_nodes_node_idx ON way_nodes (node_id);

-- pgRouting edge store --------------------------------------------------------

CREATE TABLE IF NOT EXISTS road_edges (
	id              BIGSERIAL PRIMARY KEY,
	osm_way_id      BIGINT REFERENCES ways (id) ON DELETE CASCADE,
	import_run_id   BIGINT REFERENCES import_runs (id) ON DELETE SET NULL,
	source          BIGINT,
	target          BIGINT,
	directed        BOOLEAN    NOT NULL DEFAULT FALSE,
	speed_kph       INTEGER,
	length_m        DOUBLE PRECISION GENERATED ALWAYS AS (ST_Length(geom::geography)) STORED,
	cost            DOUBLE PRECISION,
	reverse_cost    DOUBLE PRECISION,
	geom            geometry(LineString, 4326) NOT NULL,
	tags            JSONB      NOT NULL DEFAULT '{}'::JSONB,
	CHECK (cost IS NULL OR cost >= 0),
	CHECK (reverse_cost IS NULL OR reverse_cost >= 0)
);

COMMENT ON TABLE road_edges IS 'Network edges prepared for pgRouting. One record per navigable segment.';

CREATE INDEX IF NOT EXISTS road_edges_geom_idx ON road_edges USING GIST (geom);
CREATE INDEX IF NOT EXISTS road_edges_source_idx ON road_edges (source);
CREATE INDEX IF NOT EXISTS road_edges_target_idx ON road_edges (target);

-- Helpful view for pgr_dijkstra calls ----------------------------------------

CREATE OR REPLACE VIEW road_edges_pgr AS
SELECT
	id,
	source,
	target,
	COALESCE(cost, length_m)        AS cost,
	COALESCE(
		reverse_cost,
		CASE WHEN directed IS TRUE THEN 1e9 ELSE length_m END
	)                                AS reverse_cost
FROM road_edges;

COMMENT ON VIEW road_edges_pgr IS 'Minimal column set required by pgRouting shortest path functions.';

-- Utility function to refresh topology after an import -----------------------

CREATE OR REPLACE FUNCTION refresh_road_topology(
	tolerance DOUBLE PRECISION DEFAULT 0.00001
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
	PERFORM pgr_createTopology('osm.road_edges', tolerance, 'geom', 'id');
	PERFORM pgr_analyzeGraph('osm.road_edges', tolerance);
END;
$$;

COMMENT ON FUNCTION refresh_road_topology IS 'Generates pgRouting source/target ids for road_edges and validates the graph.';

-- Ensure default search_path includes osm schema -----------------------------

DO $$
BEGIN
	EXECUTE format('ALTER DATABASE %I SET search_path TO osm, public', current_database());
END;
$$;

DO $$
BEGIN
	IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'rutas_user') THEN
		EXECUTE 'ALTER ROLE rutas_user SET search_path TO osm, public';
	END IF;
END;
$$;

-- Restore default search_path ------------------------------------------------
RESET search_path;
