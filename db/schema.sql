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

-- =============================================================================
-- METADATA SCHEMA - Fuel stations, prices, promotions, and vehicle consumption
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS metadata;

COMMENT ON SCHEMA metadata IS 'Fuel stations, prices, promotions, and vehicle consumption data from CNE and other sources.';

SET search_path TO metadata, osm, public;

-- Import tracking for metadata scraping --------------------------------------

CREATE TABLE IF NOT EXISTS scrape_runs (
	id              BIGSERIAL PRIMARY KEY,
	source_type     TEXT        NOT NULL,   -- 'cne', 'promos_aliados', 'promos_estatico', 'consumo'
	source_url      TEXT,
	scraped_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	record_count    INTEGER,
	success         BOOLEAN     NOT NULL DEFAULT TRUE,
	error_message   TEXT,
	metadata        JSONB       DEFAULT '{}'::JSONB
);

CREATE INDEX IF NOT EXISTS scrape_runs_type_idx ON scrape_runs (source_type);
CREATE INDEX IF NOT EXISTS scrape_runs_date_idx ON scrape_runs (scraped_at DESC);

COMMENT ON TABLE scrape_runs IS 'Tracks each metadata scraping job from CNE API and promotion sources.';

-- Gas station brands/chains ---------------------------------------------------

CREATE TABLE IF NOT EXISTS marcas (
	id              SERIAL PRIMARY KEY,
	nombre          TEXT        NOT NULL UNIQUE,  -- 'COPEC', 'Shell', 'Petrobras', etc.
	nombre_display  TEXT,                         -- Display name (e.g., 'Shell MiCopiloto')
	logo_url        TEXT,                         -- Optional: URL to brand logo
	sitio_web       TEXT,                         -- Optional: brand website
	color_hex       TEXT,                         -- Optional: brand color for map markers
	activo          BOOLEAN     NOT NULL DEFAULT TRUE,
	created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS marcas_activo_idx ON marcas (activo) WHERE activo = TRUE;

COMMENT ON TABLE marcas IS 'Normalized gas station brands/chains (Copec, Shell, Petrobras, etc).';
COMMENT ON COLUMN marcas.nombre IS 'Canonical brand name, uppercase for consistency.';

-- Insert common Chilean gas station brands
INSERT INTO marcas (nombre, nombre_display, activo) VALUES
	('COPEC', 'Copec', TRUE),
	('SHELL', 'Shell', TRUE),
	('PETROBRAS', 'Petrobras', TRUE),
	('ARAMCO', 'Aramco', TRUE),
	('ENEX', 'Enex', TRUE),
	('TERPEL', 'Terpel', TRUE)
ON CONFLICT (nombre) DO NOTHING;

-- Gas stations from CNE -------------------------------------------------------

CREATE TABLE IF NOT EXISTS estaciones_cne (
	id              BIGSERIAL PRIMARY KEY,
	codigo          TEXT        NOT NULL UNIQUE,  -- CNE station code (e.g., 'co110101')
	marca           TEXT,                         -- Brand text from CNE (preserved)
	marca_id        INTEGER REFERENCES marcas (id) ON DELETE SET NULL,
	razon_social    TEXT,                         -- Business name
	direccion       TEXT,                         -- Street address
	region          TEXT,
	cod_region      TEXT,
	comuna          TEXT,
	cod_comuna      TEXT,
	geom            geometry(Point, 4326),        -- PostGIS point
	lat             DOUBLE PRECISION,
	lng             DOUBLE PRECISION,
	scrape_run_id   BIGINT REFERENCES scrape_runs (id) ON DELETE SET NULL,
	created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Spatial and lookup indexes
CREATE INDEX IF NOT EXISTS estaciones_cne_geom_idx ON estaciones_cne USING GIST (geom);
CREATE INDEX IF NOT EXISTS estaciones_cne_marca_idx ON estaciones_cne (marca);
CREATE INDEX IF NOT EXISTS estaciones_cne_marca_id_idx ON estaciones_cne (marca_id);
CREATE INDEX IF NOT EXISTS estaciones_cne_region_idx ON estaciones_cne (cod_region);
CREATE INDEX IF NOT EXISTS estaciones_cne_comuna_idx ON estaciones_cne (cod_comuna);

COMMENT ON TABLE estaciones_cne IS 'Gas stations from CNE (Comisión Nacional de Energía) with locations and metadata.';
COMMENT ON COLUMN estaciones_cne.geom IS 'Spatial point for routing and distance queries. Generated from lat/lng.';
COMMENT ON COLUMN estaciones_cne.marca_id IS 'Foreign key to marcas table for normalized brand reference.';

-- Fuel prices history ---------------------------------------------------------

CREATE TABLE IF NOT EXISTS precios_combustible (
	id              BIGSERIAL PRIMARY KEY,
	estacion_id     BIGINT REFERENCES estaciones_cne (id) ON DELETE CASCADE,
	tipo_combustible TEXT       NOT NULL,  -- '93', '95', '97', 'DI' (diesel)
	precio          NUMERIC(10, 2),        -- Price in Chilean pesos per liter (e.g., 1310.00)
	unidad          TEXT,                  -- Unit, typically '$/L'
	fecha           DATE,                  -- Price date
	hora            TIME,                  -- Price time
	tipo_atencion   TEXT,                  -- 'Asistido', 'Autoservicio', etc.
	scrape_run_id   BIGINT REFERENCES scrape_runs (id) ON DELETE SET NULL,
	created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	
	CONSTRAINT tipo_combustible_check CHECK (tipo_combustible IN ('93', '95', '97', 'DI')),
	CONSTRAINT precio_check CHECK (precio IS NULL OR precio >= 0)
);

CREATE INDEX IF NOT EXISTS precios_estacion_idx ON precios_combustible (estacion_id);
CREATE INDEX IF NOT EXISTS precios_tipo_idx ON precios_combustible (tipo_combustible);
CREATE INDEX IF NOT EXISTS precios_fecha_idx ON precios_combustible (fecha DESC);
CREATE INDEX IF NOT EXISTS precios_composite_idx ON precios_combustible (estacion_id, tipo_combustible, fecha DESC);

COMMENT ON TABLE precios_combustible IS 'Historical fuel prices from CNE. Tracks price changes over time.';
COMMENT ON COLUMN precios_combustible.precio IS 'Price in Chilean pesos, stored as decimal to avoid floating-point errors.';

-- Promotions from various sources ---------------------------------------------

CREATE TABLE IF NOT EXISTS promociones (
	id              BIGSERIAL PRIMARY KEY,
	titulo          TEXT        NOT NULL,
	banco           TEXT,                  -- Bank/payment method offering the promo
	descuento       TEXT,                  -- Discount amount(s), e.g., '$50, $100'
	vigencia        TEXT,                  -- Days valid, e.g., 'Lunes, Miércoles'
	fuente_url      TEXT,                  -- Source URL where scraped
	fuente_tipo     TEXT,                  -- 'aliados', 'estatico', etc.
	external_id     TEXT,                  -- Provider-specific identifier (optional)
	fecha_inicio    DATE,                  -- Optional: promo start date
	fecha_fin       DATE,                  -- Optional: promo end date
	activo          BOOLEAN     NOT NULL DEFAULT TRUE,
	scrape_run_id   BIGINT REFERENCES scrape_runs (id) ON DELETE SET NULL,
	created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	CONSTRAINT promociones_fuente_external_unique UNIQUE (fuente_tipo, external_id)
);

CREATE INDEX IF NOT EXISTS promociones_banco_idx ON promociones (banco);
CREATE INDEX IF NOT EXISTS promociones_activo_idx ON promociones (activo) WHERE activo = TRUE;
CREATE INDEX IF NOT EXISTS promociones_vigencia_idx ON promociones USING GIN (to_tsvector('spanish', vigencia));

COMMENT ON TABLE promociones IS 'Fuel promotions from banks, payment methods, and partnerships.';
COMMENT ON COLUMN promociones.descuento IS 'Discount amounts as text (may contain multiple values).';
COMMENT ON COLUMN promociones.vigencia IS 'Days of the week the promotion is valid.';

-- Many-to-many: Promotions can apply to multiple brands ----------------------

CREATE TABLE IF NOT EXISTS promociones_marcas (
	promocion_id    BIGINT REFERENCES promociones (id) ON DELETE CASCADE,
	marca_id        INTEGER REFERENCES marcas (id) ON DELETE CASCADE,
	created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	PRIMARY KEY (promocion_id, marca_id)
);

CREATE INDEX IF NOT EXISTS promociones_marcas_marca_idx ON promociones_marcas (marca_id);
CREATE INDEX IF NOT EXISTS promociones_marcas_promo_idx ON promociones_marcas (promocion_id);

COMMENT ON TABLE promociones_marcas IS 'Junction table linking promotions to applicable gas station brands.';

-- Vehicle consumption data ----------------------------------------------------

CREATE TABLE IF NOT EXISTS consumo_vehicular (
	id              BIGSERIAL PRIMARY KEY,
	patente         TEXT,                  -- License plate (optional, for caching)
	marca           TEXT,                  -- Vehicle brand
	modelo          TEXT,                  -- Vehicle model
	año             INTEGER,               -- Year
	tipo_combustible TEXT,                 -- Fuel type
	rendimiento_ciudad NUMERIC(5, 2),      -- City consumption (km/L)
	rendimiento_carretera NUMERIC(5, 2),   -- Highway consumption (km/L)
	rendimiento_mixto NUMERIC(5, 2),       -- Mixed consumption (km/L)
	fuente          TEXT,                  -- Data source
	scrape_run_id   BIGINT REFERENCES scrape_runs (id) ON DELETE SET NULL,
	created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	
	CONSTRAINT rendimiento_check CHECK (
		rendimiento_mixto IS NULL OR rendimiento_mixto > 0
	)
);

CREATE INDEX IF NOT EXISTS consumo_marca_modelo_idx ON consumo_vehicular (marca, modelo);
CREATE INDEX IF NOT EXISTS consumo_patente_idx ON consumo_vehicular (patente) WHERE patente IS NOT NULL;

COMMENT ON TABLE consumo_vehicular IS 'Vehicle fuel consumption data for cost calculation.';
COMMENT ON COLUMN consumo_vehicular.rendimiento_mixto IS 'Mixed fuel efficiency in kilometers per liter.';

-- =============================================================================
-- VIEWS AND HELPER FUNCTIONS
-- =============================================================================

-- Latest prices view (most recent price per station/fuel type) ---------------

CREATE OR REPLACE VIEW precios_actuales AS
SELECT DISTINCT ON (estacion_id, tipo_combustible)
	p.id,
	p.estacion_id,
	e.codigo,
	e.marca,
	e.comuna,
	e.region,
	p.tipo_combustible,
	p.precio,
	p.fecha,
	p.hora,
	p.tipo_atencion,
	e.geom
FROM precios_combustible p
JOIN estaciones_cne e ON e.id = p.estacion_id
WHERE p.precio IS NOT NULL
ORDER BY estacion_id, tipo_combustible, fecha DESC, hora DESC NULLS LAST;

COMMENT ON VIEW precios_actuales IS 'Latest non-null price for each station and fuel type combination.';

-- View: Promotions with their applicable brands -------------------------------

CREATE OR REPLACE VIEW promociones_con_marcas AS
SELECT 
	p.id AS promocion_id,
	p.titulo,
	p.banco,
	p.descuento,
	p.vigencia,
	p.fuente_url,
	p.external_id,
	p.fecha_inicio,
	p.fecha_fin,
	p.activo,
	m.id AS marca_id,
	m.nombre AS marca_nombre,
	m.nombre_display AS marca_display
FROM promociones p
LEFT JOIN promociones_marcas pm ON p.id = pm.promocion_id
LEFT JOIN marcas m ON pm.marca_id = m.id
WHERE p.activo = TRUE;

COMMENT ON VIEW promociones_con_marcas IS 'Promotions with their associated brands for easy querying.';

-- View: Stations with current prices and applicable promotions ---------------

CREATE OR REPLACE VIEW estaciones_con_promociones AS
SELECT DISTINCT
	e.id AS estacion_id,
	e.codigo,
	e.marca,
	m.nombre_display AS marca_display,
	e.direccion,
	e.comuna,
	e.region,
	e.geom,
	p.id AS promocion_id,
	p.titulo AS promocion_titulo,
	p.descuento,
	p.vigencia
FROM estaciones_cne e
JOIN marcas m ON e.marca_id = m.id
LEFT JOIN promociones_marcas pm ON m.id = pm.marca_id
LEFT JOIN promociones p ON pm.promocion_id = p.id AND p.activo = TRUE;

COMMENT ON VIEW estaciones_con_promociones IS 'Gas stations with all applicable active promotions based on brand.';

-- =============================================================================
-- TRIGGERS
-- =============================================================================

-- Trigger to auto-generate geometry from lat/lng -----------------------------

CREATE OR REPLACE FUNCTION update_estacion_geom()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
	IF NEW.lat IS NOT NULL AND NEW.lng IS NOT NULL THEN
		NEW.geom := ST_SetSRID(ST_MakePoint(NEW.lng, NEW.lat), 4326);
	END IF;
	NEW.updated_at := NOW();
	RETURN NEW;
END;
$$;

CREATE TRIGGER estaciones_geom_trigger
	BEFORE INSERT OR UPDATE ON estaciones_cne
	FOR EACH ROW
	EXECUTE FUNCTION update_estacion_geom();

COMMENT ON FUNCTION update_estacion_geom IS 'Automatically generates PostGIS geometry from lat/lng coordinates.';

-- Function to sync marca field with marcas table -----------------------------

CREATE OR REPLACE FUNCTION sync_estacion_marca()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
	-- Try to match the text marca field to a normalized brand
	IF NEW.marca IS NOT NULL THEN
		SELECT id INTO NEW.marca_id
		FROM marcas
		WHERE UPPER(nombre) = UPPER(TRIM(NEW.marca))
		   OR UPPER(nombre_display) = UPPER(TRIM(NEW.marca))
		LIMIT 1;
		
		-- If no match found, insert new brand
		IF NEW.marca_id IS NULL THEN
			INSERT INTO marcas (nombre, nombre_display)
			VALUES (UPPER(TRIM(NEW.marca)), TRIM(NEW.marca))
			ON CONFLICT (nombre) DO UPDATE SET nombre_display = EXCLUDED.nombre_display
			RETURNING id INTO NEW.marca_id;
		END IF;
	END IF;
	
	RETURN NEW;
END;
$$;

CREATE TRIGGER estaciones_sync_marca_trigger
	BEFORE INSERT OR UPDATE ON estaciones_cne
	FOR EACH ROW
	EXECUTE FUNCTION sync_estacion_marca();

COMMENT ON FUNCTION sync_estacion_marca IS 'Auto-populates marca_id from marca text field, creating new brands as needed.';

-- =============================================================================
-- UTILITY FUNCTIONS
-- =============================================================================

-- Helper function to find nearest gas station --------------------------------

CREATE OR REPLACE FUNCTION nearest_stations(
	lat DOUBLE PRECISION,
	lng DOUBLE PRECISION,
	fuel_type TEXT DEFAULT NULL,
	limit_count INTEGER DEFAULT 5
)
RETURNS TABLE (
	estacion_id BIGINT,
	codigo TEXT,
	marca TEXT,
	direccion TEXT,
	comuna TEXT,
	distance_m DOUBLE PRECISION,
	precio NUMERIC
)
LANGUAGE plpgsql
AS $$
BEGIN
	RETURN QUERY
	SELECT 
		e.id,
		e.codigo,
		e.marca,
		e.direccion,
		e.comuna,
		ST_Distance(e.geom::geography, ST_SetSRID(ST_MakePoint(lng, lat), 4326)::geography) AS distance_m,
		p.precio
	FROM estaciones_cne e
	LEFT JOIN LATERAL (
		SELECT precio
		FROM precios_combustible pc
		WHERE pc.estacion_id = e.id
		  AND (fuel_type IS NULL OR pc.tipo_combustible = fuel_type)
		ORDER BY pc.fecha DESC, pc.hora DESC NULLS LAST
		LIMIT 1
	) p ON TRUE
	WHERE e.geom IS NOT NULL
	ORDER BY e.geom <-> ST_SetSRID(ST_MakePoint(lng, lat), 4326)
	LIMIT limit_count;
END;
$$;

COMMENT ON FUNCTION nearest_stations IS 'Find nearest gas stations to a given coordinate, optionally filtered by fuel type.';

-- Function: Get promotions for a specific station ----------------------------

CREATE OR REPLACE FUNCTION get_promociones_estacion(station_codigo TEXT)
RETURNS TABLE (
	promocion_id BIGINT,
	titulo TEXT,
	banco TEXT,
	descuento TEXT,
	vigencia TEXT,
	fuente_url TEXT
)
LANGUAGE plpgsql
AS $$
BEGIN
	RETURN QUERY
	SELECT 
		p.id,
		p.titulo,
		p.banco,
		p.descuento,
		p.vigencia,
		p.fuente_url
	FROM estaciones_cne e
	JOIN marcas m ON e.marca_id = m.id
	JOIN promociones_marcas pm ON m.id = pm.marca_id
	JOIN promociones p ON pm.promocion_id = p.id
	WHERE e.codigo = station_codigo
	  AND p.activo = TRUE
	  AND m.activo = TRUE;
END;
$$;

COMMENT ON FUNCTION get_promociones_estacion IS 'Get all active promotions applicable to a specific gas station by its CNE code.';

-- Function: Get best price with promotions for a route -----------------------

CREATE OR REPLACE FUNCTION best_prices_on_route(
	route_geom geometry,
	buffer_meters DOUBLE PRECISION DEFAULT 1000,
	fuel_type TEXT DEFAULT 'DI',
	day_of_week TEXT DEFAULT NULL  -- 'Lunes', 'Martes', etc.
)
RETURNS TABLE (
	estacion_id BIGINT,
	codigo TEXT,
	marca TEXT,
	direccion TEXT,
	precio NUMERIC,
	tiene_promo BOOLEAN,
	promo_descuento TEXT,
	distance_to_route_m DOUBLE PRECISION
)
LANGUAGE plpgsql
AS $$
BEGIN
	RETURN QUERY
	SELECT 
		e.id,
		e.codigo,
		e.marca,
		e.direccion,
		pa.precio,
		(COUNT(p.id) > 0) AS tiene_promo,
		STRING_AGG(p.descuento, '; ') AS promo_descuento,
		ST_Distance(e.geom::geography, route_geom::geography) AS distance_m
	FROM estaciones_cne e
	CROSS JOIN LATERAL (
		SELECT precio
		FROM precios_combustible pc
		WHERE pc.estacion_id = e.id
		  AND pc.tipo_combustible = fuel_type
		ORDER BY pc.fecha DESC, pc.hora DESC NULLS LAST
		LIMIT 1
	) pa
	LEFT JOIN marcas m ON e.marca_id = m.id
	LEFT JOIN promociones_marcas pm ON m.id = pm.marca_id
	LEFT JOIN promociones p ON pm.promocion_id = p.id 
		AND p.activo = TRUE
		AND (day_of_week IS NULL OR p.vigencia ILIKE '%' || day_of_week || '%')
	WHERE e.geom IS NOT NULL
	  AND ST_DWithin(e.geom::geography, route_geom::geography, buffer_meters)
	  AND pa.precio IS NOT NULL
	GROUP BY e.id, e.codigo, e.marca, e.direccion, pa.precio, e.geom
	ORDER BY pa.precio ASC, distance_m ASC;
END;
$$;

COMMENT ON FUNCTION best_prices_on_route IS 'Find cheapest stations near a route, with promotion info.';

-- Helper function to extract brand from promotion text -----------------------

CREATE OR REPLACE FUNCTION extract_marca_from_promo(promo_text TEXT)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
	marca_record RECORD;
BEGIN
	-- Try to find brand mentioned in the promotion text
	FOR marca_record IN 
		SELECT id, nombre, nombre_display 
		FROM marcas 
		WHERE activo = TRUE
		ORDER BY LENGTH(nombre) DESC  -- Match longer names first
	LOOP
		IF promo_text ILIKE '%' || marca_record.nombre || '%' 
		   OR promo_text ILIKE '%' || marca_record.nombre_display || '%' THEN
			RETURN marca_record.id;
		END IF;
	END LOOP;
	
	RETURN NULL;
END;
$$;

COMMENT ON FUNCTION extract_marca_from_promo IS 'Attempts to extract brand from promotion title/description.';

-- =============================================================================
-- SEARCH PATH CONFIGURATION
-- =============================================================================

-- Ensure default search_path includes both schemas ---------------------------

DO $$
BEGIN
	EXECUTE format('ALTER DATABASE %I SET search_path TO metadata, osm, public', current_database());
END;
$$;

DO $$
BEGIN
	IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'rutas_user') THEN
		EXECUTE 'ALTER ROLE rutas_user SET search_path TO metadata, osm, public';
	END IF;
END;
$$;

-- Restore default search_path ------------------------------------------------
RESET search_path;
