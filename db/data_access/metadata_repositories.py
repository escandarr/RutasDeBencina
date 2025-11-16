"""Repository functions for reading/writing metadata schema tables."""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Dict, Iterable, List, Optional, Sequence

from psycopg import Connection
from psycopg.rows import dict_row

from .metadata_models import (
    Estacion,
    EstacionConPromociones,
    Marca,
    Precio,
    PrecioActual,
    Promocion,
    PromocionConMarca,
    ScrapeRun,
)


# =============================================================================
# MARCAS (Brands)
# =============================================================================

def get_all_marcas(conn: Connection, *, activo_only: bool = True) -> List[Marca]:
    """Get all gas station brands."""
    sql = "SELECT * FROM metadata.marcas"
    if activo_only:
        sql += " WHERE activo = TRUE"
    sql += " ORDER BY nombre"
    
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql)
        return [Marca(**row) for row in cur.fetchall()]


def get_marca_by_id(conn: Connection, marca_id: int) -> Optional[Marca]:
    """Get a specific brand by ID."""
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("SELECT * FROM metadata.marcas WHERE id = %s", (marca_id,))
        row = cur.fetchone()
        return Marca(**row) if row else None


def get_marca_by_nombre(conn: Connection, nombre: str) -> Optional[Marca]:
    """Get a brand by name (case-insensitive)."""
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            "SELECT * FROM metadata.marcas WHERE UPPER(nombre) = UPPER(%s) OR UPPER(nombre_display) = UPPER(%s)",
            (nombre, nombre)
        )
        row = cur.fetchone()
        return Marca(**row) if row else None


def create_marca(
    conn: Connection,
    nombre: str,
    nombre_display: Optional[str] = None,
    **kwargs
) -> int:
    """Create a new brand and return its ID."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO metadata.marcas (nombre, nombre_display, logo_url, sitio_web, color_hex)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (nombre) DO NOTHING
            RETURNING id
            """,
            (
                nombre,
                nombre_display or nombre,
                kwargs.get('logo_url'),
                kwargs.get('sitio_web'),
                kwargs.get('color_hex')
            )
        )
        result = cur.fetchone()
        conn.commit()
        return result[0] if result else get_marca_by_nombre(conn, nombre).id


# =============================================================================
# ESTACIONES (Gas Stations)
# =============================================================================

def get_all_estaciones(conn: Connection, *, limit: Optional[int] = None) -> List[Estacion]:
    """Get all CNE gas stations."""
    sql = "SELECT * FROM metadata.estaciones_cne ORDER BY codigo"
    params = None
    
    if limit:
        sql += " LIMIT %s"
        params = (limit,)
    
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, params)
        return [Estacion(**row) for row in cur.fetchall()]


def get_estacion_by_codigo(conn: Connection, codigo: str) -> Optional[Estacion]:
    """Get a station by its CNE code."""
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("SELECT * FROM metadata.estaciones_cne WHERE codigo = %s", (codigo,))
        row = cur.fetchone()
        return Estacion(**row) if row else None


def get_estaciones_by_marca(conn: Connection, marca_id: int) -> List[Estacion]:
    """Get all stations for a specific brand."""
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            "SELECT * FROM metadata.estaciones_cne WHERE marca_id = %s ORDER BY comuna, direccion",
            (marca_id,)
        )
        return [Estacion(**row) for row in cur.fetchall()]


def get_estaciones_by_region(conn: Connection, cod_region: str) -> List[Estacion]:
    """Get all stations in a specific region."""
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            "SELECT * FROM metadata.estaciones_cne WHERE cod_region = %s ORDER BY comuna, direccion",
            (cod_region,)
        )
        return [Estacion(**row) for row in cur.fetchall()]


def find_nearest_estaciones(
    conn: Connection,
    lat: float,
    lng: float,
    *,
    fuel_type: Optional[str] = None,
    limit: int = 5
) -> List[Estacion]:
    """Find nearest gas stations using the database function."""
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            "SELECT * FROM metadata.nearest_stations(%s, %s, %s, %s)",
            (lat, lng, fuel_type, limit)
        )
        return [Estacion(**row) for row in cur.fetchall()]


def upsert_estacion(
    conn: Connection,
    codigo: str,
    marca: Optional[str] = None,
    razon_social: Optional[str] = None,
    direccion: Optional[str] = None,
    region: Optional[str] = None,
    cod_region: Optional[str] = None,
    comuna: Optional[str] = None,
    cod_comuna: Optional[str] = None,
    lat: Optional[float] = None,
    lng: Optional[float] = None,
    scrape_run_id: Optional[int] = None,
) -> int:
    """Insert or update a gas station. Returns the station ID."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO metadata.estaciones_cne 
                (codigo, marca, razon_social, direccion, region, cod_region, 
                 comuna, cod_comuna, lat, lng, scrape_run_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (codigo) DO UPDATE SET
                marca = EXCLUDED.marca,
                razon_social = EXCLUDED.razon_social,
                direccion = EXCLUDED.direccion,
                region = EXCLUDED.region,
                cod_region = EXCLUDED.cod_region,
                comuna = EXCLUDED.comuna,
                cod_comuna = EXCLUDED.cod_comuna,
                lat = EXCLUDED.lat,
                lng = EXCLUDED.lng,
                scrape_run_id = EXCLUDED.scrape_run_id,
                updated_at = NOW()
            RETURNING id
            """,
            (codigo, marca, razon_social, direccion, region, cod_region,
             comuna, cod_comuna, lat, lng, scrape_run_id)
        )
        result = cur.fetchone()
        conn.commit()
        return result[0]


# =============================================================================
# PRECIOS (Fuel Prices)
# =============================================================================

def get_precios_actuales(
    conn: Connection,
    *,
    tipo_combustible: Optional[str] = None,
    marca_id: Optional[int] = None,
    cod_region: Optional[str] = None
) -> List[PrecioActual]:
    """Get current prices, optionally filtered."""
    sql = """
        SELECT 
            p.estacion_id,
            p.codigo,
            p.marca,
            p.comuna,
            p.region,
            p.tipo_combustible,
            p.precio,
            p.fecha,
            p.hora,
            e.lat,
            e.lng
        FROM metadata.precios_actuales p
        JOIN metadata.estaciones_cne e ON e.id = p.estacion_id
        WHERE 1=1
    """
    params = []
    
    if tipo_combustible:
        sql += " AND p.tipo_combustible = %s"
        params.append(tipo_combustible)
    
    if marca_id:
        sql += " AND e.marca_id = %s"
        params.append(marca_id)
    
    if cod_region:
        sql += " AND p.region = (SELECT region FROM metadata.estaciones_cne WHERE cod_region = %s LIMIT 1)"
        params.append(cod_region)
    
    sql += " ORDER BY p.precio ASC NULLS LAST"
    
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, params or None)
        return [PrecioActual(**row) for row in cur.fetchall()]


def get_precio_estacion(
    conn: Connection,
    estacion_id: int,
    tipo_combustible: str
) -> Optional[Precio]:
    """Get the latest price for a station and fuel type."""
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT * FROM metadata.precios_combustible
            WHERE estacion_id = %s AND tipo_combustible = %s
            ORDER BY fecha DESC, hora DESC NULLS LAST
            LIMIT 1
            """,
            (estacion_id, tipo_combustible)
        )
        row = cur.fetchone()
        return Precio(**row) if row else None


def insert_precio(
    conn: Connection,
    estacion_id: int,
    tipo_combustible: str,
    precio: Optional[Decimal],
    unidad: Optional[str] = None,
    fecha: Optional[date] = None,
    hora: Optional[time] = None,
    tipo_atencion: Optional[str] = None,
    scrape_run_id: Optional[int] = None,
) -> int:
    """Insert a new fuel price record."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO metadata.precios_combustible
                (estacion_id, tipo_combustible, precio, unidad, fecha, hora, tipo_atencion, scrape_run_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (estacion_id, tipo_combustible, precio, unidad, fecha, hora, tipo_atencion, scrape_run_id)
        )
        result = cur.fetchone()
        conn.commit()
        return result[0]


# =============================================================================
# PROMOCIONES (Promotions)
# =============================================================================

def get_all_promociones(conn: Connection, *, activo_only: bool = True) -> List[Promocion]:
    """Get all promotions."""
    sql = "SELECT * FROM metadata.promociones"
    if activo_only:
        sql += " WHERE activo = TRUE"
    sql += " ORDER BY created_at DESC"
    
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql)
        return [Promocion(**row) for row in cur.fetchall()]


def get_promociones_con_marcas(conn: Connection) -> List[PromocionConMarca]:
    """Get promotions with their associated brands."""
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("SELECT * FROM metadata.promociones_con_marcas ORDER BY promocion_id")
        return [PromocionConMarca(**row) for row in cur.fetchall()]


def get_promociones_by_marca(conn: Connection, marca_id: int) -> List[Promocion]:
    """Get all active promotions for a specific brand."""
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT p.* FROM metadata.promociones p
            JOIN metadata.promociones_marcas pm ON p.id = pm.promocion_id
            WHERE pm.marca_id = %s AND p.activo = TRUE
            ORDER BY p.titulo
            """,
            (marca_id,)
        )
        return [Promocion(**row) for row in cur.fetchall()]


def get_promociones_by_day(conn: Connection, day_of_week: str) -> List[PromocionConMarca]:
    """Get promotions valid on a specific day (e.g., 'Miércoles')."""
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT * FROM metadata.promociones_con_marcas
            WHERE vigencia ILIKE %s
            ORDER BY marca_nombre, titulo
            """,
            (f'%{day_of_week}%',)
        )
        return [PromocionConMarca(**row) for row in cur.fetchall()]


def insert_promocion(
    conn: Connection,
    titulo: str,
    banco: Optional[str] = None,
    descuento: Optional[str] = None,
    vigencia: Optional[str] = None,
    fuente_url: Optional[str] = None,
    fuente_tipo: Optional[str] = None,
    marca_ids: Optional[Sequence[int]] = None,
    scrape_run_id: Optional[int] = None,
    *,
    external_id: Optional[str] = None,
    fecha_inicio: Optional[date] = None,
    fecha_fin: Optional[date] = None,
    activo: bool = True,
) -> int:
    """Insert or update a promotion and optionally link it to brands.

    Upserts based on the pair (fuente_tipo, external_id) when external_id is provided.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO metadata.promociones
                (titulo, banco, descuento, vigencia, fuente_url, fuente_tipo,
                 external_id, fecha_inicio, fecha_fin, activo, scrape_run_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (fuente_tipo, external_id) DO UPDATE SET
                titulo = EXCLUDED.titulo,
                banco = EXCLUDED.banco,
                descuento = EXCLUDED.descuento,
                vigencia = EXCLUDED.vigencia,
                fuente_url = EXCLUDED.fuente_url,
                fecha_inicio = EXCLUDED.fecha_inicio,
                fecha_fin = EXCLUDED.fecha_fin,
                activo = EXCLUDED.activo,
                scrape_run_id = COALESCE(EXCLUDED.scrape_run_id, promociones.scrape_run_id),
                updated_at = NOW()
            RETURNING id
            """,
            (
                titulo,
                banco,
                descuento,
                vigencia,
                fuente_url,
                fuente_tipo,
                external_id,
                fecha_inicio,
                fecha_fin,
                activo,
                scrape_run_id,
            )
        )
        promocion_id = cur.fetchone()[0]
        
        # Link to brands if provided
        if marca_ids:
            cur.execute(
                "DELETE FROM metadata.promociones_marcas WHERE promocion_id = %s",
                (promocion_id,),
            )
            for marca_id in marca_ids:
                cur.execute(
                    """
                    INSERT INTO metadata.promociones_marcas (promocion_id, marca_id)
                    VALUES (%s, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    (promocion_id, marca_id)
                )
        
        conn.commit()
        return promocion_id


def link_promocion_to_marca(conn: Connection, promocion_id: int, marca_id: int) -> None:
    """Link a promotion to a brand."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO metadata.promociones_marcas (promocion_id, marca_id)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING
            """,
            (promocion_id, marca_id)
        )
        conn.commit()


def auto_link_promocion_to_marcas(conn: Connection, promocion_id: int) -> List[int]:
    """
    Automatically link a promotion to brands based on text analysis.
    Returns list of marca_ids that were linked.
    """
    with conn.cursor(row_factory=dict_row) as cur:
        # Get the promotion
        cur.execute("SELECT titulo, banco FROM metadata.promociones WHERE id = %s", (promocion_id,))
        promo = cur.fetchone()
        if not promo:
            return []
        
        # Try to find brand in title or banco field
        search_text = f"{promo.get('titulo', '')} {promo.get('banco', '')}"
        
        cur.execute(
            """
            SELECT id FROM metadata.marcas
            WHERE activo = TRUE
              AND (%s ILIKE '%%' || nombre || '%%' OR %s ILIKE '%%' || nombre_display || '%%')
            ORDER BY LENGTH(nombre) DESC
            """,
            (search_text, search_text)
        )
        
        linked_ids = []
        for row in cur.fetchall():
            marca_id = row['id']
            link_promocion_to_marca(conn, promocion_id, marca_id)
            linked_ids.append(marca_id)
        
        return linked_ids


# =============================================================================
# ESTACIONES CON PROMOCIONES (Stations with Promotions)
# =============================================================================

def get_estaciones_con_promociones(
    conn: Connection,
    *,
    marca_id: Optional[int] = None,
    cod_region: Optional[str] = None
) -> Dict[int, EstacionConPromociones]:
    """
    Get stations with their applicable promotions.
    Returns a dict mapping estacion_id to EstacionConPromociones.
    """
    sql = """
        SELECT 
            e.id as estacion_id,
            e.codigo,
            e.marca,
            m.nombre_display as marca_display,
            e.direccion,
            e.comuna,
            e.region,
            e.lat,
            e.lng,
            p.id as promocion_id,
            p.titulo as promocion_titulo,
            p.banco,
            p.descuento,
            p.vigencia,
            p.fuente_url
        FROM metadata.estaciones_cne e
        JOIN metadata.marcas m ON e.marca_id = m.id
        LEFT JOIN metadata.promociones_marcas pm ON m.id = pm.marca_id
        LEFT JOIN metadata.promociones p ON pm.promocion_id = p.id AND p.activo = TRUE
        WHERE 1=1
    """
    params = []
    
    if marca_id:
        sql += " AND e.marca_id = %s"
        params.append(marca_id)
    
    if cod_region:
        sql += " AND e.cod_region = %s"
        params.append(cod_region)
    
    sql += " ORDER BY e.id, p.titulo"
    
    estaciones_dict: Dict[int, EstacionConPromociones] = {}
    
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, params or None)
        
        for row in cur.fetchall():
            estacion_id = row['estacion_id']
            
            if estacion_id not in estaciones_dict:
                estaciones_dict[estacion_id] = EstacionConPromociones(
                    estacion_id=estacion_id,
                    codigo=row['codigo'],
                    marca=row['marca'],
                    marca_display=row['marca_display'],
                    direccion=row['direccion'],
                    comuna=row['comuna'],
                    region=row['region'],
                    lat=row['lat'],
                    lng=row['lng'],
                    promociones=[]
                )
            
            # Add promotion if exists
            if row.get('promocion_id'):
                promo = Promocion(
                    id=row['promocion_id'],
                    titulo=row['promocion_titulo'],
                    banco=row['banco'],
                    descuento=row['descuento'],
                    vigencia=row['vigencia'],
                    fuente_url=row['fuente_url']
                )
                estaciones_dict[estacion_id].promociones.append(promo)
    
    return estaciones_dict


def get_promociones_estacion(conn: Connection, codigo: str) -> List[Promocion]:
    """Get all promotions applicable to a specific station by its CNE code."""
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            "SELECT * FROM metadata.get_promociones_estacion(%s)",
            (codigo,)
        )
        return [Promocion(**row) for row in cur.fetchall()]


# =============================================================================
# SCRAPE RUNS (Import Tracking)
# =============================================================================

def create_scrape_run(
    conn: Connection,
    source_type: str,
    source_url: Optional[str] = None,
    record_count: Optional[int] = None,
    success: bool = True,
    error_message: Optional[str] = None,
) -> int:
    """Create a new scrape run record and return its ID."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO metadata.scrape_runs
                (source_type, source_url, record_count, success, error_message)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id
            """,
            (source_type, source_url, record_count, success, error_message)
        )
        result = cur.fetchone()
        conn.commit()
        return result[0]


def get_latest_scrape_run(conn: Connection, source_type: str) -> Optional[ScrapeRun]:
    """Get the most recent scrape run for a source type."""
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT * FROM metadata.scrape_runs
            WHERE source_type = %s
            ORDER BY scraped_at DESC
            LIMIT 1
            """,
            (source_type,)
        )
        row = cur.fetchone()
        return ScrapeRun(**row) if row else None


# =============================================================================
# BULK IMPORT HELPERS
# =============================================================================

def bulk_import_estaciones_from_cne(
    conn: Connection,
    estaciones_data: List[Dict],
    scrape_run_id: Optional[int] = None
) -> int:
    """
    Bulk import stations from CNE JSON data.
    Returns the number of stations imported.
    """
    count = 0
    for est in estaciones_data:
        upsert_estacion(
            conn,
            codigo=est.get('codigo'),
            marca=est.get('marca'),
            razon_social=est.get('razon_social'),
            direccion=est.get('direccion'),
            region=est.get('region'),
            cod_region=est.get('cod_region'),
            comuna=est.get('comuna'),
            cod_comuna=est.get('cod_comuna'),
            lat=float(est['lat']) if est.get('lat') else None,
            lng=float(est['lng']) if est.get('lng') else None,
            scrape_run_id=scrape_run_id
        )
        count += 1
    
    return count


def bulk_import_precios_from_cne(
    conn: Connection,
    estaciones_data: List[Dict],
    scrape_run_id: Optional[int] = None
) -> int:
    """
    Bulk import prices from CNE JSON data (nested in station records).
    Returns the number of price records imported.
    """
    count = 0
    
    # First, get station code to ID mapping
    codigo_to_id = {}
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("SELECT id, codigo FROM metadata.estaciones_cne")
        for row in cur.fetchall():
            codigo_to_id[row['codigo']] = row['id']
    
    for est in estaciones_data:
        codigo = est.get('codigo')
        if not codigo or codigo not in codigo_to_id:
            continue
        
        estacion_id = codigo_to_id[codigo]
        
        # Import prices for each fuel type
        for fuel_type in ['precio_93', 'precio_95', 'precio_97', 'precio_DI']:
            precio_data = est.get(fuel_type)
            if not precio_data or not isinstance(precio_data, dict):
                continue
            
            precio_val = precio_data.get('precio')
            if precio_val is None:
                continue
            
            # Convert precio to decimal (CNE sends large numbers, divide by 1000)
            try:
                precio_decimal = Decimal(str(precio_val)) / 1000
            except:
                continue
            
            tipo_comb = fuel_type.replace('precio_', '')
            fecha_str = precio_data.get('fecha')
            hora_str = precio_data.get('hora')
            
            insert_precio(
                conn,
                estacion_id=estacion_id,
                tipo_combustible=tipo_comb,
                precio=precio_decimal,
                unidad=precio_data.get('unidad'),
                fecha=datetime.strptime(fecha_str, '%Y-%m-%d').date() if fecha_str else None,
                hora=datetime.strptime(hora_str, '%H:%M:%S').time() if hora_str else None,
                tipo_atencion=precio_data.get('tipo_atencion'),
                scrape_run_id=scrape_run_id
            )
            count += 1
    
    return count


def bulk_import_promociones(
    conn: Connection,
    promociones_data: List[Dict],
    fuente_tipo: str,
    scrape_run_id: Optional[int] = None
) -> int:
    """
    Bulk import promotions and auto-link to brands.
    Returns the number of promotions imported.
    """
    count = 0
    for promo in promociones_data:
        marca_ids_value = promo.get('marca_ids')
        if isinstance(marca_ids_value, int):
            marca_ids = [marca_ids_value]
        elif isinstance(marca_ids_value, (list, tuple)):
            marca_ids = [int(mid) for mid in marca_ids_value if mid is not None]
        else:
            marca_ids = None

        fecha_inicio = promo.get('fecha_inicio')
        if isinstance(fecha_inicio, str):
            try:
                fecha_inicio = datetime.fromisoformat(fecha_inicio).date()
            except ValueError:
                fecha_inicio = None

        fecha_fin = promo.get('fecha_fin')
        if isinstance(fecha_fin, str):
            try:
                fecha_fin = datetime.fromisoformat(fecha_fin).date()
            except ValueError:
                fecha_fin = None

        promo_id = insert_promocion(
            conn,
            titulo=promo.get('titulo', ''),
            banco=promo.get('banco'),
            descuento=promo.get('descuento'),
            vigencia=promo.get('vigencia'),
            fuente_url=promo.get('fuente'),
            fuente_tipo=fuente_tipo,
            marca_ids=marca_ids,
            scrape_run_id=scrape_run_id,
            external_id=promo.get('external_id'),
            fecha_inicio=fecha_inicio,
            fecha_fin=fecha_fin,
            activo=promo.get('activo', True),
        )
        
        # Auto-link to brands based on text if not provided explicitly
        if not marca_ids:
            auto_link_promocion_to_marcas(conn, promo_id)
        count += 1
    
    return count


def delete_promociones_by_fuente(conn: Connection, fuente_tipo: str) -> int:
    """Delete promotions for a given source type. Returns number of rows removed."""
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM metadata.promociones WHERE fuente_tipo = %s",
            (fuente_tipo,)
        )
        deleted = cur.rowcount
        conn.commit()
        return deleted
