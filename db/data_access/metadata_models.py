"""Data models for the metadata schema."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal
from typing import Optional


@dataclass
class Marca:
    """Gas station brand."""
    id: int
    nombre: str
    nombre_display: Optional[str] = None
    logo_url: Optional[str] = None
    sitio_web: Optional[str] = None
    color_hex: Optional[str] = None
    activo: bool = True


@dataclass
class Estacion:
    """CNE gas station."""
    id: int
    codigo: str
    marca: Optional[str] = None
    marca_id: Optional[int] = None
    razon_social: Optional[str] = None
    direccion: Optional[str] = None
    region: Optional[str] = None
    cod_region: Optional[str] = None
    comuna: Optional[str] = None
    cod_comuna: Optional[str] = None
    lat: Optional[float] = None
    lng: Optional[float] = None
    distance_m: Optional[float] = None  # For nearest station queries


@dataclass
class Precio:
    """Fuel price at a station."""
    id: int
    estacion_id: int
    tipo_combustible: str  # '93', '95', '97', 'DI'
    precio: Optional[Decimal] = None
    unidad: Optional[str] = None
    fecha: Optional[date] = None
    hora: Optional[time] = None
    tipo_atencion: Optional[str] = None


@dataclass
class PrecioActual:
    """Current price with station info."""
    estacion_id: int
    codigo: str
    marca: str
    comuna: Optional[str] = None
    region: Optional[str] = None
    tipo_combustible: str = ''
    precio: Optional[Decimal] = None
    fecha: Optional[date] = None
    hora: Optional[time] = None
    lat: Optional[float] = None
    lng: Optional[float] = None
    distance_m: Optional[float] = None


@dataclass
class Promocion:
    """Fuel promotion from banks or partnerships."""
    id: int
    titulo: str
    banco: Optional[str] = None
    descuento: Optional[str] = None
    vigencia: Optional[str] = None
    fuente_url: Optional[str] = None
    fuente_tipo: Optional[str] = None
    external_id: Optional[str] = None
    fecha_inicio: Optional[date] = None
    fecha_fin: Optional[date] = None
    activo: bool = True


@dataclass
class PromocionConMarca:
    """Promotion with associated brand information."""
    promocion_id: int
    titulo: str
    banco: Optional[str] = None
    descuento: Optional[str] = None
    vigencia: Optional[str] = None
    fuente_url: Optional[str] = None
    external_id: Optional[str] = None
    fecha_inicio: Optional[date] = None
    fecha_fin: Optional[date] = None
    marca_id: Optional[int] = None
    marca_nombre: Optional[str] = None
    marca_display: Optional[str] = None


@dataclass
class EstacionConPromociones:
    """Station with all applicable promotions."""
    estacion_id: int
    codigo: str
    marca: Optional[str] = None
    marca_display: Optional[str] = None
    direccion: Optional[str] = None
    comuna: Optional[str] = None
    region: Optional[str] = None
    lat: Optional[float] = None
    lng: Optional[float] = None
    promociones: list[Promocion] = None
    
    def __post_init__(self):
        if self.promociones is None:
            self.promociones = []


@dataclass
class ConsumoVehicular:
    """Vehicle fuel consumption data."""
    id: int
    patente: Optional[str] = None
    marca: Optional[str] = None
    modelo: Optional[str] = None
    año: Optional[int] = None
    tipo_combustible: Optional[str] = None
    rendimiento_ciudad: Optional[Decimal] = None
    rendimiento_carretera: Optional[Decimal] = None
    rendimiento_mixto: Optional[Decimal] = None
    fuente: Optional[str] = None


@dataclass
class ScrapeRun:
    """Metadata scraping job tracking."""
    id: int
    source_type: str
    source_url: Optional[str] = None
    scraped_at: Optional[datetime] = None
    record_count: Optional[int] = None
    success: bool = True
    error_message: Optional[str] = None

