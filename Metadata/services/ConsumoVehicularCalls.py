from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

import requests


class ConsumoVehicularClient:
    """Cliente HTTP para la API de consumovehicular."""

    BASE_URL = "https://api-consumovehicular.minenergia.cl"
    DEFAULT_TIMEOUT = 10
    DEFAULT_HEADERS = {
        "Accept": "application/json, text/plain, */*",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
        "Origin": "https://www.consumovehicular.cl",
        "Referer": "https://www.consumovehicular.cl/",
    }

    def __init__(
        self,
        session: Optional[requests.Session] = None,
        timeout: Optional[int] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> None:
        self.session = session or requests.Session()
        self.timeout = timeout or self.DEFAULT_TIMEOUT

        merged_headers = dict(self.DEFAULT_HEADERS)
        if headers:
            merged_headers.update(headers)

        # Evitar sobrescribir cabeceras personalizadas ya presentes en la sesión.
        for key, value in merged_headers.items():
            self.session.headers.setdefault(key, value)

    def __enter__(self) -> "ConsumoVehicularClient":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:  # type: ignore[override]
        self.close()

    def close(self) -> None:
        self.session.close()

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None, timeout: Optional[int] = None) -> Any:
        url = f"{self.BASE_URL}{path}"
        response = self.session.get(url, params=params, timeout=timeout or self.timeout)
        response.raise_for_status()
        return response.json()

    def _fetch_paginated(self, path: str, base_params: Dict[str, Any]) -> List[Dict[str, Any]]:
        resultados: List[Dict[str, Any]] = []
        params = dict(base_params)

        while True:
            try:
                payload = self._get(path, params=params)
            except requests.RequestException as exc:
                print(f"Error al obtener datos desde {path} con parámetros {params}: {exc}")
                break

            items = payload.get("items") or []
            resultados.extend(items)

            total_pages = payload.get("totalPages") or 1
            current_index = params.get("PageIndex", 1)
            if current_index >= total_pages:
                break

            params["PageIndex"] = current_index + 1

        return resultados

    # Métodos públicos -----------------------------------------------------
    def fetch_marcas(self) -> List[Dict[str, Any]]:
        try:
            data = self._get("/marcas")
            return data if isinstance(data, list) else []
        except requests.RequestException as exc:
            print(f"Error al obtener las marcas: {exc}")
            return []

    def fetch_modelos(self, marca_id: int, page_size: int = 1000) -> List[Dict[str, Any]]:
        return self._fetch_paginated(
            "/modelos",
            {
                "MarcaId": marca_id,
                "PageIndex": 1,
                "PageSize": page_size,
            },
        )

    def fetch_vehiculos(self, modelo_id: int, page_size: int = 1000) -> List[Dict[str, Any]]:
        return self._fetch_paginated(
            "/vehiculos/listar",
            {
                "ModeloId": modelo_id,
                "PageIndex": 1,
                "PageSize": page_size,
            },
        )


# Funciones de compatibilidad -----------------------------------------------
def get_marcas_consumo() -> List[Dict[str, Any]]:
    with ConsumoVehicularClient() as client:
        return client.fetch_marcas()


def get_modelos_consumo(marca_id: int) -> List[Dict[str, Any]]:
    with ConsumoVehicularClient() as client:
        return client.fetch_modelos(marca_id)


def get_vehiculos_consumo(modelo_id: int) -> List[Dict[str, Any]]:
    with ConsumoVehicularClient() as client:
        return client.fetch_vehiculos(modelo_id)


def _extraer_anio(fecha: Optional[str]) -> Optional[int]:
    if not fecha:
        return None

    try:
        # La API devuelve fecha en formato ISO (sin o con zona horaria).
        fecha_normalizada = fecha.replace("Z", "")
        return datetime.fromisoformat(fecha_normalizada).year
    except ValueError:
        return None


def find_best_match_consumption(vehicle_info: Dict[str, Any], client: Optional[ConsumoVehicularClient] = None) -> Optional[float]:
    """Encuentra el rendimiento mixto más cercano para un vehículo."""

    print("Iniciando búsqueda para el vehículo:", vehicle_info.get("Modelo"))

    propio_client = client is None
    client = client or ConsumoVehicularClient()

    try:
        # 1. Identificar la marca
        marcas = client.fetch_marcas()
        if not marcas:
            return None

        marca_nombre = (vehicle_info.get("Marca") or "").lower()
        matched_marca = next(
            (marca for marca in marcas if marca.get("nombre", "").lower() == marca_nombre),
            None,
        )

        if not matched_marca:
            print(f"-> Marca '{vehicle_info.get('Marca')}' no encontrada en la base de datos.")
            return None

        id_marca = matched_marca.get("id")
        if id_marca is None:
            print("-> La API devolvió una marca sin identificador válido.")
            return None
        print(f"-> Marca encontrada: '{matched_marca.get('nombre')}' (ID: {id_marca})")

        # 2. Encontrar el mejor modelo
        modelos = client.fetch_modelos(id_marca)
        if not modelos:
            print(f"-> No se encontraron modelos para la marca '{vehicle_info.get('Marca')}'.")
            return None

        best_model: Optional[Dict[str, Any]] = None
        max_score = 0
        search_model_name_lower = (vehicle_info.get("Modelo") or "").lower()

        for api_model in modelos:
            api_model_name_lower = (api_model.get("nombre") or "").lower()
            current_score = sum(
                1 for word in api_model_name_lower.split() if word and word in search_model_name_lower
            )

            if current_score > max_score:
                max_score = current_score
                best_model = api_model

        if not best_model or max_score == 0:
            print(f"-> No se pudo encontrar un modelo probable para '{vehicle_info.get('Modelo')}'.")
            return None

        id_modelo = best_model.get("id")
        if id_modelo is None:
            print("-> El modelo seleccionado no posee identificador válido en la API.")
            return None
        print(
            "-> Mejor Coincidencia de Modelo: '",
            best_model.get("nombre"),
            "' (ID: ",
            id_modelo,
            ") con una puntuación de ",
            max_score,
            sep="",
        )

        # 3. Obtener vehículos y filtrar por combustible/año
        vehiculos = client.fetch_vehiculos(id_modelo)
        if not vehiculos:
            print(f"-> No se encontraron vehículos registrados para el modelo '{best_model.get('nombre')}'.")
            return None

        target_fuel = (vehicle_info.get("Combustible") or "").lower()
        if target_fuel:
            vehiculos_filtrados = [v for v in vehiculos if target_fuel in (v.get("combustible") or "").lower()]
            if vehiculos_filtrados:
                vehiculos = vehiculos_filtrados
            else:
                print(
                    f"-> No se encontró un vehículo con combustible '{target_fuel}'. Se continuará con todas las opciones disponibles."
                )

        try:
            target_year = int(vehicle_info.get("Año"))
        except (TypeError, ValueError):
            target_year = None

        best_vehicle = None
        best_year_diff = float("inf")

        for vehiculo in vehiculos:
            vehiculo_year = _extraer_anio(vehiculo.get("fechaHomologacion"))
            if target_year is not None and vehiculo_year is not None:
                year_diff = abs(vehiculo_year - target_year)
            else:
                year_diff = float("inf")

            if best_vehicle is None or year_diff < best_year_diff:
                best_vehicle = vehiculo
                best_year_diff = year_diff

        if not best_vehicle:
            print("-> No se encontraron coincidencias adecuadas de vehículos.")
            return None

        consumo = (
            best_vehicle.get("rendimientoMixto")
            or best_vehicle.get("rendimientoCombinadoCombustible")
            or best_vehicle.get("rendimientoPonderadoCombustible")
        )

        if consumo is not None:
            return best_vehicle
        return None
    finally:
        if propio_client:
            client.close()