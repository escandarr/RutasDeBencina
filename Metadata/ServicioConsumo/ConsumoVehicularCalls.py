from datetime import datetime
import requests
import json

baseURL = "https://www.consumovehicular.cl/backend/"

def get_marcas_consumo():
    """Obtiene todas las marcas de vehículos."""
    try:
        url = f"{baseURL}scv/vehiculo/marcas"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error al obtener las marcas: {e}")
        return None

def get_modelos_consumo(marca_id):
    """Obtiene todos los modelos para un ID de marca dado."""
    try:
        url = f"{baseURL}scv/vehiculo/modelos?idMarca={marca_id}"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error al obtener los modelos para el ID de marca {marca_id}: {e}")
        return None

def get_versiones_consumo(modelo_id):
    """Obtiene todas las versiones/etiquetas para un ID de modelo dado."""
    try:
        url = f"{baseURL}scv/vehiculo/etiquetas?idModelo={modelo_id}"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error al obtener las versiones para el ID de modelo {modelo_id}: {e}")
        return None

def get_consumo_vehicular(id_marca, id_modelo, id_etiqueta):
    """Obtiene los datos detallados de consumo."""
    try:
        url = f"{baseURL}scv/vehiculo/?criterio=idMarca:EQ:{id_marca};idModelo:EQ:{id_modelo};idEtiqueta:EQ:{id_etiqueta}&page=0&size=5&sort=nombreMarca"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error al obtener los datos de consumo: {e}")
        return None
    

def find_best_match_consumption(vehicle_info):
    """
    Encuentra el consumo de combustible para un vehículo haciéndolo coincidir con la API de consumovehicular.

    Args:
        vehicle_info (dict): Un diccionario con los detalles del vehículo 
                             (debe contener 'Marca', 'Modelo', 'Año', 'Combustible').

    Returns:
        float: El consumo de combustible mixto (rendimientoMixto) o None si no se encuentra una coincidencia.
    """
    print("Iniciando búsqueda para el vehículo:", vehicle_info.get('Modelo'))
    
    # 1. Encontrar ID de la Marca
    all_marcas = get_marcas_consumo()
    if not all_marcas:
        return None
        
    matched_marca = next(
        (marca for marca in all_marcas if marca['nombre'].lower() == vehicle_info['Marca'].lower()),
        None
    )

    if not matched_marca:
        print(f"-> Marca '{vehicle_info['Marca']}' no encontrada en la base de datos.")
        return None
    
    id_marca = matched_marca['idMarca']
    print(f"-> Marca encontrada: '{matched_marca['nombre']}' (ID: {id_marca})")

    # 2. Encontrar la Mejor Coincidencia de Modelo
    all_modelos = get_modelos_consumo(id_marca)
    if not all_modelos:
        print(f"-> No se encontraron modelos para la marca '{vehicle_info['Marca']}'.")
        return None

    best_model = None
    max_score = 0
    
    # El nombre descriptivo de la primera API (ej: "wingle 5 lux 4x4 2.0")
    search_model_name_lower = vehicle_info['Modelo'].lower()

    for api_model in all_modelos:
        current_score = 0
        # El nombre más simple de la segunda API (ej: "Wingle 5")
        api_model_name_lower = api_model['nombre'].lower()
        
        # La puntuación se basa en cuántas palabras del nombre simple están en el nombre descriptivo
        for word in api_model_name_lower.split():
            if word in search_model_name_lower:
                current_score += 1
        
        if current_score > max_score:
            max_score = current_score
            best_model = api_model

    if not best_model or max_score == 0:
        print(f"-> No se pudo encontrar un modelo probable para '{vehicle_info['Modelo']}'.")
        return None

    id_modelo = best_model['idModelo']
    print(f"-> Mejor Coincidencia de Modelo: '{best_model['nombre']}' (ID: {id_modelo}) con una puntuación de {max_score}")

    # 3. Encontrar la Mejor Versión (Etiqueta) basada en Combustible y Año
    all_versiones = get_versiones_consumo(id_modelo)
    if not all_versiones:
        print(f"-> No se encontraron versiones para el modelo '{best_model['nombre']}'.")
        return None

    target_fuel = vehicle_info['Combustible'].lower()
    target_year = int(vehicle_info['Año'])
    
    potential_matches = []
    for version in all_versiones:
        # El nombre de la versión a menudo contiene el tipo de combustible
        if target_fuel in version['nombre'].lower():
            potential_matches.append(version)
    
    if not potential_matches:
        print(f"-> No se encontró una versión con el tipo de combustible '{target_fuel}'. Seleccionando la primera versión disponible.")
        potential_matches.append(all_versiones[0])  # Se recurre a la primera versión como alternativa


    best_version = potential_matches[0]
    id_etiqueta = best_version['idEtiqueta']
    print(f"-> Versión Seleccionada: '{best_version['nombre']}' (ID: {id_etiqueta})")

    # 4. Obtener Datos Finales de Consumo
    consumo_data = get_consumo_vehicular(id_marca, id_modelo, id_etiqueta)
    
    if consumo_data and consumo_data.get('content'):
        # Filtrar resultados por el año más cercano
        best_vehicle_details = None
        min_year_diff = float('inf')

        for vehicle_details in consumo_data['content']:
            homologation_timestamp_ms = vehicle_details.get('fechaHomologacion')
            if homologation_timestamp_ms:
                homologation_year = datetime.fromtimestamp(homologation_timestamp_ms / 1000).year
                year_diff = abs(homologation_year - target_year)
                
                if year_diff < min_year_diff:
                    min_year_diff = year_diff
                    best_vehicle_details = vehicle_details
        
        if best_vehicle_details:
             rendimiento_mixto = best_vehicle_details.get('rendimientoMixto')
             print(f"-> Datos de consumo encontrados exitosamente para el año ~{target_year}.")
             return rendimiento_mixto

    print("-> No se pudieron obtener los datos finales de consumo para la versión encontrada.")
    return None