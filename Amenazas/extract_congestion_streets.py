import googlemaps
from datetime import datetime
import json 

API_KEY_GOOGLE = ""

def calcular_porcentaje_congestion(duracion_normal, duracion_trafico):
    if duracion_normal == 0 or duracion_trafico <= duracion_normal:
        return 0.0
    
    retraso = duracion_trafico - duracion_normal
    porcentaje = (retraso / duracion_normal) * 100
    return round(porcentaje, 2)

def analizar_congestion_ruta_google(api_key, origen, destino, umbral_congestion_pct=30.0):
    try:
        gmaps = googlemaps.Client(key=api_key)
        
       
        print(f"Consultando la API de Google Maps:")
        print(f"   De: {origen}")
        print(f"   A:  {destino}\n")
        
        now = datetime.now()
        directions_result = gmaps.directions(origen,
                                             destino,
                                             mode="driving",
                                             departure_time=now)
    except Exception as e:
        print(f"No se pudo conectar con la API de Google.")
        print(e)
        return None

    if not directions_result:
        print("No se encontro una ruta de Google Maps.")
        return None

    ruta_principal = directions_result[0]['legs'][0]
    lista_segmentos_json = []

    for paso in ruta_principal['steps']:
    
        duracion_paso_normal = paso['duration']['value']
        duracion_paso_trafico_data = paso.get('duration_in_traffic')
        
        if duracion_paso_trafico_data:
            duracion_paso_trafico = duracion_paso_trafico_data['value']
        else:
            duracion_paso_trafico = duracion_paso_normal

        congestion_segmento_pct = calcular_porcentaje_congestion(duracion_paso_normal, duracion_paso_trafico)
        congestion_normalizada = round(congestion_segmento_pct / 100.0, 4)
        congestion_final = min(congestion_normalizada, 1.0)
        
        instruccion = paso['html_instructions'].replace('<b>', '').replace('</b>', '').replace('</div>', '').replace('<div style="font-size:0.9em">', ' (') + ')'

        if congestion_segmento_pct > umbral_congestion_pct:
            print(f"Segmento CONGESTIONADO: ({congestion_segmento_pct}% de retraso)")
            print(f"           Lugar: {instruccion}")
        else:
            print(f"Segmento despejado: ({congestion_segmento_pct}%)")
            print(f"           Lugar: {instruccion}")

     
        segmento_data = {
            'instruccion': instruccion,
            'distancia': paso['distance']['text'],
            'duracion_normal_seg': duracion_paso_normal,
            'duracion_trafico_seg': duracion_paso_trafico,
            'congestion': congestion_final  
        }
        lista_segmentos_json.append(segmento_data)
            
    return lista_segmentos_json


def main():
    origen = "Palacio de La Moneda, Santiago, Chile"
    destino = "Costanera Center, Santiago, Chile"
    
    segmentos_data = analizar_congestion_ruta_google(
        API_KEY_GOOGLE, 
        origen, 
        destino, 
        umbral_congestion_pct=30.0 
    )
    
    if segmentos_data:
        json_output = json.dumps(segmentos_data, indent=2, ensure_ascii=False)
        print(json_output)
        
        ruta_salida = "Amenazas/outputs/resultados_concurrencia_calles.json"
 
        with open(ruta_salida, 'w', encoding='utf-8') as f:
            f.write(json_output)
    

if __name__ == "__main__":
    main()