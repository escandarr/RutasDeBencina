import psycopg
import sys

# Esta es la URL de conexión a tu base de datos en Docker
DB_CONNECTION_STRING = "postgresql://rutas_user:supersecretpassword@localhost:5432/rutasdb"

def get_amenazas_ejemplo():
    """
    Simula la obtención de datos de amenazas (Punto 4).
    
    A futuro, esta función debería llamar a 'extract_congestion_streets.py'
    y retornar los datos de congestión reales.
    
    Por ahora, solo busca calles con un nombre específico para simular.
    """
    print("Obteniendo datos de amenazas (simulación)...")
    
    # Ejemplo: Vamos a simular que todas las calles "Avenida Providencia"
    # tienen una congestión 'alta'.
    # El script buscará estas calles y les asignará una probabilidad.
    return [
        {'nombre_calle': 'Avenida Providencia', 'probabilidad': 80},
        {'nombre_calle': 'Avenida Vitacura', 'probabilidad': 60}
    ]

def aplicar_probabilidades_en_db(amenazas):
    """
    Se conecta a la BD y actualiza la columna 'probabilidad_falla'
    en la tabla 'osm.road_edges' basado en las amenazas.
    """
    if not amenazas:
        print("No hay amenazas para aplicar.")
        return

    print(f"Conectando a la base de datos en {DB_CONNECTION_STRING.split('@')[-1]}...")
    
    try:
        with psycopg.connect(DB_CONNECTION_STRING) as conn:
            with conn.cursor() as cur:
                
                total_actualizado = 0
                for amenaza in amenazas:
                    nombre = amenaza['nombre_calle']
                    prob = amenaza['probabilidad']
                    
                    print(f"-> Aplicando {prob}% de falla a calles llamadas '{nombre}'...")
                    
                    # Ejecutamos el UPDATE en la tabla correcta
                    # (Usamos 'tags->'name'' que es como Overpass guarda los nombres)
                    cur.execute(
                        """
                        UPDATE osm.road_edges
                        SET probabilidad_falla = %s
                        WHERE tags->'name' = %s
                        """,
                        (prob, nombre)
                    )
                    
                    # cur.rowcount nos dice cuántas filas se actualizaron
                    if cur.rowcount > 0:
                        print(f"   ... {cur.rowcount} calle(s) actualizada(s).")
                        total_actualizado += cur.rowcount

                conn.commit()
                print(f"\n Éxito: Se actualizaron un total de {total_actualizado} calles en la BD.")

    except Exception as e:
        print(f"\n ERROR al conectar o actualizar la base de datos:", file=sys.stderr)
        print(e, file=sys.stderr)

def main():
    print("--- Iniciando script de aplicación de probabilidades (Punto 4) ---")
    # 1. Obtiene los datos de amenazas (ahora simulados)
    lista_amenazas = get_amenazas_ejemplo()
    
    # 2. Aplica esos datos a la BD
    aplicar_probabilidades_en_db(lista_amenazas)
    print("--- Script finalizado ---")

if __name__ == "__main__":
    main()