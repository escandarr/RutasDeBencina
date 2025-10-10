import PatenteChileCalls
import ConsumoVehicularCalls

if __name__ == "__main__":
    patente_a_buscar = "gyvb70"
    
    final_html = PatenteChileCalls.fetch_vehicle_data(patente_a_buscar)
    
    if final_html:
        vehicle_info = PatenteChileCalls.parse_vehicle_data(final_html)

        fuel_consumption = ConsumoVehicularCalls.find_best_match_consumption(vehicle_info)

        print("\n" + "="*40)
        if fuel_consumption is not None:
            print(f"✅ Resultado Final: El consumo de combustible mixto estimado es de: {fuel_consumption} km/L")
        else:
            print(f"❌ Resultado Final: No se pudo determinar el consumo de combustible para el vehículo.")
        print("="*40)