from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

sys.path.insert(0, str(Path(__file__).parent.parent / "services"))

import PatenteChileCalls
import ConsumoVehicularCalls


OUTPUT_FILE = Path(__file__).parent.parent / "outputs" / "consumo" / "resultado_consumo.json"


def save_results_to_json(payload: Dict[str, Any], output_path: Path = OUTPUT_FILE) -> None:
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def main(patente_a_buscar = "gyvb70") -> None:

    resultado: Dict[str, Any] = {
        "patente": patente_a_buscar,
        "generadoEn": datetime.now(timezone.utc).isoformat(),
    }

    final_html = PatenteChileCalls.fetch_vehicle_data(patente_a_buscar)

    if not final_html:
        resultado["mensaje"] = "Sin datos de vehículo"
        save_results_to_json(resultado)
        return

    vehicle_info = PatenteChileCalls.parse_vehicle_data(final_html)
    resultado["vehiculo"] = vehicle_info

    fuel_consumption = ConsumoVehicularCalls.find_best_match_consumption(vehicle_info)

    consumo_valor: Any
    detalle_consumo: Dict[str, Any] | None = None

    if isinstance(fuel_consumption, dict):
        detalle_consumo = fuel_consumption
        consumo_valor = fuel_consumption.get("rendimientoMixto")
    else:
        consumo_valor = fuel_consumption

    resultado["consumoMixtoKmL"] = consumo_valor
    if detalle_consumo:
        resultado["detalleConsumo"] = detalle_consumo

    save_results_to_json(resultado)


if __name__ == "__main__":
    main()




