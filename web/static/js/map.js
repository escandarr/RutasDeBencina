const MAP_CONTAINER_ID = "map";
const STATUS_ELEMENT_ID = "status";
const ROUTE_ENDPOINT = "/api/routes/shortest";
const START_BUTTON_ID = "select-start";
const END_BUTTON_ID = "select-end";
const COMPUTE_BUTTON_ID = "compute-route";
const CLEAR_BUTTON_ID = "clear-route";
const START_SUMMARY_ID = "start-summary";
const END_SUMMARY_ID = "end-summary";

let map;
let startMarker = null;
let endMarker = null;
let routeLayer = null;
let currentMode = "start";

function updateStatus(message, isError = false) {
	const statusEl = document.getElementById(STATUS_ELEMENT_ID);
	if (!statusEl) {
		return;
	}

	statusEl.textContent = message;
	statusEl.style.color = isError ? "#fca5a5" : "#cbd5f5";
}

function formatLatLng(latlng) {
	const lat = latlng.lat.toFixed(6);
	const lng = latlng.lng.toFixed(6);
	return `Lat: ${lat} | Lon: ${lng}`;
}

function syncButtonStates() {
	const startButton = document.getElementById(START_BUTTON_ID);
	const endButton = document.getElementById(END_BUTTON_ID);
	const computeButton = document.getElementById(COMPUTE_BUTTON_ID);

	if (startButton) {
		startButton.textContent = currentMode === "start" ? "Origen activo" : "Seleccionar origen";
		startButton.classList.toggle("cta", currentMode === "start");
		startButton.classList.toggle("secondary", currentMode !== "start");
	}

	if (endButton) {
		endButton.textContent = currentMode === "end" ? "Destino activo" : "Seleccionar destino";
		endButton.classList.toggle("cta", currentMode === "end");
		endButton.classList.toggle("secondary", currentMode !== "end");
	}

	const canCompute = Boolean(startMarker && endMarker);
	if (computeButton) {
		computeButton.disabled = !canCompute;
	}
}

function setMode(mode) {
	currentMode = mode;
	syncButtonStates();
	updateStatus(mode === "start" ? "Haz clic para establecer el origen." : "Haz clic para establecer el destino.");
}

function placeMarker(latlng) {
	const markerOptions = {
		draggable: true,
		opacity: 0.85
	};

	if (currentMode === "start") {
		if (startMarker) {
			startMarker.setLatLng(latlng);
		} else {
			startMarker = L.marker(latlng, markerOptions).addTo(map);
			startMarker.on("dragend", () => updateSummaries());
		}
	} else {
		if (endMarker) {
			endMarker.setLatLng(latlng);
		} else {
			endMarker = L.marker(latlng, markerOptions).addTo(map);
			endMarker.on("dragend", () => updateSummaries());
		}
	}

	if (currentMode === "start") {
		setMode("end");
	} else {
		updateStatus("Listo para calcular la ruta.");
	}

	updateSummaries();
}

function updateSummaries() {
	const startSummary = document.getElementById(START_SUMMARY_ID);
	const endSummary = document.getElementById(END_SUMMARY_ID);

	if (startSummary) {
		startSummary.textContent = startMarker ? formatLatLng(startMarker.getLatLng()) : "Sin origen";
	}

	if (endSummary) {
		endSummary.textContent = endMarker ? formatLatLng(endMarker.getLatLng()) : "Sin destino";
	}

	syncButtonStates();
}

async function computeRoute() {
	if (!startMarker || !endMarker) {
		return;
	}

	updateStatus("Calculando ruta…");

	const start = startMarker.getLatLng();
	const end = endMarker.getLatLng();

	const payload = {
		start: { lat: start.lat, lon: start.lng },
		end: { lat: end.lat, lon: end.lng }
	};

	try {
		const response = await fetch(ROUTE_ENDPOINT, {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify(payload)
		});

		if (!response.ok) {
			const errorBody = await response.json().catch(() => ({}));
			throw new Error(errorBody.message || "No se pudo calcular la ruta.");
		}

		const data = await response.json();
		drawRoute(data.route);
		updateStatus("Ruta calculada correctamente.");
	} catch (error) {
		console.error("Error calculando la ruta:", error);
		updateStatus(error.message || "No se pudo calcular la ruta.", true);
	}
}

function drawRoute(feature) {
	if (!map || !feature || !feature.geometry || !Array.isArray(feature.geometry.coordinates)) {
		updateStatus("Respuesta inválida del servicio.", true);
		return;
	}

	const latLngs = feature.geometry.coordinates.map(([lon, lat]) => [lat, lon]);

	if (routeLayer) {
		routeLayer.remove();
	}

	routeLayer = L.polyline(latLngs, {
		color: "#38bdf8",
		weight: 5,
		opacity: 0.85
	}).addTo(map);

	map.fitBounds(routeLayer.getBounds(), { padding: [40, 40] });
}

function clearRoute() {
	if (routeLayer) {
		routeLayer.remove();
		routeLayer = null;
	}

	if (startMarker) {
		startMarker.remove();
		startMarker = null;
	}

	if (endMarker) {
		endMarker.remove();
		endMarker = null;
	}

	setMode("start");
	updateSummaries();
	updateStatus("Selecciona un origen para comenzar.");
}

function attachUiHandlers() {
	const startButton = document.getElementById(START_BUTTON_ID);
	const endButton = document.getElementById(END_BUTTON_ID);
	const computeButton = document.getElementById(COMPUTE_BUTTON_ID);
	const clearButton = document.getElementById(CLEAR_BUTTON_ID);

	if (startButton) {
		startButton.addEventListener("click", () => setMode("start"));
	}

	if (endButton) {
		endButton.addEventListener("click", () => setMode("end"));
	}

	if (computeButton) {
		computeButton.addEventListener("click", computeRoute);
	}

	if (clearButton) {
		clearButton.addEventListener("click", clearRoute);
	}
}

function initMap() {
	const mapElement = document.getElementById(MAP_CONTAINER_ID);
	if (!mapElement) {
		console.error(`Contenedor Leaflet "${MAP_CONTAINER_ID}" no encontrado.`);
		return;
	}

	map = L.map(mapElement, {
		minZoom: 3,
		maxZoom: 18,
		zoomControl: true
	}).setView([-33.45, -70.66], 12);

	L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
		attribution: "&copy; <a href='https://www.openstreetmap.org/copyright'>OpenStreetMap</a> contributors",
		maxZoom: 19
	}).addTo(map);

	map.on("click", (event) => placeMarker(event.latlng));

	attachUiHandlers();
	syncButtonStates();
	updateStatus("Selecciona un origen para comenzar.");
}

document.addEventListener("DOMContentLoaded", initMap);


