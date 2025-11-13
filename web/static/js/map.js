const MAP_CONTAINER_ID = "map";
const STATUS_ELEMENT_ID = "status";
const ROUTE_ENDPOINT = "/api/routes/shortest";
const STATIONS_ENDPOINT = "/api/stations";
const MATRICULA_ENDPOINT = "/api/matriculas";
const START_BUTTON_ID = "select-start";
const END_BUTTON_ID = "select-end";
const COMPUTE_BUTTON_ID = "compute-route";
const CLEAR_BUTTON_ID = "clear-route";
const START_SUMMARY_ID = "start-summary";
const END_SUMMARY_ID = "end-summary";
const STATION_LIST_ID = "station-list";
const FUEL_FILTER_ID = "fuel-filter";
const MATRICULA_FORM_ID = "matricula-form";
const MATRICULA_INPUT_ID = "matricula-input";
const MATRICULA_SUBMIT_ID = "matricula-submit";
const MATRICULA_ERROR_ID = "matricula-error";
const MATRICULA_RESULT_ID = "matricula-result";

const STATION_PRECISION_DIGITS = 5;

const DEFAULT_MARKER_ICON = L.icon({
	iconUrl: "/static/js/leaflet/dist/images/marker-icon.png",
	shadowUrl: "/static/js/leaflet/dist/images/marker-shadow.png",
	iconSize: [25, 41],
	iconAnchor: [12, 41],
	shadowSize: [41, 41],
	shadowAnchor: [12, 41],
	popupAnchor: [1, -34]
});

const brandIconCache = new Map();

let map;
let startMarker = null;
let endMarker = null;
let routeLayer = null;
let currentMode = "start";
let stationMarkersLayer = null;
let stationDataCache = [];
let stationFetchAbortController = null;

function getFuelFilterValue() {
	const select = document.getElementById(FUEL_FILTER_ID);
	return select ? select.value : "";
}

function formatPrice(value) {
	if (value === null || value === undefined) {
		return "—";
	}
	return Number(value).toLocaleString("es-CL", {
		style: "currency",
		currency: "CLP",
		maximumFractionDigits: 0
	});
}

function clearStationMarkers() {
	if (stationMarkersLayer) {
		stationMarkersLayer.clearLayers();
	}
}

function escapeAttribute(value = "") {
	return value
		.replace(/&/g, "&amp;")
		.replace(/"/g, "&quot;")
		.replace(/</g, "&lt;")
		.replace(/>/g, "&gt;");
}

function createLogoMarkerIcon(logoUrl, brandName = "") {
	const safeBrand = brandName ? escapeAttribute(brandName) : "";
	const titleAttr = safeBrand ? ` title="${safeBrand}"` : "";
	const altAttr = safeBrand ? ` alt="${safeBrand}"` : ` alt=""`;

	return L.divIcon({
		html: `<div class="station-marker-logo"${titleAttr}><img src="${logoUrl}"${altAttr} loading="lazy"></div>`,
		className: "station-marker-icon",
		iconSize: [50, 58],
		iconAnchor: [25, 56],
		popupAnchor: [0, -52]
	});
}

function getMarkerIconForStation(station) {
	const logoUrl = station.logo_url;
	if (logoUrl) {
		if (!brandIconCache.has(logoUrl)) {
			brandIconCache.set(logoUrl, createLogoMarkerIcon(logoUrl, station.marca));
		}
		return brandIconCache.get(logoUrl);
	}

	return DEFAULT_MARKER_ICON;
}

function renderStationMarkers(stations) {
	if (!map) {
		return;
	}

	if (!stationMarkersLayer) {
		stationMarkersLayer = L.layerGroup().addTo(map);
	} else {
		clearStationMarkers();
	}

	stations.forEach((station) => {
		if (typeof station.lat !== "number" || typeof station.lng !== "number") {
			return;
		}

		const marker = L.marker([station.lat, station.lng], {
			icon: getMarkerIconForStation(station),
			opacity: station.logo_url ? 1 : 0.85,
			riseOnHover: true,
			title: station.marca || "Estación"
		});
		const fuelType = getFuelFilterValue();
		let priceText = "";
		if (fuelType) {
			const price = station.precios ? station.precios[fuelType] : undefined;
			priceText = price ? `<strong>${fuelType}</strong>: ${formatPrice(price)}` : "Sin precio registrado";
		} else if (station.precios) {
			const entries = Object.entries(station.precios)
				.filter((entry) => entry[1] !== null && entry[1] !== undefined)
				.sort((a, b) => a[0].localeCompare(b[0]));
			priceText = entries
				.map(([key, value]) => `<strong>${key}</strong>: ${formatPrice(value)}`)
				.join("<br>");
		}

		const popup = `
			<div>
				<strong>${station.marca || "Estación"}</strong><br>
				${station.direccion || "Sin dirección"}<br>
				${priceText}
				${station.lat && station.lng ? `<br><small>${station.lat.toFixed(STATION_PRECISION_DIGITS)}, ${station.lng.toFixed(STATION_PRECISION_DIGITS)}</small>` : ""}
			</div>
		`;

		marker.bindPopup(popup);
		stationMarkersLayer.addLayer(marker);
	});
}

function renderStationList(stations) {
	const list = document.getElementById(STATION_LIST_ID);
	if (!list) {
		return;
	}

	if (!stations.length) {
		list.innerHTML = `<p class="empty-state">No hay estaciones con datos disponibles en el área.</p>`;
		return;
	}

	const selectedFuel = getFuelFilterValue();
	const items = stations.map((station) => {
		const priceTags = station.precios
			? Object.entries(station.precios)
				.filter((entry) => entry[1] !== null && entry[1] !== undefined)
				.map(([fuel, price]) => {
					const highlightClass = selectedFuel && fuel === selectedFuel ? " price-tag--active" : "";
					return `<span class="price-tag${highlightClass}"><strong>${fuel}</strong> ${formatPrice(price)}</span>`;
				})
				.join("")
			: "";

		return `
			<article class="station-card" role="listitem">
				<div class="station-header">
					<span>${station.marca || "Estación"}</span>
					${station.precios && selectedFuel && station.precios[selectedFuel] ? `<span>${formatPrice(station.precios[selectedFuel])}</span>` : ""}
				</div>
				<div class="station-meta">${station.direccion || "Sin dirección"}${station.comuna ? `, ${station.comuna}` : ""}</div>
				<div class="price-tags">${priceTags || `<span class="empty-state">Sin precios registrados</span>`}</div>
			</article>
		`;
	}).join("");

	list.innerHTML = items;
}

function filterStationData() {
	const fuelType = getFuelFilterValue();
	if (!fuelType) {
		return stationDataCache.slice();
	}

	return stationDataCache.filter((station) => {
		const stationPrices = station.precios || {};
		return stationPrices[fuelType] !== undefined && stationPrices[fuelType] !== null;
	});
}

function updateStationDisplays() {
	const filteredStations = filterStationData();
	renderStationMarkers(filteredStations);
	renderStationList(filteredStations);
}

function handleFuelFilterChange() {
	updateStationDisplays();
}

async function fetchStations() {
	if (!map) {
		return;
	}

	if (stationFetchAbortController) {
		stationFetchAbortController.abort();
	}

	stationFetchAbortController = new AbortController();
	const { signal } = stationFetchAbortController;

	const bounds = map.getBounds();
	const params = new URLSearchParams({
		north: bounds.getNorth().toString(),
		south: bounds.getSouth().toString(),
		east: bounds.getEast().toString(),
		west: bounds.getWest().toString(),
		limit: "200"
	});

	const fuel = getFuelFilterValue();
	if (fuel) {
		params.set("fuel_type", fuel);
	}

	try {
		const response = await fetch(`${STATIONS_ENDPOINT}?${params.toString()}`, { signal });
		if (!response.ok) {
			throw new Error("No se pudo obtener la información de estaciones.");
		}
		const data = await response.json();
		stationDataCache = Array.isArray(data.stations) ? data.stations : [];
		updateStationDisplays();
	} catch (error) {
		if (error.name === "AbortError") {
			return;
		}
		console.error("Error al obtener estaciones:", error);
		const list = document.getElementById(STATION_LIST_ID);
		if (list) {
			list.innerHTML = `<p class="empty-state">Error al cargar estaciones.</p>`;
		}
	}
}

function setupStationDataHandlers() {
	const fuelFilter = document.getElementById(FUEL_FILTER_ID);
	if (fuelFilter) {
		fuelFilter.addEventListener("change", handleFuelFilterChange);
	}

	if (map) {
		map.on("moveend", fetchStations);
		fetchStations();
	}
}

function clearMatriculaResult() {
	const result = document.getElementById(MATRICULA_RESULT_ID);
	if (result) {
		result.innerHTML = "";
	}

	const error = document.getElementById(MATRICULA_ERROR_ID);
	if (error) {
		error.textContent = "";
	}
}

function renderMatriculaResult(data) {
	const result = document.getElementById(MATRICULA_RESULT_ID);
	if (!result) {
		return;
	}

	const rendimiento = data.rendimiento || {};
	const rows = [
		["Patente", data.patente],
		["Marca", data.marca],
		["Modelo", data.modelo],
		["Año", data.anio],
		["Combustible", data.tipo_combustible],
		["Rendimiento mixto", rendimiento.mixto ? `${rendimiento.mixto} km/L` : "—"],
		["Rendimiento ciudad", rendimiento.ciudad ? `${rendimiento.ciudad} km/L` : "—"],
		["Rendimiento carretera", rendimiento.carretera ? `${rendimiento.carretera} km/L` : "—"],
		["Fuente", data.fuente],
		["Actualizado", data.actualizado_en ? new Date(data.actualizado_en).toLocaleString("es-CL") : "—"],
	];

	result.innerHTML = rows
		.filter(([, value]) => value !== undefined && value !== null)
		.map(([label, value]) => `<div class="matricula-row"><strong>${label}:</strong> ${value}</div>`)
		.join("");
}

function setMatriculaError(message) {
	const error = document.getElementById(MATRICULA_ERROR_ID);
	if (error) {
		error.textContent = message;
	}
}

async function fetchMatriculaData(patente) {
	const submitButton = document.getElementById(MATRICULA_SUBMIT_ID);
	if (submitButton) {
		submitButton.disabled = true;
		submitButton.textContent = "Consultando…";
	}

	try {
		const response = await fetch(`${MATRICULA_ENDPOINT}/${encodeURIComponent(patente)}`);
		if (!response.ok) {
			const body = await response.json().catch(() => ({}));
			const message = body.message || body.description || "No se encontraron datos para la matrícula.";
			throw new Error(message);
		}

		const data = await response.json();
		renderMatriculaResult(data);
	} catch (error) {
		renderMatriculaResult({});
		setMatriculaError(error.message || "No se pudo consultar la matrícula.");
	}

	if (submitButton) {
		submitButton.disabled = false;
		submitButton.textContent = "Consultar";
	}
}

function handleMatriculaSubmit(event) {
	event.preventDefault();
	clearMatriculaResult();

	const input = document.getElementById(MATRICULA_INPUT_ID);
	if (!input) {
		return;
	}

	const value = input.value.trim().toUpperCase();
	if (!value) {
		setMatriculaError("Ingresa una matrícula válida.");
		return;
	}

	fetchMatriculaData(value);
}

function setupMatriculaForm() {
	const form = document.getElementById(MATRICULA_FORM_ID);
	if (!form) {
		return;
	}

	form.addEventListener("submit", handleMatriculaSubmit);
	const input = document.getElementById(MATRICULA_INPUT_ID);
	if (input) {
		input.addEventListener("input", () => {
			const normalized = input.value.toUpperCase().replace(/[^A-Z0-9]/g, "");
			input.value = normalized;
			clearMatriculaResult();
			setMatriculaError("");
		});
	}
}
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

	setupStationDataHandlers();
	setupMatriculaForm();
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


