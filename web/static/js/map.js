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
const STATION_TOGGLE_ID = "station-toggle";
const STATION_SECTION_ID = "station-section";
const VEHICLE_SECTION_ID = "vehicle-section";
const VEHICLE_BRAND_ID = "vehicle-brand";
const VEHICLE_MODEL_ID = "vehicle-model";
const VEHICLE_YEAR_ID = "vehicle-year";
const VEHICLE_FUEL_TYPE_ID = "vehicle-fuel-type";
const CONSUMPTION_CITY_ID = "consumption-city";
const CONSUMPTION_HIGHWAY_ID = "consumption-highway";
const CONSUMPTION_MIXED_ID = "consumption-mixed";
const TANK_CAPACITY_ID = "tank-capacity";
const TANK_LEVEL_ID = "tank-level";
const TANK_PERCENTAGE_ID = "tank-percentage";
const TANK_LITERS_ID = "tank-liters";
const MODE_SELECT_ID = "mode-select";

const ROUTE_MODES = {
	shortest: "shortest",
	cheapest: "cheapest"
};

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

const ENDPOINT_MARKER_ICON = L.divIcon({
	html: '<div class="endpoint-marker-dot"></div>',
	className: "endpoint-marker",
	iconSize: [24, 24],
	iconAnchor: [12, 12]
});

const HIGHLIGHT_STATION_ICON = L.divIcon({
	html: '<div class="highlight-station-marker-dot"></div>',
	className: "highlight-station-marker",
	iconSize: [34, 34],
	iconAnchor: [17, 34],
	popupAnchor: [0, -32]
});

const brandIconCache = new Map();

let map;
let startMarker = null;
let endMarker = null;
let routeLayer = null;
let currentMode = "start";
let stationMarkersLayer = null;
let highlightedStationLayer = null;
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

function formatCurrencyCLP(value) {
	if (value === null || value === undefined || Number.isNaN(value)) {
		return "—";
	}
	return Number(value).toLocaleString("es-CL", {
		style: "currency",
		currency: "CLP",
		maximumFractionDigits: 0
	});
}

function ensureStationLayer() {
	if (!map) {
		return null;
	}
	if (!stationMarkersLayer) {
		stationMarkersLayer = L.layerGroup().addTo(map);
	}
	return stationMarkersLayer;
}

function ensureHighlightedLayer() {
	if (!map) {
		return null;
	}
	if (!highlightedStationLayer) {
		highlightedStationLayer = L.layerGroup().addTo(map);
	}
	return highlightedStationLayer;
}

function clearStationMarkers() {
	if (stationMarkersLayer) {
		stationMarkersLayer.clearLayers();
	}
}

function clearHighlightedStation() {
	if (highlightedStationLayer) {
		highlightedStationLayer.clearLayers();
	}
}

function clearAllStationLayers() {
	clearStationMarkers();
	clearHighlightedStation();
}

function isStationToggleEnabled() {
	const toggle = document.getElementById(STATION_TOGGLE_ID);
	return toggle ? toggle.checked : true;
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

	const layer = ensureStationLayer();
	if (!layer) {
		return;
	}

	layer.clearLayers();

	if (!isStationToggleEnabled()) {
		return;
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
		layer.addLayer(marker);
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
	renderStationList(filteredStations);
}

function getSelectedMode() {
	const select = document.getElementById(MODE_SELECT_ID);
	return select ? select.value : ROUTE_MODES.shortest;
}

function updateModeUI() {
	const mode = getSelectedMode();
	const stationSection = document.getElementById(STATION_SECTION_ID);
	const vehicleSection = document.getElementById(VEHICLE_SECTION_ID);

	const shouldShowExtras = mode === ROUTE_MODES.cheapest;
	if (stationSection) {
		stationSection.classList.toggle("hidden", !shouldShowExtras);
	}
	if (vehicleSection) {
		vehicleSection.classList.toggle("hidden", !shouldShowExtras);
	}
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

function handleStationToggleChange() {
	updateStationDisplays();
}

function setupStationDataHandlers() {
	const fuelFilter = document.getElementById(FUEL_FILTER_ID);
	if (fuelFilter) {
		fuelFilter.addEventListener("change", handleFuelFilterChange);
	}

	const stationToggle = document.getElementById(STATION_TOGGLE_ID);
	if (stationToggle) {
		stationToggle.addEventListener("change", handleStationToggleChange);
	}

	if (map) {
		map.on("moveend", fetchStations);
		fetchStations();
	}
}

function updateTankDisplay() {
	const tankLevel = document.getElementById(TANK_LEVEL_ID);
	const tankCapacity = document.getElementById(TANK_CAPACITY_ID);
	const tankPercentage = document.getElementById(TANK_PERCENTAGE_ID);
	const tankLiters = document.getElementById(TANK_LITERS_ID);
	
	if (!tankLevel || !tankPercentage || !tankLiters) return;
	
	const percentage = parseInt(tankLevel.value);
	tankPercentage.textContent = `${percentage}%`;
	
	if (tankCapacity && tankCapacity.value) {
		const capacity = parseFloat(tankCapacity.value);
		const liters = (capacity * percentage / 100).toFixed(1);
		tankLiters.textContent = `(${liters} L)`;
	} else {
		tankLiters.textContent = "";
	}
}

function highlightCheapestStations(stations) {
	clearHighlightedStation();
	if (!map || !Array.isArray(stations) || !stations.length) {
		return;
	}
	const layer = ensureHighlightedLayer();
	if (!layer) {
		return;
	}

	let firstMarker = null;

	stations.forEach((station) => {
		if (!station) {
			return;
		}
		const latNum = typeof station.lat === "number" ? station.lat : parseFloat(station.lat);
		const lngNum = typeof station.lng === "number" ? station.lng : parseFloat(station.lng);
		if (Number.isNaN(latNum) || Number.isNaN(lngNum)) {
			return;
		}

		const marker = L.marker([latNum, lngNum], {
			icon: HIGHLIGHT_STATION_ICON,
			title: station.marca || "Estación seleccionada",
			riseOnHover: true
		});

		const priceText = station.precio ? `<strong>Precio</strong>: ${formatPrice(station.precio)}` : "";
		const litersText = station.liters_planned ? `<br><small>Recargar: ${station.liters_planned} L</small>` : "";
		const popup = `
			<div>
				<strong>${station.marca || "Estación"}</strong><br>
				${station.direccion || "Sin dirección"}
				${priceText ? `<br>${priceText}` : ""}
				${litersText}
			</div>
		`;
		marker.bindPopup(popup);
		layer.addLayer(marker);
		if (!firstMarker) {
			firstMarker = marker;
		}
	});

	if (firstMarker && typeof firstMarker.openPopup === "function") {
		firstMarker.openPopup();
	}
}

function collectVehicleData() {
	const brand = document.getElementById(VEHICLE_BRAND_ID)?.value || null;
	const model = document.getElementById(VEHICLE_MODEL_ID)?.value || null;
	const yearValue = document.getElementById(VEHICLE_YEAR_ID)?.value;
	const fuelType = document.getElementById(VEHICLE_FUEL_TYPE_ID)?.value || null;
	const cityValue = document.getElementById(CONSUMPTION_CITY_ID)?.value;
	const highwayValue = document.getElementById(CONSUMPTION_HIGHWAY_ID)?.value;
	const mixedValue = document.getElementById(CONSUMPTION_MIXED_ID)?.value;
	const tankCapacityValue = document.getElementById(TANK_CAPACITY_ID)?.value;
	const tankLevelValue = document.getElementById(TANK_LEVEL_ID)?.value;

	return {
		marca: brand,
		modelo: model,
		anio: yearValue ? parseInt(yearValue) : null,
		fuel_type: fuelType,
		consumption: {
			city_km_l: cityValue ? parseFloat(cityValue) : null,
			highway_km_l: highwayValue ? parseFloat(highwayValue) : null,
			mixed_km_l: mixedValue ? parseFloat(mixedValue) : null
		},
		tank: {
			capacity_l: tankCapacityValue ? parseFloat(tankCapacityValue) : null,
			level_percent: tankLevelValue ? parseInt(tankLevelValue) : 50
		}
	};
}

function setVehicleData(data) {
	if (data.marca) document.getElementById(VEHICLE_BRAND_ID).value = data.marca;
	if (data.modelo) document.getElementById(VEHICLE_MODEL_ID).value = data.modelo;
	if (data.anio) document.getElementById(VEHICLE_YEAR_ID).value = data.anio;
	if (data.tipo_combustible) document.getElementById(VEHICLE_FUEL_TYPE_ID).value = data.tipo_combustible;
	
	if (data.rendimiento) {
		if (data.rendimiento.ciudad) document.getElementById(CONSUMPTION_CITY_ID).value = data.rendimiento.ciudad;
		if (data.rendimiento.carretera) document.getElementById(CONSUMPTION_HIGHWAY_ID).value = data.rendimiento.carretera;
		if (data.rendimiento.mixto) document.getElementById(CONSUMPTION_MIXED_ID).value = data.rendimiento.mixto;
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
		submitButton.textContent = "Buscando…";
	}

	try {
		const response = await fetch(`${MATRICULA_ENDPOINT}/${encodeURIComponent(patente)}`);
		if (!response.ok) {
			const body = await response.json().catch(() => ({}));
			const message = body.message || body.description || "No se encontraron datos para la matrícula.";
			throw new Error(message);
		}

		const data = await response.json();
		// Populate the form fields with the fetched data
		setVehicleData(data);
		// Clear any error message
		setMatriculaError("");
	} catch (error) {
		setMatriculaError(error.message || "No se pudo consultar la matrícula.");
	}

	if (submitButton) {
		submitButton.disabled = false;
		submitButton.textContent = "Buscar";
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


function setupVehicleForm() {
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

	// Setup tank level slider
	const tankSlider = document.getElementById(TANK_LEVEL_ID);
	const tankCapacity = document.getElementById(TANK_CAPACITY_ID);
	
	if (tankSlider) {
		tankSlider.addEventListener("input", updateTankDisplay);
	}
	
	if (tankCapacity) {
		tankCapacity.addEventListener("input", updateTankDisplay);
	}
	
	// Initialize tank display
	updateTankDisplay();
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
		opacity: 0.95,
		icon: ENDPOINT_MARKER_ICON
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

	const mode = getSelectedMode();
	clearHighlightedStation();
	const payload = {
		start: { lat: start.lat, lon: start.lng },
		end: { lat: end.lat, lon: end.lng },
		mode
	};

	if (mode === ROUTE_MODES.cheapest) {
		payload.vehicle = collectVehicleData();
	}

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
		if (mode === ROUTE_MODES.cheapest && data.cost && typeof data.cost.estimated_cost_clp === "number") {
			const formatted = formatCurrencyCLP(data.cost.estimated_cost_clp);
			const stations = Array.isArray(data.cost.stations) ? data.cost.stations : [];
			if (stations.length) {
				const firstStation = stations[0];
				const stationLabel = ` usando ${firstStation.marca || "estación"} (${firstStation.codigo || "s/n"})`;
				updateStatus(`Ruta más barata estimada: ${formatted}${stationLabel}.`);
			} else {
				updateStatus(`Ruta más barata estimada: ${formatted}.`);
			}
			highlightCheapestStations(stations);
		} else {
			updateStatus("Ruta calculada correctamente.");
			clearHighlightedStation();
		}
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
	clearHighlightedStation();
}

function attachUiHandlers() {
	const startButton = document.getElementById(START_BUTTON_ID);
	const endButton = document.getElementById(END_BUTTON_ID);
	const computeButton = document.getElementById(COMPUTE_BUTTON_ID);
	const clearButton = document.getElementById(CLEAR_BUTTON_ID);
	const modeSelect = document.getElementById(MODE_SELECT_ID);

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

	if (modeSelect) {
		modeSelect.addEventListener("change", () => {
			updateModeUI();
		});
		updateModeUI();
	}

	setupStationDataHandlers();
	setupVehicleForm();
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


