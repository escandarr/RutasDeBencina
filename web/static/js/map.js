


const MAP_CONTAINER_ID = "map";
const STATUS_ELEMENT_ID = "map-status";
const INFRA_ENDPOINT = "/infraestructura/data";
const FETCH_DEBOUNCE_MS = 350;
const PAGE_SIZE = 7500;
const BBOX_DECIMALS = 5;

let infraestructuraLayer;
let debounceHandle = null;
let activeRequestId = 0;
let lastRequestedBBox = null;

function updateStatus(message, isError = false) {
	const statusEl = document.getElementById(STATUS_ELEMENT_ID);
	if (!statusEl) {
		return;
	}

	statusEl.textContent = message;
	statusEl.style.color = isError ? "#fca5a5" : "#cbd5f5";
}

function buildBBoxParam(bounds) {
	if (!bounds) {
		return null;
	}

	const toPrecision = (value) => Number.parseFloat(value).toFixed(BBOX_DECIMALS);

	const south = toPrecision(bounds.getSouth());
	const west = toPrecision(bounds.getWest());
	const north = toPrecision(bounds.getNorth());
	const east = toPrecision(bounds.getEast());

	return `${south},${west},${north},${east}`;
}

async function fetchViewportData(map) {
	if (!map) {
		return;
	}

	const bbox = buildBBoxParam(map.getBounds());
	if (!bbox) {
		return;
	}

	if (bbox === lastRequestedBBox) {
		return;
	}

	lastRequestedBBox = bbox;
	const requestId = ++activeRequestId;

	updateStatus("Cargando infraestructura…");
	infraestructuraLayer.clearLayers();

	let page = 1;
	let totalLoaded = 0;

	try {
		while (true) {
			const url = new URL(INFRA_ENDPOINT, window.location.origin);
			url.searchParams.set("bbox", bbox);
			url.searchParams.set("page", page);
			url.searchParams.set("page_size", PAGE_SIZE);

			const response = await fetch(url.toString(), { cache: "no-store" });
			if (requestId !== activeRequestId) {
				return; // A newer request started.
			}

			if (!response.ok) {
				throw new Error(`HTTP ${response.status}`);
			}

			const payload = await response.json();
			if (requestId !== activeRequestId) {
				return;
			}

			const features = Array.isArray(payload.features) ? payload.features : [];
			if (features.length > 0) {
				infraestructuraLayer.addData(features);
				totalLoaded += features.length;
			}

			if (!payload.has_more) {
				break;
			}

			page += 1;
		}

		if (totalLoaded === 0) {
			updateStatus("No hay infraestructura para esta vista.");
		} else {
			updateStatus(`Infraestructura cargada (${totalLoaded} tramos).`);
		}
	} catch (error) {
		if (requestId === activeRequestId) {
			infraestructuraLayer.clearLayers();
			updateStatus("No pudimos cargar la infraestructura.", true);
			console.error("Error cargando infraestructura:", error);
			lastRequestedBBox = null;
		}
	}
}

function scheduleViewportFetch(map) {
	if (debounceHandle) {
		clearTimeout(debounceHandle);
	}

	debounceHandle = setTimeout(() => {
		fetchViewportData(map);
	}, FETCH_DEBOUNCE_MS);
}

function initMap() {
	const mapElement = document.getElementById(MAP_CONTAINER_ID);
	if (!mapElement) {
		console.error(`Contenedor Leaflet "${MAP_CONTAINER_ID}" no encontrado.`);
		return;
	}

	const map = L.map(mapElement, {
		minZoom: 3,
		maxZoom: 18,
		zoomControl: true,
		preferCanvas: true
	}).setView([-33.45, -70.66], 12);

	L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
		attribution: "&copy; <a href='https://www.openstreetmap.org/copyright'>OpenStreetMap</a> contributors",
		maxZoom: 19,
	}).addTo(map);

	infraestructuraLayer = L.geoJSON(null, {
		style: {
			color: "#38bdf8",
			weight: 2,
			opacity: 0.7,
		},
		onEachFeature: (feature, layer) => {
			if (!feature || !feature.properties) {
				return;
			}

			const entries = Object.entries(feature.properties)
				.filter(([_, value]) => value !== null && value !== "")
				.slice(0, 8);

			if (entries.length > 0) {
				const content = entries
					.map(([key, value]) => `<strong>${key}</strong>: ${value}`)
					.join("<br>");
				layer.bindPopup(content);
			}
		},
	}).addTo(map);
	map.whenReady(() => {
		scheduleViewportFetch(map);
	});
}

document.addEventListener("DOMContentLoaded", initMap);


