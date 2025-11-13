const STATUS_CONTAINER_ID = "status-area";

const REQUEST_TIMEOUT_MS = 15 * 60 * 1000; // 15 minutes

function qs(selector, context = document) {
	return context.querySelector(selector);
}

function qsa(selector, context = document) {
	return Array.from(context.querySelectorAll(selector));
}

function setBusyState(container, isBusy) {
	const button = qs('button[data-action="run"]', container);
	if (button) {
		button.disabled = isBusy;
		button.textContent = isBusy ? "Ejecutando…" : "Ejecutar";
	}
}

function readOptions(container) {
	const options = {};
	qsa('input[type="checkbox"]', container).forEach((checkbox) => {
		const key = checkbox.dataset.option;
		if (!key) {
			return;
		}
		options[key] = checkbox.checked;
	});
	return options;
}

function appendStatusEntry(dataset, response) {
	const statusArea = document.getElementById(STATUS_CONTAINER_ID);
	if (!statusArea) {
		return;
	}

	const successful = Boolean(response && response.success);
	const card = document.createElement("div");
	card.className = "status-entry";
	card.dataset.success = String(successful);

	const header = document.createElement("header");
	const title = document.createElement("span");
	title.textContent = `Dataset: ${dataset} · ${successful ? "OK" : "Error"}`;
	header.appendChild(title);

	const meta = document.createElement("span");
	meta.className = "chip";
	meta.textContent = new Date().toLocaleTimeString("es-CL");
	header.appendChild(meta);

	card.appendChild(header);

	if (Array.isArray(response?.steps)) {
		response.steps.forEach((step) => {
			const stepWrapper = document.createElement("div");
			stepWrapper.className = "step-entry";

			const label = document.createElement("div");
			label.innerHTML = `<strong>${step.label || step.command?.[1] || "Paso"}</strong> · ${
				step.success ? "OK" : "Error"
			} · ${typeof step.duration_seconds === "number" ? `${step.duration_seconds}s` : ""}`;
			stepWrapper.appendChild(label);

			if (step.stdout) {
				const stdout = document.createElement("pre");
				stdout.textContent = `STDOUT:\n${step.stdout}`;
				stepWrapper.appendChild(stdout);
			}

			if (step.stderr) {
				const stderr = document.createElement("pre");
				stderr.textContent = `STDERR:\n${step.stderr}`;
				stepWrapper.appendChild(stderr);
			}

			card.appendChild(stepWrapper);
		});
	}

	if (!Array.isArray(response?.steps) || response.steps.length === 0) {
		const empty = document.createElement("pre");
		empty.textContent = "No se recibieron logs.";
		card.appendChild(empty);
	}

	statusArea.prepend(card);
}

async function postRefreshRequest(dataset, options) {
	const controller = new AbortController();
	const timeout = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
	try {
		const response = await fetch("/api/admin/refresh", {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify({ dataset, ...options }),
			signal: controller.signal
		});

		const payload = await response.json().catch(() => ({}));

		return {
			ok: response.ok,
			status: response.status,
			body: payload
		};
	} catch (error) {
		return {
			ok: false,
			status: error.name === "AbortError" ? 408 : 500,
			body: {
				success: false,
				steps: [
					{
						label: "request",
						success: false,
						stdout: "",
						stderr: error.message || "Error inesperado"
					}
				]
			}
		};
	} finally {
		clearTimeout(timeout);
	}
}

function handleRunClick(event) {
	const button = event.currentTarget;
	const container = button.closest(".card");
	if (!container) {
		return;
	}

	const dataset = container.dataset.target;
	if (!dataset) {
		return;
	}

	const options = readOptions(container);
	setBusyState(container, true);

	postRefreshRequest(dataset, options)
		.then((resp) => {
			appendStatusEntry(dataset, resp.body);
			if (!resp.ok) {
				console.error("Error al ejecutar tarea:", resp.status, resp.body);
				alert(
					`La ejecución terminó con errores (HTTP ${resp.status}). Revisa los detalles en el historial.`
				);
			}
		})
		.catch((error) => {
			console.error("Error inesperado:", error);
			alert("No se pudo completar la ejecución. Revisa la consola para más detalles.");
		})
		.finally(() => {
			setBusyState(container, false);
		});
}

function bootstrap() {
	qsa('.card button[data-action="run"]').forEach((button) => {
		button.addEventListener("click", handleRunClick);
	});
}

document.addEventListener("DOMContentLoaded", bootstrap);


