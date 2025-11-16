"""Fetch recurring fuel promotions from descuentosrata.com and export JSON/CSV.

The API exposes a weekly schedule (`dia=1..7`). We iterate over each day,
filter for the "#RataBencinera" category, enrich the records with matching
fuel brand metadata, and store the results under Metadata/outputs/promos/.
"""
from __future__ import annotations

import csv
import json
import logging
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import requests

API_ENDPOINT = "https://descuentosrata.com/api/v1/recurrente/"
API_TIMEOUT = 30

OUTPUT_DIR = Path(__file__).resolve().parent.parent / "outputs" / "promos"
JSON_PATH = OUTPUT_DIR / "promos_rata.json"
CSV_PATH = OUTPUT_DIR / "promos_rata.csv"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36 RutasDeBencina/1.0"
    )
}

DAY_PARAMS = {
    1: "lunes",
    2: "martes",
    3: "miércoles",
    4: "jueves",
    5: "viernes",
    6: "sábado",
    7: "domingo",
}


@dataclass(frozen=True)
class BrandMatcher:
    brand_id: int
    canonical_name: str
    aliases: Iterable[str]


BRAND_MATCHERS: List[BrandMatcher] = [
    BrandMatcher(1, "Copec", ["copec", "copec app"]),
    BrandMatcher(2, "Shell", ["shell", "micopiloto"]),
    BrandMatcher(3, "Petrobras", ["petrobras", "aramco", "aramco/petrobras"]),
    BrandMatcher(4, "Aramco", ["aramco"]),
    BrandMatcher(5, "Enex", ["enex", "enex prime"]),
    BrandMatcher(6, "Terpel", ["terpel"]),
    BrandMatcher(7, "Pronto", ["pronto", "prontocopec"]),
    BrandMatcher(8, "Abastible", ["abastible"]),
    BrandMatcher(9, "Gasco", ["gasco"]),
    BrandMatcher(10, "Lipigas", ["lipigas", "lipiapp"]),
]

CATEGORY_SLUG = "bencina"
CATEGORY_NAME = "#RataBencinera"
LOGGER = logging.getLogger("extract_promos_rata")


def ensure_output_dir() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def normalize(text: Optional[str]) -> str:
    if not text:
        return ""
    return re.sub(r"[^a-z0-9]+", " ", text.lower()).strip()


def strip_html(html: Optional[str]) -> str:
    if not html:
        return ""
    text = re.sub(r"<\s*br\s*/?>", "\n", html, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def match_brand(tienda: Optional[dict]) -> Dict[str, Optional[str]]:
    name_candidates = []
    if not tienda:
        return {"brand_id": None, "brand_name": None}

    name = tienda.get("nombre")
    slug = tienda.get("slug")
    name_candidates.extend([name, slug])

    if tienda.get("etiquetas"):
        for tag in tienda["etiquetas"]:
            name_candidates.append(tag.get("nombre"))

    norm_candidates = [normalize(candidate) for candidate in name_candidates if candidate]

    for matcher in BRAND_MATCHERS:
        alias_norms = [normalize(alias) for alias in matcher.aliases]
        for candidate in norm_candidates:
            if not candidate:
                continue
            if any(candidate.startswith(alias) or alias in candidate for alias in alias_norms):
                return {"brand_id": matcher.brand_id, "brand_name": matcher.canonical_name}

    return {"brand_id": None, "brand_name": name}


def fetch_promos_for_day(day: int) -> List[dict]:
    params = {"dia": day, "categoria": CATEGORY_SLUG}
    response = requests.get(API_ENDPOINT, params=params, headers=HEADERS, timeout=API_TIMEOUT)
    response.raise_for_status()

    payload = response.json()
    results = payload.get("results", [])

    filtered = []
    for item in results:

        if "⛽️" not in item.get("nombre"):
            continue
        
        categorias = item.get("categorias") or []
        if any(cat.get("slug") == CATEGORY_SLUG and cat.get("nombre") == CATEGORY_NAME for cat in categorias):
            filtered.append(item)

    LOGGER.debug("Day %s (%s) -> %s promos", day, DAY_PARAMS.get(day, "?"), len(filtered))
    return filtered


def collect_promotions() -> List[dict]:
    collected: Dict[int, dict] = {}

    for day in range(1, 8):
        try:
            for promo in fetch_promos_for_day(day):
                collected[promo["id"]] = promo
        except requests.RequestException as exc:
            LOGGER.warning("Failed to fetch day %s: %s", day, exc)

    LOGGER.info("Collected %s unique promotions", len(collected))
    return list(collected.values())


def transform_promotion(raw: dict) -> dict:
    tienda = raw.get("tienda") or {}
    convenio = raw.get("convenio") or {}

    brand_info = match_brand(tienda)
    days = raw.get("dias") or []

    return {
        "id": raw.get("id"),
        "title": raw.get("nombre"),
        "slug": raw.get("slug"),
        "details_html": raw.get("detalle") or "",
        "details_text": strip_html(raw.get("detalle")),
        "discount_amount": raw.get("monto"),
        "valid_from": raw.get("vigencia_ini"),
        "valid_to": raw.get("vigencia_fin"),
        "is_active": raw.get("activo"),
        "source_url": raw.get("url_rateada") or "",
        "days": days,
        "partner": {
            "id": convenio.get("id"),
            "name": convenio.get("nombre"),
            "slug": convenio.get("slug"),
            "type": convenio.get("tipo"),
        },
        "brand": {
            "brand_id": brand_info.get("brand_id"),
            "brand_name": brand_info.get("brand_name"),
            "store_id": tienda.get("id"),
            "store_name": tienda.get("nombre"),
            "store_slug": tienda.get("slug"),
        },
    }


def export_json(records: List[dict]) -> None:
    ensure_output_dir()
    with JSON_PATH.open("w", encoding="utf-8") as fp:
        json.dump(records, fp, ensure_ascii=False, indent=2)
    LOGGER.info("JSON exported to %s", JSON_PATH)


def export_csv(records: List[dict]) -> None:
    ensure_output_dir()
    fieldnames = [
        "id",
        "partner_name",
        "brand_id",
        "brand_name",
        "title",
        "details_text",
        "discount_amount",
        "vigencia_days",
        "source_url",
        "valid_from",
        "valid_to",
    ]

    with CSV_PATH.open("w", encoding="utf-8", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames)
        writer.writeheader()
        for record in records:
            writer.writerow(
                {
                    "id": record.get("id"),
                    "partner_name": record.get("partner", {}).get("name"),
                    "brand_id": record.get("brand", {}).get("brand_id"),
                    "brand_name": record.get("brand", {}).get("brand_name"),
                    "title": record.get("title"),
                    "details_text": record.get("details_text"),
                    "discount_amount": record.get("discount_amount"),
                    "vigencia_days": ", ".join(record.get("days") or []),
                    "source_url": record.get("source_url"),
                    "valid_from": record.get("valid_from"),
                    "valid_to": record.get("valid_to"),
                }
            )
    LOGGER.info("CSV exported to %s", CSV_PATH)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    try:
        raw_promos = collect_promotions()
        transformed = [transform_promotion(promo) for promo in raw_promos]
        transformed.sort(key=lambda item: item.get("id") or 0)
        export_json(transformed)
        export_csv(transformed)
    except Exception as exc:  # pragma: no cover - defensive
        LOGGER.exception("Extraction failed: %s", exc)
        return 1

    LOGGER.info("✅ Completed extraction of %s promotions", len(transformed))
    return 0


if __name__ == "__main__":
    sys.exit(main())
