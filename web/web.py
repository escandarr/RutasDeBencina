import json
from pathlib import Path
from typing import Iterable, List, NamedTuple, Optional, Sequence, Tuple

from flask import Flask, abort, jsonify, render_template, request

# Create an instance of the Flask application
app = Flask(__name__)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
INFRA_FILE = PROJECT_ROOT / "Infraestructura" / "infra.geojson"


BoundingBox = Tuple[float, float, float, float]
Point = Tuple[float, float]


class InfraDataset(NamedTuple):
    features: List[dict]
    bounds: List[Optional[BoundingBox]]


def _iter_points(coords: Sequence) -> Iterable[Point]:
    """Yield individual (lon, lat) points from arbitrary GeoJSON coordinate structure."""
    if not coords:
        return

    first = coords[0]
    if isinstance(first, (int, float)) and len(coords) >= 2:
        # Base case: a single coordinate pair [lon, lat, *optional]
        yield coords[0], coords[1]
        return

    for child in coords:
        if isinstance(child, (list, tuple)):
            yield from _iter_points(child)


def _compute_feature_bounds(feature: dict) -> Optional[BoundingBox]:
    geometry = feature.get('geometry') or {}
    coords = geometry.get('coordinates')
    if coords is None:
        return None

    lats: List[float] = []
    lons: List[float] = []
    for lon, lat in _iter_points(coords):
        lats.append(lat)
        lons.append(lon)

    if not lats or not lons:
        return None

    return min(lats), min(lons), max(lats), max(lons)


def _load_dataset_from_disk() -> InfraDataset:
    """Load infraestructura features from disk and pre-compute bounding boxes."""
    if not INFRA_FILE.exists():
        raise FileNotFoundError('Infraestructura data not found.')

    with INFRA_FILE.open('r', encoding='utf-8') as fh:
        data = json.load(fh)

    features = data.get('features')
    if not isinstance(features, list):
        raise ValueError('Infraestructura GeoJSON does not contain a feature list.')

    bounds = [_compute_feature_bounds(feature) for feature in features]
    return InfraDataset(features=features, bounds=bounds)


try:
    INFRA_DATASET: Optional[InfraDataset] = _load_dataset_from_disk()
    INFRA_DATASET_ERROR: Optional[Exception] = None
except (FileNotFoundError, ValueError) as exc:  # pragma: no cover - startup failure fallback
    INFRA_DATASET = None
    INFRA_DATASET_ERROR = exc


def _bbox_intersects(a: BoundingBox, b: BoundingBox) -> bool:
    south_a, west_a, north_a, east_a = a
    south_b, west_b, north_b, east_b = b
    return not (
        east_a < west_b or east_b < west_a or north_a < south_b or north_b < south_a
    )


def _parse_bbox_param(raw_bbox: Optional[str]) -> BoundingBox:
    if not raw_bbox:
        raise ValueError('Missing "bbox" query parameter.')

    parts = [p.strip() for p in raw_bbox.split(',') if p.strip()]
    if len(parts) != 4:
        raise ValueError('The "bbox" parameter must contain four comma-separated numbers.')

    south, west, north, east = map(float, parts)
    if south > north:
        south, north = north, south
    if west > east:
        west, east = east, west

    return south, west, north, east

# Define a route for the homepage
@app.route('/')
def home():
    """Render the Leaflet map view."""
    return render_template('map.html')

# Define another route for a different page
@app.route('/about')
def about():
    """This function runs when someone visits the /about page."""
    return '<h2>This is the About Page!</h2>'


@app.route('/infraestructura/data')
def infraestructura_data():
    """Serve infraestructura data filtered by bounding box and paginated."""
    try:
        bbox = _parse_bbox_param(request.args.get('bbox'))
    except ValueError as exc:
        abort(400, description=str(exc))

    if INFRA_DATASET is None:
        message = str(INFRA_DATASET_ERROR) if INFRA_DATASET_ERROR else 'Infraestructura data unavailable.'
        abort(500, description=message)

    dataset = INFRA_DATASET

    try:
        page = max(1, int(request.args.get('page', 1)))
    except (TypeError, ValueError):
        abort(400, description='"page" must be an integer value.')

    page_size_value = request.args.get('page_size', 750)
    try:
        page_size = max(1, min(5000, int(page_size_value)))
    except (TypeError, ValueError):
        abort(400, description='"page_size" must be an integer value.')

    filtered: List[dict] = []
    for feature, feature_bounds in zip(dataset.features, dataset.bounds):
        if feature_bounds and _bbox_intersects(feature_bounds, bbox):
            filtered.append(feature)

    total = len(filtered)
    start = (page - 1) * page_size
    end = start + page_size
    sliced = filtered[start:end]

    response = {
        'type': 'FeatureCollection',
        'features': sliced,
        'page': page,
        'page_size': page_size,
        'total': total,
        'returned': len(sliced),
        'has_more': end < total,
        'bbox': {
            'south': bbox[0],
            'west': bbox[1],
            'north': bbox[2],
            'east': bbox[3],
        },
    }

    return jsonify(response)

# This block allows you to run the app directly from the script
if __name__ == '__main__':
    app.run(debug=True)