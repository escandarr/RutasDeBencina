import atexit
from typing import Tuple

from flask import Flask, abort, jsonify, render_template, request

from db.data_access import Database, RouteResult, compute_route_between_points

# Create an instance of the Flask application
app = Flask(__name__)

DATABASE = Database()
atexit.register(DATABASE.close)


Point = Tuple[float, float]


def _parse_point_payload(payload: dict, key: str) -> Point:
    data = payload.get(key)
    if not isinstance(data, dict):
        raise ValueError(f'"{key}" must be an object with "lat" and "lon" fields.')

    try:
        lat = float(data["lat"])
        lon = float(data["lon"])
    except (KeyError, TypeError, ValueError):
        raise ValueError(f'"{key}" must contain numeric "lat" and "lon" values.')

    return lon, lat

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


def _route_result_to_feature(route: RouteResult) -> dict:
    coords = list(route.coordinates)
    if len(coords) == 1:
        coords = coords * 2

    return {
        "type": "Feature",
        "geometry": {
            "type": "LineString",
            "coordinates": coords,
        },
        "properties": {
            "start_node": route.start.id,
            "end_node": route.end.id,
            "total_cost": route.total_cost,
            "segment_count": len(route.segments),
        },
    }


@app.route('/api/routes/shortest', methods=['POST'])
def api_shortest_route():
    payload = request.get_json(silent=True)
    if payload is None:
        abort(400, description='Expected JSON body with "start" and "end" points.')

    try:
        start_lon, start_lat = _parse_point_payload(payload, 'start')
        end_lon, end_lat = _parse_point_payload(payload, 'end')
    except ValueError as exc:
        abort(400, description=str(exc))

    with DATABASE.connection() as conn:
        try:
            route = compute_route_between_points(
                conn,
                start_lon,
                start_lat,
                end_lon,
                end_lat,
            )
        except ValueError as exc:
            abort(400, description=str(exc))
        except Exception as exc:  # pragma: no cover - defensive fallback
            app.logger.exception("Error computing route", exc_info=exc)
            abort(500, description='Unable to compute route at this time.')

    feature = _route_result_to_feature(route)

    return jsonify(
        {
            "route": feature,
            "start": {"id": route.start.id, "lon": route.start.lon, "lat": route.start.lat},
            "end": {"id": route.end.id, "lon": route.end.lon, "lat": route.end.lat},
        }
    )

# This block allows you to run the app directly from the script
if __name__ == '__main__':
    app.run(debug=True)