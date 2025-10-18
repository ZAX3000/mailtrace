# app/mapview.py
from flask import Blueprint, current_app, jsonify, render_template
from .extensions import db
from .models import GeoPoint
from .auth import login_required

# Name the blueprint "map" and give it a /map prefix
map_bp = Blueprint("map", __name__, url_prefix="/map")

@map_bp.get("/")
@login_required
def index():
    token = current_app.config.get("MAPBOX_TOKEN", "").strip()
    use_mapbox = bool(token)
    return render_template("map.html", use_mapbox=use_mapbox, mapbox_token=token)

@map_bp.get("/data")
@login_required
def data():
    # Return latest 1000 geo points (matches first, then mail)
    q = db.session.query(GeoPoint).order_by(GeoPoint.created_at.desc()).limit(1000).all()
    feats = []
    for g in q:
        if g.lat is None or g.lon is None:
            continue
        feats.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [g.lon, g.lat]},
            "properties": {"label": g.label or "", "address": g.address or "", "kind": g.kind or ""},
        })
    return jsonify({"type": "FeatureCollection", "features": feats})