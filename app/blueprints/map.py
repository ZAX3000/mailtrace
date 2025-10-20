# app/blueprints/map.py
from __future__ import annotations
import os, time
from flask import Blueprint, current_app, jsonify, make_response, render_template
from .auth import login_required
from app.services.map_cache import cached_payload_if_exists, build_map_cache

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
    payload = cached_payload_if_exists()
    if payload is None:
        # Ensure an empty cache exists and try again
        path = build_map_cache()
        with open(path, "rb") as f:
            payload = f.read()

    resp = make_response(payload)
    resp.mimetype = "application/json"
    resp.headers["Cache-Control"] = "public, max-age=60"
    return resp