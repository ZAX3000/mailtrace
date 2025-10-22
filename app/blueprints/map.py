# app/blueprints/map.py
from __future__ import annotations

from typing import Optional, cast

from flask import Blueprint, current_app, make_response, render_template, Response
from flask.typing import ResponseReturnValue

from .auth import login_required
from app.services.map_cache import cached_payload_if_exists, build_map_cache

map_bp = Blueprint("map", __name__, url_prefix="/map")


@map_bp.get("/")
@login_required
def index() -> str:
    token = (current_app.config.get("MAPBOX_TOKEN") or "").strip()
    use_mapbox = bool(token)
    return render_template("map.html", use_mapbox=use_mapbox, mapbox_token=token)


@map_bp.get("/data")
@login_required
def data() -> ResponseReturnValue:
    # Try in-memory / precomputed cache first
    payload: Optional[bytes] = cached_payload_if_exists()

    # If not present, build on demand and read from disk
    if payload is None:
        path = build_map_cache()
        try:
            with open(path, "rb") as f:
                payload = f.read()
        except OSError:
            # If the environment is read-only or cache build/read failed,
            # return an empty JSON object rather than a 500.
            payload = b"{}"

    # Build response
    resp: Response = make_response(cast(bytes, payload))
    resp.mimetype = "application/json"
    # Short-lived client/proxy cache; tune as needed
    resp.headers["Cache-Control"] = "public, max-age=60"
    return resp