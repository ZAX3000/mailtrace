# app/blueprints/api.py
from __future__ import annotations

import json
from flask import Blueprint, jsonify, request, session, current_app

from app.errors import BadRequest
from app.services import pipeline

api_bp = Blueprint("api", __name__, url_prefix="/api")

def _ensure_dev_session_user():
    """
    In dev (DISABLE_AUTH=1), ensure session has a real user_id backed by DB.
    Reuses the helper in auth blueprint to create/get a durable dev user.
    """
    if not current_app.config.get("DISABLE_AUTH"):
        return
    if "user_id" in session:
        return
    # Lazy import to avoid circulars at module import time
    from app.blueprints.auth import _ensure_dev_user
    u = _ensure_dev_user()
    session["user_id"] = str(u.id)
    session["email"] = u.email

@api_bp.post("/uploads")
def uploads():
    """
    POST multipart/form-data:
      - file: CSV file
      - kind: 'mail' | 'crm'
      - mapping: optional JSON string of column-map
    """
    # Dev auto-login if auth is disabled
    _ensure_dev_session_user()

    f = request.files.get("file")
    kind = (request.form.get("kind") or "").strip().lower()
    mapping_raw = request.form.get("mapping")
    mapping_json = None
    if mapping_raw:
        try:
            mapping_json = json.loads(mapping_raw)
        except Exception:
            raise BadRequest("mapping must be valid JSON")

    if not f or kind not in {"mail", "crm"}:
        raise BadRequest("file and kind (mail|crm) are required")

    user_id = session.get("user_id")
    if not user_id:
        return jsonify({"error": "unauthorized"}), 401

    run_id = pipeline.start_upload_pipeline(
        user_id=user_id,
        kind=kind,
        file_stream=f.stream,
        filename=f.filename,
        mapping_json=mapping_json,
    )
    return jsonify({"run_id": str(run_id), "step": "queued"}), 202


@api_bp.get("/runs/<uuid:run_id>/status")
def run_status(run_id):
    _ensure_dev_session_user()
    return jsonify(pipeline.get_status(str(run_id)))


@api_bp.get("/matches/<uuid:match_id>/result")
def match_result(match_id):
    _ensure_dev_session_user()
    return jsonify(pipeline.get_result(str(match_id))), 501