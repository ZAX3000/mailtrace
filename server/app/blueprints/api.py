# app/blueprints/api.py
from __future__ import annotations
from flask import Blueprint, jsonify, request, session, current_app

from app.errors import BadRequest
from app.services import pipeline
from app.services.result import get_result as svc_get_result
from app.services.mapper import (
    get_headers as svc_get_headers,
    get_mapping as svc_get_mapping,
    save_mapping as svc_save_mapping,
    ingest_raw_file as svc_ingest_raw_file,
)

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
    from app.blueprints.auth import _ensure_dev_user
    u = _ensure_dev_user()
    session["user_id"] = str(u.id)
    session["email"] = u.email

@api_bp.post("/runs")
def create_run():
    _ensure_dev_session_user()
    user_id = session.get("user_id")
    run_id = pipeline.create_or_get_active_run(user_id)  # service handles DAO
    return jsonify({"run_id": str(run_id)}), 201

@api_bp.post("/runs/<uuid:run_id>/uploads/<kind>")
def upload_raw(run_id, kind):
    _ensure_dev_session_user()
    user_id = session.get("user_id")
    f = request.files.get("file")
    if not f:
        raise BadRequest("missing file")
    # service parses CSV and writes RAW rows; may return 409-need_mapping payload
    status, payload = svc_ingest_raw_file(str(run_id), str(user_id), kind, f.stream, filename=f.filename)
    if status == "need_mapping":
        return jsonify(payload), 409
    return jsonify(payload), 201  # {run_id, side, state:'ready'|'raw_only'}

@api_bp.post("/runs/<uuid:run_id>/mapping")
def save_mapping(run_id):
    _ensure_dev_session_user()
    payload = request.get_json(force=True) or {}
    kind = (payload.get("kind") or "mail").lower()
    mapping = payload.get("mapping") or {}
    out = svc_save_mapping(str(run_id), kind, mapping)
    return jsonify(out)

@api_bp.get("/runs/<uuid:run_id>/headers")
def headers_for_mapper(run_id):
    _ensure_dev_session_user()
    kind = (request.args.get("kind") or "mail").lower()
    sample = int(request.args.get("sample") or 25)
    return jsonify(svc_get_headers(str(run_id), kind, sample))

@api_bp.get("/runs/<uuid:run_id>/mapping")
def get_mapping(run_id):
    _ensure_dev_session_user()
    kind = (request.args.get("kind") or "mail").lower()
    return jsonify(svc_get_mapping(str(run_id), kind))

@api_bp.get("/runs/<uuid:run_id>/status")
def run_status(run_id):
    _ensure_dev_session_user()
    return jsonify(pipeline.get_status(str(run_id)))  # service

@api_bp.get("/runs/<uuid:run_id>/result")
def run_result(run_id):
    _ensure_dev_session_user()
    user_id = session.get("user_id")
    return jsonify(svc_get_result(str(run_id), str(user_id)))