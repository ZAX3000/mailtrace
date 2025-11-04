# app/blueprints/api.py
from __future__ import annotations

from uuid import UUID

from flask import Blueprint, jsonify, request, session, current_app

from app.errors import BadRequest, NotFound, Conflict, Unauthorized
from app.services import pipeline
from app.services.result import get_result as svc_get_result
from app.services.mapper import (
    get_headers as svc_get_headers,
    get_mapping as svc_get_mapping,
    save_mapping as svc_save_mapping,
    ingest_raw_file as svc_ingest_raw_file,
)

api_bp = Blueprint("api", __name__, url_prefix="/api")

VALID_SOURCES = {"mail", "crm"}

# -----------------------
# Helpers / dev session
# -----------------------

def _ensure_dev_session_user() -> None:
    """
    In dev (DISABLE_AUTH=1), ensure session has a real user_id backed by DB.
    """
    if not current_app.config.get("DISABLE_AUTH"):
        return
    if "user_id" in session:
        return
    from app.blueprints.auth import _ensure_dev_user  # lazy to avoid cycles
    u = _ensure_dev_user()
    session["user_id"] = str(u.id)
    session["email"] = u.email

def _uid() -> str:
    _ensure_dev_session_user()
    uid = session.get("user_id")
    if not uid:
        # If auth is enabled and there's no user, treat as bad request for now.
        raise BadRequest("missing session user")
    return str(uid)

def _norm_source(s: str) -> str:
    src = (s or "").strip().lower()
    if src not in VALID_SOURCES:
        raise BadRequest(f"invalid source: {s!r}")
    return src

# -----------------------
# Runs lifecycle
# -----------------------

@api_bp.post("/runs")
def create_run():
    uid = _uid()
    run_id = pipeline.create_or_get_active_run(uid)
    return jsonify({"run_id": str(run_id)}), 201


@api_bp.post("/runs/<uuid:run_id>/uploads/<source>")
def upload_raw(run_id: UUID, source: str):
    uid = _uid()
    source = _norm_source(source)

    f = request.files.get("file")
    if not f:
        raise BadRequest("missing file")

    # Parse CSV and store RAW rows (JSONB) only
    _fname, payload = svc_ingest_raw_file(
        str(run_id), uid, source, f.stream, filename=f.filename or ""
    )
    return jsonify(payload), 201


@api_bp.post("/runs/<uuid:run_id>/mapping")
def save_mapping_route(run_id: UUID):
    uid = _uid()
    body = request.get_json(force=True) or {}
    source = _norm_source(body.get("source") or "mail")
    mapping = body.get("mapping") or {}
    out = svc_save_mapping(str(run_id), uid, source, mapping)
    return jsonify(out), 200


@api_bp.post("/runs/<uuid:run_id>/start")
def start_run(run_id: UUID):
    uid = _uid()

    # fail fast on mapping gaps
    missing = pipeline.check_mapping_readiness(str(run_id))
    if missing:
        return jsonify({"message": "Mapping required", "missing": missing}), 409

    pipeline.start_pipeline(str(run_id), uid)
    return jsonify({"ok": True}), 202


@api_bp.get("/runs/<uuid:run_id>/status")
def run_status(run_id: UUID):
    _ = _uid()
    return jsonify(pipeline.get_status(str(run_id))), 200


@api_bp.get("/runs/<uuid:run_id>/result")
def run_result(run_id: UUID):
    uid = _uid()
    try:
        return jsonify(svc_get_result(str(run_id), uid)), 200
    except NotFound as e:
        return jsonify({"error": str(e)}), 404
    except Unauthorized as e:
        return jsonify({"error": str(e)}), 403
    except Conflict as e:
        return jsonify({"error": str(e)}), 409


# -----------------------
# Mapper utilities
# -----------------------


@api_bp.get("/runs/<uuid:run_id>/headers")
def headers_for_mapper(run_id: UUID):
    _ = _uid()
    source = _norm_source(request.args.get("source") or "mail")
    sample = int(request.args.get("sample") or 25)
    return jsonify(svc_get_headers(str(run_id), source, sample)), 200


@api_bp.get("/runs/<uuid:run_id>/mapping")
def get_mapping(run_id: UUID):
    _ = _uid()
    source = _norm_source(request.args.get("source") or "mail")
    return jsonify(svc_get_mapping(str(run_id), source)), 200