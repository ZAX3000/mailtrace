# app/blueprints/api.py
from __future__ import annotations

from uuid import UUID

from typing import cast
from flask import Blueprint, jsonify, request, session, current_app, Flask
from sqlalchemy import text
from app.extensions import db

from app.errors import BadRequest, NotFound, Conflict, Unauthorized

from app.services import runs as runs_svc
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
    if not current_app.config.get("DISABLE_AUTH"):
        return

    uid = session.get("user_id")
    if uid:
        db.session.execute(
            text("""
                INSERT INTO users (id, email, provider, full_name, created_at)
                VALUES (CAST(:uid AS uuid), :email, 'dev', 'Dev User', NOW())
                ON CONFLICT (id) DO NOTHING
            """),
            {"uid": uid, "email": f"dev_{str(uid)[:8]}@local"},
        )
        db.session.commit()
        session.setdefault("email", f"dev_{str(uid)[:8]}@local")
        return

    row = db.session.execute(
        text("""
            WITH seeded AS (
              INSERT INTO users (email, provider, full_name, created_at)
              VALUES ('dev_' || substring(gen_random_uuid()::text,1,8) || '@local',
                      'dev', 'Dev User', NOW())
              RETURNING id::text AS id, email
            )
            SELECT id, email FROM seeded
        """)
    ).one()
    session["user_id"] = row.id
    session["email"] = row.email
    db.session.commit()

def _uid() -> str:
    _ensure_dev_session_user()
    uid = session.get("user_id")
    if not uid:
        raise BadRequest("missing session user")
    return str(uid)

def _norm_source(s: str) -> str:
    src = (s or "").strip().lower()
    if src not  in VALID_SOURCES:
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
    payload = svc_ingest_raw_file(str(run_id), uid, source, f.stream, filename=f.filename or "")
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
    missing = pipeline.check_mapping_readiness(str(run_id))
    if missing:
        return jsonify({"message": "Mapping required", "missing": missing}), 409

    flask_app = cast(Flask, getattr(current_app, "_get_current_object")())
    pipeline.start_pipeline(str(run_id), uid, flask_app)
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


@api_bp.get("/runs/latest")
def latest_run():
    """
    Return the user's latest run snapshot.
    If you pass ?require=done, only return a completed run.
    200 with JSON if found, 204 if none.
    """
    uid = _uid()
    require = (request.args.get("require") or "").strip().lower()
    only_done = require in {"done", "completed", "finished", "true", "1", "yes"}
    rec = pipeline.latest_run_for_user(uid, only_done=only_done)
    if not rec:
        return ("", 204)
    return jsonify(rec), 200


@api_bp.get("/runs")
def list_runs():
    """
    Return a compact list of the user's recent runs for the History dropdown.
    Query params:
      - limit: int (default 25, max 100)
      - before: run_id (optional, cursor)
    """
    uid = _uid()

    try:
        limit = int(request.args.get("limit", 25))
    except ValueError:
        limit = 25
    limit = max(1, min(limit, 100))

    before = request.args.get("before") or None  # optional run_id cursor

    rows = runs_svc.list_for_user(uid, limit=limit, before=before)  # <-- use runs_svc

    return jsonify({
        "items": rows,          # [{id, started_at, summary?, status}]
        "next_cursor": rows[-1]["id"] if rows else None
    }), 200


@api_bp.post("/runs/<uuid:run_id>/activate")
def activate_run(run_id: UUID):
    uid = _uid()
    ok = runs_svc.set_active_run(uid, str(run_id))
    return jsonify({"ok": bool(ok)}), 200

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