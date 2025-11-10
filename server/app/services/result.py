from __future__ import annotations
from typing import Dict, Any

from app.errors import NotFound, Conflict, Unauthorized
from app.dao import runs_dao
from app.services import summary

def get_result(run_id: str, user_id: str) -> Dict[str, Any]:
    """Return KPIs (computed from DB) for a finished run; enforce ownership and state."""
    meta = runs_dao.status(run_id)
    if not meta:
        raise NotFound("run not found")

    # enforce ownership
    if str(meta.get("user_id", "")) != str(user_id):
        raise Unauthorized("forbidden")

    status = (meta.get("status") or "").lower()
    if status == "failed":
        # Optional: surface a different message; keeping Conflict to match client logic
        raise Conflict("failed")
    if status != "done":
        raise Conflict("not_ready")

    # Compute fresh from the normalized/matched tables; no materialized payload needed.
    try:
        payload: Dict[str, Any] = summary.build_payload(run_id)
    except TypeError:
        # Back-compat if summary has optional on_progress param, etc.
        payload = summary.build_payload(run_id)

    # Ensure the shape the UI expects
    return {"kpis": payload.get("kpis", payload)}