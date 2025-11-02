from __future__ import annotations
from typing import Dict, Any

from app.errors import NotFound, Conflict, Unauthorized
from app.dao import run_dao, result_dao  # service may talk to DAOs

def get_result(run_id: str, user_id: str) -> Dict[str, Any]:
    """Return the final result payload for a completed run, enforcing ownership and run state."""
    meta = run_dao.status(run_id)
    if not meta:
        raise NotFound("run not found")
    if str(meta.user_id) != str(user_id):
        raise Unauthorized("forbidden")
    if getattr(meta, "status", None) != "done":
        # 409 to signal 'not ready yet' to the client polling loop
        raise Conflict("not_ready")

    return result_dao.get_full_result(run_id)