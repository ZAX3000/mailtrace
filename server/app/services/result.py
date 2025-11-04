from __future__ import annotations
from typing import Dict, Any, Mapping, cast

from app.errors import NotFound, Conflict, Unauthorized
from app.dao import run_dao, result_dao

def get_result(run_id: str, user_id: str) -> Dict[str, Any]:
    """Return the final result payload for a run that is done, enforcing ownership and run state."""
    meta = run_dao.status(run_id)
    m = cast(Mapping[str, Any], meta)
    if not meta:
        raise NotFound("run not found")
    if str(m.get("user_id", "")) != str(user_id):
        raise Unauthorized("forbidden")
    if getattr(meta, "status", None) != "done":
        raise Conflict("not_ready")

    return result_dao.get_full_result(run_id)