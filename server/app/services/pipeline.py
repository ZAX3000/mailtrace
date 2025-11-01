# app/services/pipeline.py
from __future__ import annotations

from threading import Thread
from typing import Dict, Any, List

from app.dao import run_dao, result_dao, staging_dao
from app.services import summary


# -------- Public API (controllers call only these) --------

def create_or_get_active_run(user_id: str) -> str:
    """
    Ensure there's an active run for this user and return its id.
    run_dao is the source of truth for status/pct/step.
    """
    return run_dao.create_or_get_active_run(user_id)


def get_status(run_id: str) -> Dict[str, Any]:
    """
    Compact, UI-friendly status snapshot, e.g.:
      { "run_id": "...", "status": "matching|done|failed|...", "pct": 0-100,
        "step": "text", "message": "...", "error": "..."? }
    """
    return run_dao.status(run_id)


def maybe_kick_matching(run_id: str) -> None:
    """
    If BOTH normalized sides exist for this run and the run isn't already
    in a terminal/active state, switch to 'matching' and launch the async
    worker to match → aggregate → persist result.
    """
    meta = run_dao.get(run_id)
    if not meta:
        return

    current = getattr(meta, "status", None)
    if current in {"matching", "aggregating", "done", "failed"}:
        return

    if not _sides_ready(run_id):
        return

    run_dao.update_step(run_id, step="matching", pct=70, message="Computing matches")
    Thread(target=_match_and_aggregate_async, args=(run_id,), daemon=True).start()


def get_result(run_id: str) -> Dict[str, Any]:
    """
    Return the consolidated result payload (KPIs, series, download links).
    Intended to be called by the UI after status == 'done'.
    """
    meta = run_dao.get(run_id)
    if not meta or getattr(meta, "status", None) != "done":
        return {"error": "not_ready"}
    return result_dao.get_full_result(run_id)


# -------- Optional convenience for mapper service --------

def on_normalized_side_ready(run_id: str) -> None:
    """
    Call this from mapper_service.normalize_from_raw(...) whenever a side completes.
    It will opportunistically kick matching if both sides are now ready.
    """
    maybe_kick_matching(run_id)


# -------- Internal helpers --------

def _sides_ready(run_id: str) -> bool:
    """
    True when both normalized staging tables have rows for this run_id.
    Uses DAO fetchers; you can replace with COUNT-based DAO methods later
    (e.g., staging_dao.count_normalized_mail/crm) without changing callers.
    """
    # NOTE: if datasets are large, swap these to COUNT queries in the DAO.
    mail_rows: List[dict] = staging_dao.fetch_normalized_mail_rows(run_id)
    crm_rows:  List[dict] = staging_dao.fetch_normalized_crm_rows(run_id)
    return bool(mail_rows) and bool(crm_rows)


def _match_and_aggregate_async(run_id: str) -> None:
    """
    Background worker:
      1) Build final payload (summary builds KPIs/series via matching.run_matching)
      2) Persist a single result blob for fast /result responses
      3) Mark the run done; on error, mark failed with message
    """
    try:
        run_dao.update_step(run_id, step="matching", pct=75, message="Linking mail ↔ CRM")

        # Runs matcher and KPI/series aggregation over normalized staging
        payload: Dict[str, Any] = summary.build_payload(run_id)

        # Persist results (e.g., JSON blob or table rows depending on your result_dao)
        result_dao.save_full_result(run_id, payload)

        run_dao.update_step(run_id, step="done", pct=100, message="Run complete")
    except Exception as e:
        run_dao.update_step(run_id, step="failed", pct=100, message=str(e))