# app/services/pipeline.py
from __future__ import annotations

from threading import Thread
from typing import Dict, Any, List, Set
from flask import current_app

from app.dao import run_dao, result_dao, staging_dao, mapper_dao
from app.services import summary
from app.services.mapper import _canon_for, _apply_mapping


# -------- Public API (controllers call only these) --------

def create_or_get_active_run(user_id: str) -> str:
    """
    Ensure there's an active run for this user and return its id.
    run_dao is the source of truth for status/pct/step.
    """
    return run_dao.create_or_get_active_run(user_id)


def get_status(run_id: str) -> Dict[str, Any]:
    """Compact, UI-friendly status snapshot."""
    return run_dao.status(run_id)


def maybe_kick_matching(run_id: str) -> None:
    meta = run_dao.status(run_id)
    if not meta:
        return
    current = getattr(meta, "status", None)
    if current in {"matching", "aggregating", "done", "failed"}:
        return
    if not _sources_ready(run_id):
        return

    # mark & start thread WITH app context captured
    run_dao.update_step(run_id, step="matching", pct=70, message="Computing matches")

    app = current_app._get_current_object()
    Thread(
        target=_match_and_aggregate_async,
        args=(app, run_id),   # pass the app into the thread
        daemon=True
    ).start()


def get_result(run_id: str) -> Dict[str, Any]:
    """Return consolidated result payload once status == 'done'."""
    meta = run_dao.status(run_id)
    if not meta or getattr(meta, "status", None) != "done":
        return {"error": "not_ready"}
    return result_dao.get_full_result(run_id)


# -------- Mapping gate (used by POST /runs/:id/run before normalize) --------

def check_mapping_readiness(run_id: str) -> Dict[str, List[str]]:
    """
    Returns a dict of missing canonical fields per source, e.g.:
      {}  -> ready
      {"mail": ["zip"], "crm": ["job_date"]}  -> needs mapping
    Logic: look at RAW headers + saved mapping + alias tables.
    """
    out: Dict[str, List[str]] = {}

    for source in ("mail", "crm"):
        required, alias = _canon_for(source)  # required: Set[str], alias: Dict[str, List[str]]

        hdrs = mapper_dao.get_raw_headers(run_id, source, sample=1)  # cheap probe
        raw_headers: List[str] = [h or "" for h in (hdrs.get("headers") or [])]
        raw_lc = {h.strip().lower() for h in raw_headers if h is not None}

        mapping = mapper_dao.get_mapping(run_id, source) or {}  # {canonical -> raw_header}

        # coverage by explicit mapping
        covered: Set[str] = {
            canon for canon, raw_name in mapping.items()
            if (raw_name or "").strip().lower() in raw_lc
        }

        # coverage by alias (fallbacks)
        for canon, alts in alias.items():
            if canon in covered:
                continue
            for alt in alts + [canon]:
                if (alt or "").strip().lower() in raw_lc:
                    covered.add(canon)
                    break

        missing = sorted(k for k in required if k not in covered)
        if missing:
            out[source] = missing

    return out


# -------- Normalize RAW -> staging for a single source --------

def normalize_from_raw(run_id: str, user_id: str, source: str) -> int:
    """
    Read RAW rows for `source`, apply mapping+aliases, coerce types, and write to staging_{source}.
    Returns the number of rows inserted.
    """
    source = (source or "").strip().lower()
    if source not in {"mail", "crm"}:
        raise ValueError(f"invalid source: {source}")

    required, alias = _canon_for(source)
    mapping = mapper_dao.get_mapping(run_id, source) or {}

    # Full RAW read (do NOT use get_raw_headers here)
    raw_rows: List[Dict[str, Any]] = mapper_dao.get_raw_rows(run_id, source)

    # Canonicalize keys using mapping + alias fallbacks
    normalized = _apply_mapping(raw_rows, mapping, alias)

    # --- type coercions live here (not at upload time) ---
    def _none_if_empty(v):
        if v is None: return None
        if isinstance(v, str) and v.strip() == "": return None
        return v

    def _to_str_or_none(v):
        v = _none_if_empty(v)
        return None if v is None else str(v).strip()

    def _coerce_mail(row):
        r = dict(row)
        r["id"] = _to_str_or_none(r.get("id"))  # <-- TEXT now
        # r["sent_date"] = _parse_date_or_none(r.get("sent_date"))  # if you type the column
        return r

    def _coerce_crm(row):
        r = dict(row)
        r["crm_id"] = _to_str_or_none(r.get("crm_id") or r.get("id"))  # <-- TEXT now
        # r["job_date"] = _parse_date_or_none(r.get("job_date"))
        return r

    if source == "mail":
        rows_for_db = [_coerce_mail(r) for r in normalized]
        count = mapper_dao.insert_normalized_mail(run_id, user_id, rows_for_db)
        run_dao.update_counts(run_id, mail_count=count, mail_ready=True)
    else:
        rows_for_db = [_coerce_crm(r) for r in normalized]
        count = mapper_dao.insert_normalized_crm(run_id, user_id, rows_for_db)
        run_dao.update_counts(run_id, crm_count=count, crm_ready=True)

    on_normalized_source_ready(run_id)
    return count


# -------- Optional convenience for mapper service --------

def on_normalized_source_ready(run_id: str) -> None:
    """Opportunistically kick matching if both sources ready."""
    maybe_kick_matching(run_id)


# -------- Internal helpers --------

def _sources_ready(run_id: str) -> bool:
    """
    True when both normalized staging tables have rows for this run_id.
    If you add count_normalized_mail/crm to staging_dao, switch to those for large datasets.
    """
    # Prefer COUNT-based DAO if available
    count_mail = getattr(staging_dao, "count_normalized_mail", None)
    count_crm  = getattr(staging_dao, "count_normalized_crm", None)

    if callable(count_mail) and callable(count_crm):
        return (count_mail(run_id) > 0) and (count_crm(run_id) > 0)

    # Fallback: fetch lists (fine for small/medium data)
    mail_rows: List[dict] = staging_dao.fetch_normalized_mail_rows(run_id)
    crm_rows:  List[dict] = staging_dao.fetch_normalized_crm_rows(run_id)
    return bool(mail_rows) and bool(crm_rows)

def _match_and_aggregate_async(app, run_id: str) -> None:
    # ensure the worker has a Flask app context
    with app.app_context():
        try:
            run_dao.update_step(run_id, step="matching", pct=75, message="Linking mail ↔ CRM")

            payload: Dict[str, Any] = summary.build_payload(run_id)

            result_dao.save_full_result(run_id, payload)
            run_dao.update_step(run_id, step="done", pct=100, message="Run complete")
        except Exception as e:
            # now this will succeed because we have app context
            run_dao.update_step(run_id, step="failed", pct=100, message=str(e))