# app/services/pipeline.py
from __future__ import annotations

import logging
from threading import Thread
from typing import Dict, Any, List, Set
from flask import current_app

from app.dao import run_dao, result_dao, staging_dao, mapper_dao
from app.services import summary
from app.services.mapper import _canon_for, _apply_mapping

log = logging.getLogger(__name__)

# ---------- Friendly step labels shown in UI ----------
STEP = {
    "starting":            (  5, "Starting run"),
    "normalizing_mail":    ( 15, "Normalizing Mail (reading RAW)"),
    "mail_inserting":      ( 35, "Normalizing Mail (writing to staging)"),
    "mail_ready":          ( 55, "Mail normalized"),
    "normalizing_crm":     ( 60, "Normalizing CRM (reading RAW)"),
    "crm_inserting":       ( 78, "Normalizing CRM (writing to staging)"),
    "crm_ready":           ( 85, "CRM normalized"),
    "matching":            ( 90, "Linking Mail ↔ CRM"),
    "aggregating":         ( 97, "Aggregating results"),
    "done":                (100, "Run complete"),
}

def _set(run_id: str, key: str, *, pct: int | None = None, msg: str | None = None) -> None:
    """Single place to push status/pct/message with sane defaults."""
    default_pct, default_msg = STEP.get(key, (None, None))
    pct = default_pct if pct is None else pct
    msg = default_msg if msg is None else msg
    log.debug("status [%s] run_id=%s pct=%s msg=%s", key, run_id, pct, msg)
    run_dao.update_step(run_id, step=key, pct=pct or 0, message=msg or "")

# -------- Public API (controllers call only these) --------

def create_or_get_active_run(user_id: str) -> str:
    """
    Ensure there's an active run for this user and return its id.
    run_dao is the source of truth for status/pct/step.
    """
    rid = run_dao.create_or_get_active_run(user_id)
    log.debug("create_or_get_active_run: user=%s -> run_id=%s", user_id, rid)
    return rid

def mark_start(run_id: str) -> None:
    """Flip UI immediately so loader pops before heavy work."""
    _set(run_id, "starting")

def get_status(run_id: str) -> Dict[str, Any]:
    """Compact, UI-friendly status snapshot (stable shape)."""
    s = run_dao.status(run_id) or {}
    out = {
        "run_id": run_id,
        "status": s.get("status") or "queued",
        "pct": s.get("pct") or 0,
        "step": s.get("step"),
        "message": s.get("message"),
    }
    log.debug("get_status(%s) -> %s", run_id, out)
    return out

def maybe_kick_matching(run_id: str) -> None:
    """
    If both sources are normalized, mark 'matching' and start the worker.
    Captures the Flask app and passes it into the thread to avoid
    'Working outside of application context.'
    """
    meta = run_dao.status(run_id) or {}
    current = (meta or {}).get("status")
    log.debug("maybe_kick_matching: run_id=%s status=%r", run_id, current)

    if current in {"matching", "aggregating", "done", "failed"}:
        log.debug("maybe_kick_matching: already in terminal/active step, skip")
        return
    if not _sources_ready(run_id):
        log.debug("maybe_kick_matching: sources not ready, skip")
        return

    _set(run_id, "matching")  # pct≈90 + friendly label
    log.info("maybe_kick_matching: starting background matcher for run_id=%s", run_id)

    app = current_app._get_current_object()
    Thread(
        target=_match_and_aggregate_async,
        args=(app, run_id),
        daemon=True,
        name=f"mt-match-{run_id}",
    ).start()

def get_result(run_id: str) -> Dict[str, Any]:
    """Return consolidated result payload once status == 'done'."""
    meta = run_dao.status(run_id) or {}
    if (meta or {}).get("status") != "done":
        log.debug("get_result(%s): not ready yet", run_id)
        return {"error": "not_ready"}
    payload = result_dao.get_full_result(run_id)
    log.debug("get_result(%s): bytes=%s", run_id, len(str(payload)))
    return payload

# -------- Mapping gate (used by POST /runs/:id/run before normalize) --------

def check_mapping_readiness(run_id: str) -> Dict[str, List[str]]:
    """
    Returns a dict of missing canonical fields per source, e.g.:
      {}  -> ready
      {"mail": ["zip"], "crm": ["job_date"]}  -> needs mapping

    Logic: RAW headers + saved mapping + alias tables.
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

    log.debug("check_mapping_readiness(%s) -> %s", run_id, out)
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

    log.info("normalize_from_raw: run_id=%s user_id=%s source=%s", run_id, user_id, source)

    _required, alias = _canon_for(source)
    mapping = mapper_dao.get_mapping(run_id, source) or {}

    # PHASE 1: read + map
    if source == "mail":
        _set(run_id, "normalizing_mail")
    else:
        _set(run_id, "normalizing_crm")

    raw_rows: List[Dict[str, Any]] = mapper_dao.get_raw_rows(run_id, source)
    log.debug("normalize_from_raw: raw_rows=%d", len(raw_rows))

    normalized = _apply_mapping(raw_rows, mapping, alias)
    log.debug("normalize_from_raw: normalized_rows=%d", len(normalized))

    # --- type coercions live here (not at upload time) ---
    def _none_if_empty(v):
        if v is None: return None
        if isinstance(v, str) and v.strip() == "": return None
        return v

    def _to_str_or_none(v):
        v = _none_if_empty(v)
        return None if v is None else str(v).strip()

    if source == "mail":
        rows_for_db = [dict(r, id=_to_str_or_none(r.get("id"))) for r in normalized]

        # PHASE 2: insert mail
        _set(run_id, "mail_inserting")
        count = mapper_dao.insert_normalized_mail(run_id, user_id, rows_for_db)
        log.info("normalize_from_raw: inserted %d rows into staging_mail", count)

        try:
            run_dao.update_counts(run_id, mail_count=count, mail_ready=True)
        except TypeError:
            log.debug("run_dao.update_counts signature mismatch; skipping mail_ready flag")

        _set(run_id, "mail_ready")

    else:
        rows_for_db = [dict(r, crm_id=_to_str_or_none(r.get("crm_id") or r.get("id"))) for r in normalized]

        # PHASE 2: insert crm
        _set(run_id, "crm_inserting")
        count = mapper_dao.insert_normalized_crm(run_id, user_id, rows_for_db)
        log.info("normalize_from_raw: inserted %d rows into staging_crm", count)

        try:
            run_dao.update_counts(run_id, crm_count=count, crm_ready=True)
        except TypeError:
            log.debug("run_dao.update_counts signature mismatch; skipping crm_ready flag")

        _set(run_id, "crm_ready")

    on_normalized_source_ready(run_id)
    return count

# -------- Optional convenience for mapper service --------

def on_normalized_source_ready(run_id: str) -> None:
    """Opportunistically kick matching if both sources ready."""
    log.debug("on_normalized_source_ready(%s)", run_id)
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
        ok = (count_mail(run_id) > 0) and (count_crm(run_id) > 0)
        log.debug("_sources_ready(%s) via counts -> %s", run_id, ok)
        return ok

    # Fallback: fetch lists (fine for small/medium data)
    mail_rows: List[dict] = staging_dao.fetch_normalized_mail_rows(run_id)
    crm_rows:  List[dict] = staging_dao.fetch_normalized_crm_rows(run_id)
    ok = bool(mail_rows) and bool(crm_rows)
    log.debug("_sources_ready(%s) via fetch -> %s", run_id, ok)
    return ok

def _match_and_aggregate_async(app, run_id: str) -> None:
    """Background worker: build summary payload and mark run complete."""
    with app.app_context():
        try:
            log.info("matcher: begin run_id=%s", run_id)
            # 'matching' already set by maybe_kick_matching()

            payload: Dict[str, Any] = summary.build_payload(run_id)
            log.debug("matcher: payload built (len=%s) for run_id=%s", len(str(payload)), run_id)

            _set(run_id, "aggregating")  # nudge to ~97% with a friendly label
            result_dao.save_full_result(run_id, payload)

            _set(run_id, "done")
            log.info("matcher: done run_id=%s", run_id)

        except Exception as e:
            log.exception("matcher: failed run_id=%s: %s", run_id, e)
            run_dao.update_step(run_id, step="failed", pct=100, message=str(e))