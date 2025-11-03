# app/services/pipeline.py
from __future__ import annotations

import logging
import time
from threading import Thread, Event, current_thread
from typing import Dict, Any, List, Set, Optional, Callable

from flask import current_app, has_app_context

from app.dao import run_dao, result_dao, staging_dao, mapper_dao
from app.services import summary
from app.services.mapper import canon_for, apply_mapping
from app.services.matching import persist_matches_for_run  # writes to matches table

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
    "failed":              (100, "Run failed"),
}

# ---------- Tiny helpers (handle legacy/new DAO signatures) ----------
def _count_with_optional_user(fn: Callable, run_id: str, user_id: Optional[str]) -> int:
    """Call count fn that may accept (run_id) or (run_id, user_id)."""
    try:
        return int(fn(run_id, user_id))  # new-style (rid, uid)
    except TypeError:
        return int(fn(run_id))           # legacy (rid)

def _fetch_with_user(fn: Callable, run_id: str, user_id: Optional[str], limit: Optional[int] = None):
    """
    Always pass user_id to fetch fns that expect it; tolerate legacy signatures that don't.
    We prefer fetch(run_id, user_id[, limit]) but fall back to fetch(run_id[, limit]).
    """
    try:
        if limit is None:
            return fn(run_id, user_id)
        return fn(run_id, user_id, limit)
    except TypeError:
        if limit is None:
            return fn(run_id)
        return fn(run_id, limit)

def _set(run_id: str, key: str, *, pct: int | None = None, msg: str | None = None) -> None:
    """Single place to push status/pct/message with sane defaults."""
    default_pct, default_msg = STEP.get(key, (None, None))
    pct = default_pct if pct is None else pct
    msg = default_msg if msg is None else msg
    log.debug("status [%s] run_id=%s pct=%s msg=%s", key, run_id, pct, msg)
    run_dao.update_step(run_id, step=key, pct=pct or 0, message=msg or "")

def _tick(run_id: str, substep: str, *, pct: int | None = None, msg: str | None = None) -> None:
    """
    Lightweight progress nudge during long matching phases.
    Uses the 'matching' umbrella step; decorates message with a substep label.
    """
    base_pct, _ = STEP.get("matching", (90, "Linking Mail ↔ CRM"))
    pct = base_pct if pct is None else pct
    m = f"[{substep}] {msg or ''}".strip()
    log.debug("tick run_id=%s ctx=%s pct=%s %s", run_id, has_app_context(), pct, m)
    run_dao.update_step(run_id, step="matching", pct=pct, message=m)

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
    If both sources are normalized, mark 'matching' and start the worker thread.
    Idempotent: won't double-start if already in matching/aggregating/done/failed.
    """
    meta = run_dao.status(run_id) or {}
    current = (meta or {}).get("status")
    log.debug("maybe_kick_matching: run_id=%s status=%r", run_id, current)

    if current in {"matching", "aggregating", "done", "failed"}:
        log.debug("maybe_kick_matching: already active/terminal, skip")
        return
    if not _sources_ready(run_id):
        log.debug("maybe_kick_matching: sources not ready, skip")
        return

    # Transition to 'matching' to block duplicates
    _set(run_id, "matching")  # pct≈90 + friendly label
    log.info("maybe_kick_matching: starting background matcher for run_id=%s", run_id)

    app = current_app._get_current_object()
    t = Thread(
        target=_match_and_aggregate_async,
        args=(app, run_id),
        daemon=True,
        name=f"mt-match-{run_id}",
    )
    t.start()

def get_result(run_id: str) -> Dict[str, Any]:
    """Return consolidated result payload once status == 'done'."""
    meta = run_dao.status(run_id) or {}
    if (meta or {}).get("status") != "done":
        log.debug("get_result(%s): not ready yet", run_id)
        return {"error": "not_ready"}
    payload = result_dao.get_full_result(run_id)
    log.debug("get_result(%s): bytes=%s", run_id, len(str(payload)))
    return payload

# -------- Mapping gate (used by POST /runs/:id/start before normalize) --------

def check_mapping_readiness(run_id: str) -> Dict[str, List[str]]:
    """
    Returns a dict of missing canonical fields per source, e.g.:
      {}  -> ready
      {"mail": ["zip"], "crm": ["job_date"]}  -> needs mapping
    Logic: RAW headers + saved mapping + alias tables.
    """
    out: Dict[str, List[str]] = {}

    for source in ("mail", "crm"):
        required, alias = canon_for(source)  # required: Set[str], alias: Dict[str, List[str]]

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

    _required, alias = canon_for(source)
    mapping = mapper_dao.get_mapping(run_id, source) or {}

    if source == "mail":
        _set(run_id, "normalizing_mail")
    else:
        _set(run_id, "normalizing_crm")

    raw_rows: List[Dict[str, Any]] = mapper_dao.get_raw_rows(run_id, source)
    log.debug("normalize_from_raw: raw_rows=%d", len(raw_rows))

    normalized = apply_mapping(raw_rows, mapping, alias)
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
        rows_for_db = [dict(r, source_id=_to_str_or_none(r.get("id"))) for r in normalized]

        _set(run_id, "mail_inserting")
        count = mapper_dao.insert_normalized_mail(run_id, user_id, rows_for_db)
        log.info("normalize_from_raw: inserted %d rows into staging_mail", count)

        try:
            run_dao.update_counts(run_id, mail_count=count, mail_ready=True)
        except TypeError:
            log.debug("run_dao.update_counts signature mismatch; skipping mail_ready flag")

        _set(run_id, "mail_ready")

    else:
        rows_for_db = [dict(r, source_id=_to_str_or_none(r.get("source_id") or r.get("id"))) for r in normalized]

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
    Uses count_* if present (preferred), else falls back to fetch (both tolerant to user_id arity).
    """
    meta = run_dao.status(run_id) or {}
    user_id = meta.get("user_id") or run_dao.get_user_id(run_id)

    count_mail = getattr(staging_dao, "count_normalized_mail", None)
    count_crm  = getattr(staging_dao, "count_normalized_crm", None)

    if callable(count_mail) and callable(count_crm):
        ok = (_count_with_optional_user(count_mail, run_id, user_id) > 0
              and _count_with_optional_user(count_crm, run_id, user_id) > 0)
        log.debug("_sources_ready(%s) via counts -> %s", run_id, ok)
        return ok

    # Fallback: fetch a few rows (DAO may require user_id)
    mail_rows: List[dict] = _fetch_with_user(staging_dao.fetch_normalized_mail_rows, run_id, user_id, limit=1)
    crm_rows:  List[dict] = _fetch_with_user(staging_dao.fetch_normalized_crm_rows,  run_id, user_id, limit=1)
    ok = bool(mail_rows) and bool(crm_rows)
    log.debug("_sources_ready(%s) via fetch -> %s", run_id, ok)
    return ok

def _match_and_aggregate_async(app, run_id: str) -> None:
    """
    Background worker:
      1) Build/refresh `matches` rows for this run (persist_matches_for_run)
      2) Build summary payload (summary.build_payload) reading from `matches`
      3) Save payload and mark run done
    """
    with app.app_context():
        stop = Event()

        def _heartbeat() -> None:
            while not stop.wait(5):
                try:
                    _tick(run_id, "heartbeat", pct=92, msg="Working…")
                except Exception:
                    pass

        hb = Thread(target=_heartbeat, daemon=True, name=f"mt-match-hb-{run_id}")
        hb.start()

        try:
            log.info("matcher: begin run_id=%s thread=%s", run_id, current_thread().name or "n/a")

            _tick(run_id, "load", pct=91, msg="Loading normalized rows")

            # Identify user for this run (needed by multi-tenant staging reads)
            run_meta = run_dao.status(run_id) or {}
            user_id: Optional[str] = run_meta.get("user_id") or run_dao.get_user_id(run_id)

            # Optional counts for nicer progress text
            try:
                mail_n = _count_with_optional_user(
                    getattr(staging_dao, "count_normalized_mail",
                            lambda rid, uid: len(_fetch_with_user(staging_dao.fetch_normalized_mail_rows, rid, uid))),
                    run_id, user_id
                )
                crm_n  = _count_with_optional_user(
                    getattr(staging_dao, "count_normalized_crm",
                            lambda rid, uid: len(_fetch_with_user(staging_dao.fetch_normalized_crm_rows, rid, uid))),
                    run_id, user_id
                )
                _tick(run_id, "fetch_done", pct=92, msg=f"Fetched mail={mail_n} crm={crm_n}")
            except Exception:
                _tick(run_id, "fetch_done", pct=92, msg="Fetched normalized rows")

            _tick(run_id, "match_start", pct=93, msg="Running matcher")
            t0 = time.time()

            # Fetch normalized rows (DAO may require user_id)
            mail_rows: List[dict] = _fetch_with_user(staging_dao.fetch_normalized_mail_rows, run_id, user_id)
            crm_rows:  List[dict] = _fetch_with_user(staging_dao.fetch_normalized_crm_rows,  run_id, user_id)

            if not mail_rows or not crm_rows:
                _tick(run_id, "match_done", pct=96, msg="No rows to match")
            else:
                inserted = persist_matches_for_run(run_id, user_id, mail_rows, crm_rows)
                _tick(run_id, "match_done", pct=96, msg=f"Matched rows={inserted}")
                log.debug("matcher: persist_matches_for_run wrote %s rows in %.2fs",
                          inserted, time.time() - t0)

            _tick(run_id, "kpi", pct=96, msg="Computing KPIs")
            t1 = time.time()
            try:
                payload = summary.build_payload(
                    run_id,
                    on_progress=lambda label, pct=None, msg=None: _tick(run_id, label, pct=pct, msg=msg),
                )
            except TypeError:
                log.debug("matcher: summary.build_payload has legacy signature; continuing without on_progress")
                payload = summary.build_payload(run_id)

            log.debug("matcher: payload built (len=%s) for run_id=%s dur=%.2fs",
                      len(str(payload)), run_id, time.time() - t1)

            _set(run_id, "aggregating")
            _tick(run_id, "finalize", pct=99, msg="Finalizing payload")

            # If your summary builder doesn't persist, you might do:
            # result_dao.save_payload(run_id, payload)  # <- uncomment if needed

            _set(run_id, "done")
            log.info("matcher: done run_id=%s", run_id)

        except Exception as e:
            log.exception("matcher: failed run_id=%s: %s", run_id, e)
            run_dao.update_step(run_id, step="failed", pct=100, message=str(e))

        finally:
            stop.set()