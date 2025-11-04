# app/services/pipeline.py
from __future__ import annotations

import logging
import time
from threading import Thread, Event, current_thread
from typing import Dict, Any, List, Set, Optional, Callable
from datetime import date, datetime

from flask import current_app, has_app_context

from app.dao import run_dao, staging_dao, mapper_dao
from app.services import summary
from app.services.mapper import canon_for, apply_mapping
from app.services.matching import persist_matches_for_run

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
    try:
        return int(fn(run_id, user_id))
    except TypeError:
        return int(fn(run_id))

def _fetch_with_user(fn: Callable, run_id: str, user_id: Optional[str], limit: Optional[int] = None):
    try:
        if limit is None:
            return fn(run_id, user_id)
        return fn(run_id, user_id, limit)
    except TypeError:
        if limit is None:
            return fn(run_id)
        return fn(run_id, limit)

def _set(run_id: str, key: str, *, pct: int | None = None, msg: str | None = None) -> None:
    default_pct, default_msg = STEP.get(key, (None, None))
    pct = default_pct if pct is None else pct
    msg = default_msg if msg is None else msg
    log.debug("status [%s] run_id=%s pct=%s msg=%s", key, run_id, pct, msg)
    run_dao.update_step(run_id, step=key, pct=pct or 0, message=msg or "")

def _fail(run_id: str, *, msg: str) -> None:
    log.error("run %s failed: %s", run_id, msg)
    run_dao.update_step(run_id, step="failed", pct=100, message=msg)
    raise RuntimeError(msg)

def _tick(run_id: str, substep: str, *, pct: int | None = None, msg: str | None = None) -> None:
    base_pct, _ = STEP.get("matching", (90, "Linking Mail ↔ CRM"))
    pct = base_pct if pct is None else pct
    m = f"[{substep}] {msg or ''}".strip()
    log.debug("tick run_id=%s ctx=%s pct=%s %s", run_id, has_app_context(), pct, m)
    run_dao.update_step(run_id, step="matching", pct=pct, message=m)

_DATE_FORMATS = (
    "%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y", "%d-%m-%Y",
    "%Y/%m/%d", "%m/%d/%y", "%d-%m-%y",
)

def to_date_or_none(v: Any) -> Optional[date]:
    if isinstance(v, date):
        return v
    if not isinstance(v, str) or not v.strip():
        return None
    s = v.strip().replace("/", "-")
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    try:
        # tolerate ISO with time / trailing Z
        return datetime.fromisoformat(v.strip().replace("Z", "+00:00")).date()
    except Exception:
        return None

# -------- Public API (controllers call only these) --------

def create_or_get_active_run(user_id: str) -> str:
    rid = run_dao.create_or_get_active_run(user_id)
    log.debug("create_or_get_active_run: user=%s -> run_id=%s", user_id, rid)
    return rid

def mark_start(run_id: str) -> None:
    _set(run_id, "starting")

def get_status(run_id: str) -> Dict[str, Any]:
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

# -------- Orchestration entrypoint --------

def start_pipeline(run_id: str, user_id: str) -> None:
    mark_start(run_id)

    _set(run_id, "normalizing_mail")
    mail_n = normalize_from_raw(run_id, user_id, "mail")
    if mail_n <= 0:
        _fail(run_id, msg="Mail CSV appears empty or invalid after normalization.")

    _set(run_id, "normalizing_crm")
    crm_n = normalize_from_raw(run_id, user_id, "crm")
    if crm_n <= 0:
        _fail(run_id, msg="CRM CSV appears empty or invalid after normalization.")

    if not run_dao.pair_ready(run_id):
        _fail(run_id, msg="Staging not ready after normalization (internal consistency error).")

    start_matching(run_id)

# -------- Matching launcher (service-layer, uses DAO for persistence) --------

def start_matching(run_id: str) -> None:
    meta = run_dao.status(run_id) or {}
    current = (meta or {}).get("status")

    if current in {"matching", "aggregating", "done", "failed"}:
        log.debug("start_matching: run_id=%s already %s; skip", run_id, current)
        return

    if not run_dao.pair_ready(run_id):
        log.debug("start_matching: run_id=%s not pair_ready; skip", run_id)
        return

    run_dao.update_step(run_id, step="matching", pct=90, message="Linking Mail ↔ CRM")
    log.info("start_matching: claimed run_id=%s; spawning matcher thread", run_id)

    app = current_app._get_current_object()
    t = Thread(
        target=_match_and_aggregate_async,
        args=(app, run_id),
        daemon=True,
        name=f"mt-match-{run_id}",
    )
    t.start()

# -------- Mapping gate (used by controller before starting pipeline) --------

def check_mapping_readiness(run_id: str) -> Dict[str, List[str]]:
    out: Dict[str, List[str]] = {}

    for source in ("mail", "crm"):
        required, alias = canon_for(source)

        hdrs = mapper_dao.get_raw_headers(run_id, source, sample=1)
        raw_headers: List[str] = [h or "" for h in (hdrs.get("headers") or [])]
        raw_lc = {h.strip().lower() for h in raw_headers if h is not None}

        mapping = mapper_dao.get_mapping(run_id, source) or {}

        covered: Set[str] = {
            canon for canon, raw_name in mapping.items()
            if (raw_name or "").strip().lower() in raw_lc
        }

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
    source = (source or "").strip().lower()
    if source not in {"mail", "crm"}:
        raise ValueError(f"invalid source: {source}")

    log.info("normalize_from_raw: run_id=%s user_id=%s source=%s", run_id, user_id, source)

    required, alias = canon_for(source)
    mapping = mapper_dao.get_mapping(run_id, source) or {}

    raw_rows: List[Dict[str, Any]] = mapper_dao.get_raw_rows(run_id, source)
    log.debug("normalize_from_raw: raw_rows=%d", len(raw_rows))

    normalized = apply_mapping(raw_rows, mapping, alias)
    log.debug("normalize_from_raw: normalized_rows=%d", len(normalized))

    if len(normalized) == 0:
        missing = required - set(mapping.keys())
        hint = ""
        if missing:
            hint = f"Missing required mapping for: {', '.join(sorted(missing))}."
        _fail(
            run_id,
            msg=(f"{source.upper()} normalization produced 0 rows. "
                 f"Check your CSV and mapping. {hint}").strip()
        )

    def _none_if_empty(v):
        if v is None: 
            return None
        if isinstance(v, str) and v.strip() == "": 
            return None
        return v

    def _to_str_or_none(v):
        v = _none_if_empty(v)
        return None if v is None else str(v).strip()

    if source == "mail":
        for r in normalized:
            r["sent_date"] = to_date_or_none(r.get("sent_date"))
        rows_for_db = [dict(r, source_id=_to_str_or_none(r.get("id"))) for r in normalized]

        _set(run_id, "mail_inserting")
        count = mapper_dao.insert_normalized_mail(run_id, user_id, rows_for_db)
        log.info("normalize_from_raw: inserted %d rows into staging_mail", count)

        try:
            run_dao.update_counts(run_id, mail_count=count, mail_ready=True)
        except TypeError:
            log.debug("run_dao.update_counts signature mismatch; skipping mail_ready flag")

        _set(run_id, "mail_ready")

    else:  # source == "crm"
        for r in normalized:
            r["job_date"] = to_date_or_none(r.get("job_date") or r.get("date") or r.get("created_at"))
        rows_for_db = [dict(r, source_id=_to_str_or_none(r.get("source_id") or r.get("id"))) for r in normalized]

        _set(run_id, "crm_inserting")
        count = mapper_dao.insert_normalized_crm(run_id, user_id, rows_for_db)
        log.info("normalize_from_raw: inserted %d rows into staging_crm", count)

        try:
            run_dao.update_counts(run_id, crm_count=count, crm_ready=True)
        except TypeError:
            log.debug("run_dao.update_counts signature mismatch; skipping crm_ready flag")

        _set(run_id, "crm_ready")

    return count

# -------- Internal worker --------

def _match_and_aggregate_async(app, run_id: str) -> None:
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

            run_meta = run_dao.status(run_id) or {}
            user_id: Optional[str] = run_meta.get("user_id") or run_dao.get_user_id(run_id)

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
            _set(run_id, "done")
            log.info("matcher: done run_id=%s", run_id)

        except Exception as e:
            log.exception("matcher: failed run_id=%s: %s", run_id, e)
            run_dao.update_step(run_id, step="failed", pct=100, message=str(e))

        finally:
            stop.set()