# app/blueprints/api.py
from __future__ import annotations

import os
import threading
import uuid
from datetime import datetime
from functools import wraps
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd
from flask import Blueprint, current_app, jsonify, request, session
from flask.typing import ResponseReturnValue
from werkzeug.utils import secure_filename
from sqlalchemy import text

from app.extensions import db
from app.models import Match, Run
from app.services.matching import run_matching
from app.dao.staging_mail import ensure_staging_mail, copy_mail_csv_path, count_mail
from app.dao.staging_crm import ensure_staging_crm, copy_crm_csv_path, count_crm

api_bp = Blueprint("api_bp", __name__, url_prefix="/api")

# ---------------------------------------------------------------------------
# Common decorator (dev-friendly rollback + JSON warning) - move to utils ASAP
# ---------------------------------------------------------------------------
def dev_safe_api(f: Callable[..., Any]) -> Callable[..., ResponseReturnValue]:
    @wraps(f)
    def _inner(*args: Any, **kwargs: Any) -> ResponseReturnValue:
        try:
            return f(*args, **kwargs)
        except Exception as e:
            try:
                db.session.rollback()
            except Exception:
                pass
            # Keep current behavior: warn instead of 500, but DO NOT swallow explicit 4xx we return
            return jsonify({"ok": True, "partial": True, "warn": str(e)}), 200
    return _inner

# ---------------------------------------------------------------------------
# Background job bookkeeping (in-memory) - move to services/DB ASAP
# ---------------------------------------------------------------------------
JOBS: Dict[str, Dict[str, Any]] = {}
JOBS_LOCK = threading.Lock()

def _set_job_progress(
    job_id: str,
    *,
    percent: Optional[int] = None,
    phase: Optional[str] = None,
    status: Optional[str] = None,
    result: Optional[Dict[str, Any]] = None,
    error: Optional[str] = None,
) -> None:
    with JOBS_LOCK:
        st = JOBS.get(job_id, {"status": "running", "percent": 0, "phase": "loading"})
        if percent is not None:
            st["percent"] = int(percent)
        if phase is not None:
            st["phase"] = str(phase)
        if status is not None:
            st["status"] = status
        if result is not None:
            st["result"] = result
        if error is not None:
            st["error"] = error
        JOBS[job_id] = st

def _any_job_running() -> bool:
    with JOBS_LOCK:
        return any(st.get("status") in ("running",) for st in JOBS.values())

def _current_running_job_id() -> Optional[str]:
    with JOBS_LOCK:
        for jid, st in JOBS.items():
            if st.get("status") == "running":
                return jid
    return None

# ---------------------------------------------------------------------------
# Ingest endpoints (now guarded to avoid UI 500s during active matching)
# ---------------------------------------------------------------------------
@api_bp.post("/ingest-mail")
@dev_safe_api
def ingest_mail() -> ResponseReturnValue:
    # Block ingest while a job is running to avoid race/500
    if _any_job_running():
        return jsonify({"error": "job_running", "job_id": _current_running_job_id()}), 409

    file = request.files.get("file")
    if not file:
        return jsonify({"error": "file is required"}), 400

    tmp_dir = os.path.join(current_app.instance_path, "tmp")
    os.makedirs(tmp_dir, exist_ok=True)
    tmp_path = os.path.join(tmp_dir, secure_filename(file.filename) or "mail.csv")
    file.save(tmp_path)

    engine = db.engine
    ensure_staging_mail(engine)
    truncate = request.args.get("truncate") in ("1", "true", "True")
    attempted = copy_mail_csv_path(engine, tmp_path, truncate=truncate)
    total = count_mail(engine)
    return jsonify({"ok": True, "rows_attempted": attempted, "staging_mail_total": total}), 200


@api_bp.post("/ingest-crm")
@dev_safe_api
def ingest_crm() -> ResponseReturnValue:
    # Block ingest while a job is running to avoid race/500
    if _any_job_running():
        return jsonify({"error": "job_running", "job_id": _current_running_job_id()}), 409

    file = request.files.get("file")
    if not file:
        return jsonify({"error": "file is required"}), 400

    tmp_dir = os.path.join(current_app.instance_path, "tmp")
    os.makedirs(tmp_dir, exist_ok=True)
    tmp_path = os.path.join(tmp_dir, secure_filename(file.filename) or "crm.csv")
    file.save(tmp_path)

    engine = db.engine
    ensure_staging_crm(engine)
    truncate = request.args.get("truncate") in ("1", "true", "True")
    attempted = copy_crm_csv_path(engine, tmp_path, truncate=truncate)
    total = count_crm(engine)
    return jsonify({"ok": True, "rows_attempted": attempted, "staging_crm_total": total}), 200

# ---------------------------------------------------------------------------
# DB loaders -> DataFrames
# ---------------------------------------------------------------------------
def _load_mail_df_from_db(engine) -> pd.DataFrame:
    sql = """
        SELECT
            COALESCE(id,'')                  AS id,
            COALESCE(address1,'')            AS address1,
            COALESCE(address2,'')            AS address2,
            COALESCE(city,'')                AS city,
            COALESCE(state,'')               AS state,
            COALESCE(postal_code,'')         AS postal_code,
            to_char(sent_date, 'YYYY-MM-DD') AS sent_date
        FROM staging.mail
    """
    return pd.read_sql_query(sql, engine)

def _load_crm_df_from_db(engine) -> pd.DataFrame:
    sql = """
        SELECT
            COALESCE(crm_id,'')              AS crm_id,
            COALESCE(address1,'')            AS address1,
            COALESCE(address2,'')            AS address2,
            COALESCE(city,'')                AS city,
            COALESCE(state,'')               AS state,
            COALESCE(postal_code,'')         AS postal_code,
            to_char(job_date, 'YYYY-MM-DD')  AS job_date,
            job_value
        FROM staging.crm
    """
    return pd.read_sql_query(sql, engine)

# ---------------------------------------------------------------------------
# Persist matches with de-dupe (append-only)
# ---------------------------------------------------------------------------
def _persist_matches(df: pd.DataFrame, user_id: Optional[uuid.UUID], run: Run) -> Tuple[int, int]:
    """
    Insert rows from matcher DataFrame into matches, skipping ones that already exist
    for (user_id, crm_job_date, matched_mail_full_address).
    Returns (inserted_count, skipped_existing).
    """
    if df.empty:
        return 0, 0

    def _norm(s: Any) -> str:
        return ("" if s is None else str(s)).strip()

    # Build keys to check existing
    incoming_keys: List[Tuple[str, str, str]] = []
    for _, r in df.iterrows():
        incoming_keys.append((
            str(user_id) if user_id else "",
            _norm(r.get("crm_job_date")),
            _norm(r.get("matched_mail_full_address")),
        ))

    existing: set[Tuple[str, str, str]] = set()
    CHUNK = 1000
    for i in range(0, len(incoming_keys), CHUNK):
        chunk = incoming_keys[i:i+CHUNK]
        if not chunk:
            continue
        ors = []
        params: Dict[str, Any] = {}
        for j, (uid_s, d, addr) in enumerate(chunk):
            ors.append(f"(user_id = :uid{j} AND crm_job_date = :d{j} AND matched_mail_full_address = :a{j})")
            params[f"uid{j}"] = uuid.UUID(uid_s) if uid_s else None
            try:
                parsed = pd.to_datetime(d, errors="coerce")
                params[f"d{j}"] = None if pd.isna(parsed) else parsed.date()
            except Exception:
                params[f"d{j}"] = None
            params[f"a{j}"] = addr
        where = " OR ".join(ors) if ors else "FALSE"
        sql = text(f"""
            SELECT user_id::text, crm_job_date::text, matched_mail_full_address
            FROM matches WHERE {where}
        """)
        rows = db.session.execute(sql, params).fetchall()
        for u, d, a in rows:
            existing.add((u or "", d or "", a or ""))

    inserted = 0
    skipped = 0

    def _parse_date_str(s: str) -> Optional[datetime.date]:
        try:
            d = pd.to_datetime(s, errors="coerce")
            if pd.isna(d):
                return None
            return d.date()
        except Exception:
            return None

    for _, r in df.iterrows():
        key = (
            str(user_id) if user_id else "",
            _norm(r.get("crm_job_date")),
            _norm(r.get("matched_mail_full_address")),
        )
        if key in existing:
            skipped += 1
            continue

        m = Match(
            run_id=run.id,
            user_id=user_id,
            crm_id=_norm(r.get("crm_id")),
            crm_job_date=_parse_date_str(_norm(r.get("crm_job_date"))),
            job_value=pd.to_numeric(r.get("job_value"), errors="coerce") if "job_value" in r else None,
            matched_mail_full_address=_norm(r.get("matched_mail_full_address")),
            mail_dates_in_window=_norm(r.get("mail_dates_in_window")),
            mail_count_in_window=int(r.get("mail_count_in_window") or 0),
            confidence_percent=int(r.get("confidence_percent") or 0),
            match_notes=_norm(r.get("match_notes")),
            crm_city=_norm(r.get("crm_city")),
            crm_state=_norm(r.get("crm_state")),
            crm_zip=_norm(r.get("crm_zip")),
            zip5=_norm(r.get("mail_zip"))[:5] if "mail_zip" in r else None,
            state=_norm(r.get("mail_state"))[:2] if "mail_state" in r else None,
            last_mail_date=_parse_date_str(_norm(r.get("last_mail_date"))) if "last_mail_date" in r else None,
        )
        db.session.add(m)
        inserted += 1

    if inserted:
        db.session.commit()
    return inserted, skipped

# ---------------------------------------------------------------------------
# Match using staged data (no uploads) - move logic out to services ASAP
# ---------------------------------------------------------------------------
@api_bp.post("/match_start")
@dev_safe_api
def match_start() -> ResponseReturnValue:
    # Allow multipart/form-data submissions as long as there aren't real files attached.
    if request.files and any(f.filename for f in request.files.values()):
        return jsonify({"error": "no_files_here", "detail": "Upload files via /api/ingest-* first"}), 400

    # Block starting a new run while any is running
    if _any_job_running():
        return jsonify({"error": "job_already_running", "job_id": _current_running_job_id()}), 409

    uid = session.get("user_id")
    try:
        uid_val = uuid.UUID(uid) if isinstance(uid, str) and uid.strip() else None
    except Exception:
        uid_val = None

    app = current_app._get_current_object()
    job_id = str(uuid.uuid4())
    _set_job_progress(job_id, percent=1, phase="loading", status="running")

    def worker() -> None:
        with app.app_context():
            try:
                engine = db.engine
                mail_df = _load_mail_df_from_db(engine)
                crm_df = _load_crm_df_from_db(engine)

                total_mail = len(mail_df)
                total_jobs = len(crm_df)
                _set_job_progress(job_id, percent=2, phase="matching")

                df = run_matching(mail_df, crm_df)

                run = Run(
                    id=uuid.uuid4(),
                    user_id=uid_val,
                    mail_csv_url="staging.mail",
                    crm_csv_url="staging.crm",
                    status="running",
                    started_at=datetime.utcnow(),
                )
                db.session.add(run)
                db.session.commit()

                inserted, skipped = _persist_matches(df, uid_val, run)

                match_rate = (inserted / total_mail) * 100 if total_mail else 0.0
                try:
                    job_value_series = (
                        pd.to_numeric(df["job_value"], errors="coerce")
                        if "job_value" in df.columns else pd.Series([], dtype="float64")
                    )
                    match_revenue = float(job_value_series.fillna(0).sum())
                except Exception:
                    match_revenue = 0.0

                kpis = {
                    "mail": total_mail,
                    "crm": total_jobs,
                    "matches": inserted,
                    "skipped_existing": skipped,
                    "match_rate": round(match_rate, 2),
                    "match_revenue": round(match_revenue, 2),
                }

                crm_col = df["crm_job_date"] if "crm_job_date" in df.columns else pd.Series([], dtype="object")
                dt = pd.to_datetime(crm_col, errors="coerce")
                df["_y"] = dt.dt.year
                df["_m"] = dt.dt.strftime("%m-%Y")
                years = df["_y"].dropna().astype(int)
                if not years.empty:
                    cur_year = int(years.max())
                    prev_year = cur_year - 1
                    months_cur = df.loc[df["_y"] == cur_year, "_m"].value_counts().sort_index()
                    months_prev = df.loc[df["_y"] == prev_year, "_m"].value_counts().sort_index()
                    months_list = [f"{m:02d}-{cur_year}" for m in range(1, 13)]
                    graph = {
                        "months": months_list,
                        "matches": [int(months_cur.get(m, 0)) for m in months_list],
                        "prev_year": [
                            int(months_prev.get(m.replace(str(cur_year), str(prev_year)), 0))
                            for m in months_list
                        ],
                    }
                else:
                    graph = {"months": [], "matches": [], "prev_year": []}

                summary: List[Dict[str, Any]] = []
                for _, r in df.head(2000).iterrows():
                    summary.append(
                        {
                            "mail_address1": r.get("matched_mail_full_address", ""),
                            "mail_unit": "",
                            "crm_address1": r.get("crm_address1_original", ""),
                            "crm_unit": r.get("crm_address2_original", ""),
                            "city": r.get("mail_city", ""),
                            "state": r.get("mail_state", ""),
                            "zip": str(r.get("mail_zip", ""))[:5],
                            "mail_dates": r.get("mail_dates_in_window", ""),
                            "crm_date": r.get("crm_job_date", ""),
                            "amount": r.get("job_value", 0),
                            "confidence": r.get("confidence_percent", 0),
                            "notes": r.get("match_notes", ""),
                        }
                    )

                run.status = "completed"
                run.finished_at = datetime.utcnow()
                db.session.add(run)
                db.session.commit()

                result = {"kpis": kpis, "graph": graph, "summary": summary}
                _set_job_progress(job_id, percent=100, phase="done", status="done", result=result)

            except Exception as e:
                try:
                    db.session.rollback()
                except Exception:
                    pass
                _set_job_progress(job_id, status="error", error=str(e), phase="error", percent=100)

    threading.Thread(target=worker, daemon=True).start()
    return jsonify({"job_id": job_id})

# ---------------------------------------------------------------------------
# Progress & result polling
# ---------------------------------------------------------------------------
@api_bp.get("/match_progress")
@dev_safe_api
def match_progress() -> ResponseReturnValue:
    job_id = request.args.get("job_id")
    if not job_id:
        return jsonify({"error": "missing job_id"}), 400
    with JOBS_LOCK:
        st = JOBS.get(job_id)
    if not st:
        return jsonify({"error": "not found"}), 404
    return jsonify({
        "status": st.get("status", "running"),
        "percent": st.get("percent", 0),
        "phase": st.get("phase", ""),
    })

@api_bp.get("/match_result")
@dev_safe_api
def match_result() -> ResponseReturnValue:
    job_id = request.args.get("job_id")
    if not job_id:
        return jsonify({"error": "missing job_id"}), 400

    view = request.args.get("view", "all")   # "all" | "kpis" | "graph" | "summary"
    limit = int(request.args.get("limit", "0"))

    with JOBS_LOCK:
        st = JOBS.get(job_id)
    if not st:
        return jsonify({"error": "not found"}), 404
    if st.get("status") == "error":
        return jsonify({"error": st.get("error", "unknown")}), 500
    if st.get("status") != "done":
        return jsonify({"status": "running"}), 202

    result = st.get("result", {}) or {}

    # Optional lightweight views for CLI; default keeps existing behavior
    if view == "kpis":
        return jsonify(result.get("kpis", {}))
    if view == "graph":
        return jsonify(result.get("graph", {}))
    if view == "summary":
        rows = result.get("summary", []) or []
        if limit > 0:
            return jsonify({"summary": rows[:limit], "count": len(rows)})
        return jsonify({"summary": rows, "count": len(rows)})

    if limit > 0 and "summary" in result:
        result = dict(result)
        result["summary"] = result["summary"][:limit]
    return jsonify(result)

# ---------------------------------------------------------------------------
# Aggregates / history JSON - move to services ASAP
# ---------------------------------------------------------------------------
def _row_to_dict(m: Match) -> Dict[str, Any]:
    return dict(
        crm_id=m.crm_id,
        crm_job_date=(m.crm_job_date.strftime("%m-%d-%y") if m.crm_job_date else ""),
        job_value=float(m.job_value) if m.job_value is not None else 0.0,
        matched_mail_full_address=m.matched_mail_full_address or "",
        mail_dates_in_window=m.mail_dates_in_window or "",
        mail_count_in_window=m.mail_count_in_window or 0,
        confidence_percent=m.confidence_percent or 0,
        match_notes=m.match_notes or "",
        crm_city=m.crm_city or "",
        crm_state=m.crm_state or "",
        crm_zip=m.crm_zip or "",
    )

def _dedup_records(rows: List[Match]) -> List[Match]:
    seen: set[Tuple[str, str]] = set()
    out: List[Match] = []
    for r in rows:
        key = (
            r.matched_mail_full_address or "",
            r.crm_job_date.strftime("%Y-%m-%d") if r.crm_job_date else "",
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out

@api_bp.get("/runs")
def list_runs() -> ResponseReturnValue:
    try:
        uid = session.get("user_id")
        uid_val: Optional[Any] = None
        try:
            from uuid import UUID
            if isinstance(uid, UUID):
                uid_val = uid
            elif isinstance(uid, str) and uid.strip():
                try:
                    uid_val = UUID(uid)
                except Exception:
                    uid_val = None
        except Exception:
            uid_val = None

        q = db.session.query(Run)
        if uid_val is not None:
            q = q.filter(Run.user_id == uid_val)
        q = q.order_by(Run.started_at.desc()).limit(50)

        rows = q.all()
        runs = [
            {
                "run_id": str(r.id),
                "started_at": (r.started_at.isoformat() if getattr(r, "started_at", None) else None),
                "mail_count": getattr(r, "mail_count", 0) or 0,
                "match_count": getattr(r, "match_count", 0) or 0,
                "status": getattr(r, "status", None),
                "error": getattr(r, "error", None),
            }
            for r in rows
        ]
        return jsonify(runs), 200
    except Exception:
        try:
            db.session.rollback()
        except Exception:
            pass
        return jsonify([]), 200

def _build_payload(rows: List[Match], mail_total: int = 0) -> Dict[str, Any]:
    total_matches = len(rows)
    total_mail = mail_total or sum([(r.mail_count_in_window or 0) for r in rows])
    confs = [(r.confidence_percent or 0) for r in rows]
    avg_conf = (sum(confs) / max(1, len(confs))) if rows else 0

    from collections import defaultdict
    buckets: Dict[str, int] = defaultdict(int)
    for r in rows:
        if r.crm_job_date:
            key = r.crm_job_date.strftime("%Y-%m")
            buckets[key] += (r.mail_count_in_window or 0)
    labels = sorted(buckets.keys())
    values = [buckets[k] for k in labels]

    city_counts: Dict[Tuple[str, str], int] = defaultdict(int)
    zip_counts: Dict[str, int] = defaultdict(int)
    for r in rows:
        city_counts[(r.crm_city or "", r.crm_state or "")] += (r.mail_count_in_window or 0)
        zip_counts[(r.crm_zip or "")] += (r.mail_count_in_window or 0)
    top_cities = [
        {"city": c[0], "state": c[1], "count": n}
        for c, n in sorted(city_counts.items(), key=lambda x: -x[1])[:20]
    ]
    top_zips = [{"zip": z, "count": n} for z, n in sorted(zip_counts.items(), key=lambda x: -x[1])[:20]]

    summary = [_row_to_dict(r) for r in rows[:2000]]

    return {
        "kpis": {
            "total_matches": total_matches,
            "total_mail": total_mail,
            "match_rate": (total_matches / total_mail) if total_mail else 0,
            "avg_confidence": (avg_conf / 100) if avg_conf > 1 else avg_conf,
        },
        "graph": {"labels": labels, "values": values},
        "top_cities": top_cities,
        "top_zips": top_zips,
        "summary": summary,
    }

@api_bp.get("/aggregate")
def aggregate() -> ResponseReturnValue:
    uid = session.get("user_id")
    if not uid:
        return jsonify(_build_payload([]))
    rows = db.session.query(Match).filter(Match.user_id == uid).all()
    rows = _dedup_records(rows)
    mail_total = sum([(r.mail_count_in_window or 0) for r in rows])
    return jsonify(_build_payload(rows, mail_total=mail_total))

@api_bp.get("/run_result")
def run_result() -> ResponseReturnValue:
    uid = session.get("user_id")
    run_id = request.args.get("run_id")
    if not uid or not run_id:
        return jsonify(_build_payload([]))
    rows = (
        db.session.query(Match)
        .filter(Match.user_id == uid, Match.run_id == run_id)
        .all()
    )
    rows = _dedup_records(rows)
    mail_total = sum([(r.mail_count_in_window or 0) for r in rows])
    return jsonify(_build_payload(rows, mail_total=mail_total))