# app/api_jobs.py
from __future__ import annotations

from functools import wraps
from typing import Any, Callable, Dict, List, Optional, Tuple

import threading
import uuid

import pandas as pd
from flask import Blueprint, jsonify, request, session
from flask.typing import ResponseReturnValue

from .matching import run_matching
from .extensions import db
from .models import Run, Match


def dev_safe_api(f: Callable[..., Any]) -> Callable[..., ResponseReturnValue]:
    @wraps(f)
    def _inner(*args: Any, **kwargs: Any) -> ResponseReturnValue:
        try:
            return f(*args, **kwargs)
        except Exception as e:
            # Roll back any open DB transaction (best-effort)
            try:
                db.session.rollback()
            except Exception:
                pass
            # Dev-friendly: avoid 500 popup but surface warning
            return jsonify({"ok": True, "partial": True, "warn": str(e)}), 200

    return _inner


api_bp = Blueprint("api_bp", __name__, url_prefix="/api")

# In-memory job state (simple dev-mode progress tracker)
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


@api_bp.post("/match_start")
@dev_safe_api
def match_start() -> ResponseReturnValue:
    if "mail_csv" not in request.files or "crm_csv" not in request.files:
        return jsonify({"error": "Missing mail_csv or crm_csv"}), 400

    mail_bytes = request.files["mail_csv"].read()
    crm_bytes = request.files["crm_csv"].read()
    job_id = str(uuid.uuid4())
    _set_job_progress(job_id, percent=1, phase="loading", status="running")

    def worker() -> None:
        from io import BytesIO

        def _read_csv_safe(b: bytes) -> pd.DataFrame:
            try:
                return pd.read_csv(BytesIO(b))
            except Exception:
                return pd.read_csv(BytesIO(b), encoding="latin-1")

        try:
            mail_df = _read_csv_safe(mail_bytes)
            crm_df = _read_csv_safe(crm_bytes)

            _set_job_progress(job_id, percent=2, phase="canonicalize")
            # RapidFuzz-only matching; no fuzzy/mode/progress_cb params anymore
            df = run_matching(mail_df, crm_df)

            total_mail = len(mail_df)
            uniqmail = (
                mail_df[["address1", "address2", "city", "state", "postal_code"]]
                .drop_duplicates()
                .shape[0]
                if {"address1", "address2", "city", "state", "postal_code"}.issubset(
                    set(mail_df.columns)
                )
                else total_mail
            )
            total_jobs = len(crm_df)
            matches = len(df)
            match_rate = (matches / total_mail) * 100 if total_mail else 0.0
            try:
                job_value_series = (
                    pd.to_numeric(df["job_value"], errors="coerce")
                    if "job_value" in df.columns
                    else pd.Series([], dtype="float64")
                )
                match_rev = float(job_value_series.fillna(0).sum())
            except Exception:
                match_rev = 0.0
            kpis = {
                "mail": total_mail,
                "uniqmail": uniqmail,
                "crm": total_jobs,
                "matches": matches,
                "match_rate": round(match_rate, 2),
                "match_revenue": round(match_rev, 2),
            }

            # YoY graph (months aligned)
            crm_col = df["crm_job_date"] if "crm_job_date" in df.columns else pd.Series([], dtype="object")
            dt = pd.to_datetime(crm_col, errors="coerce")
            df["_y"] = dt.dt.year
            df["_m"] = dt.dt.strftime("%m-%Y")
            years = df["_y"].dropna().astype(int)
            if not years.empty:
                cur_year = int(years.max())
                prev_year = cur_year - 1
                months_cur = df.loc[df["_y"] == cur_year, "_m"].value_counts().sort_index()
                months_prev = (
                    df.loc[df["_y"] == prev_year, "_m"].value_counts().sort_index()
                )
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

            # Summary (Mail location fields)
            summary: List[Dict[str, Any]] = []
            for _, r in df.iterrows():
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

            # Top lists (Mail-based)
            try:
                top_cities = (
                    df["mail_city"].value_counts().head(10).reset_index().values.tolist()
                )
            except Exception:
                top_cities = []
            try:
                top_zips = (
                    df["mail_zip"]
                    .astype(str)
                    .str[:5]
                    .value_counts()
                    .head(10)
                    .reset_index()
                    .values
                    .tolist()
                )
            except Exception:
                top_zips = []

            result = {
                "kpis": kpis,
                "graph": graph,
                "summary": summary,
                "top_cities": top_cities,
                "top_zips": top_zips,
            }
            _set_job_progress(
                job_id, percent=100, phase="done", status="done", result=result
            )
        except Exception as e:
            _set_job_progress(
                job_id, status="error", error=str(e), phase="error", percent=100
            )

    threading.Thread(target=worker, daemon=True).start()
    return jsonify({"job_id": job_id})


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
    return jsonify(
        {
            "status": st.get("status", "running"),
            "percent": st.get("percent", 0),
            "phase": st.get("phase", ""),
        }
    )


@api_bp.get("/match_result")
@dev_safe_api
def match_result() -> ResponseReturnValue:
    job_id = request.args.get("job_id")
    if not job_id:
        return jsonify({"error": "missing job_id"}), 400
    with JOBS_LOCK:
        st = JOBS.get(job_id)
    if not st:
        return jsonify({"error": "not found"}), 404
    if st.get("status") == "done":
        return jsonify(st.get("result", {}))
    if st.get("status") == "error":
        return jsonify({"error": st.get("error", "unknown")}), 500
    return jsonify({"status": "running"}), 202


# ---- JSON endpoints for history/aggregate ----

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


@api_bp.route("/runs", methods=["GET"])
def list_runs() -> ResponseReturnValue:
    try:
        uid = session.get("user_id")

        # Normalize UUID-like values (optional)
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
                "started_at": (
                    r.started_at.isoformat() if getattr(r, "started_at", None) else None
                ),
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
        city_counts[(r.crm_city or "", r.crm_state or "")] += (
            r.mail_count_in_window or 0
        )
        zip_counts[(r.crm_zip or "")] += (r.mail_count_in_window or 0)
    top_cities = [
        {"city": c[0], "state": c[1], "count": n}
        for c, n in sorted(city_counts.items(), key=lambda x: -x[1])[:20]
    ]
    top_zips = [
        {"zip": z, "count": n}
        for z, n in sorted(zip_counts.items(), key=lambda x: -x[1])[:20]
    ]

    summary = [_row_to_dict(r) for r in rows[:2000]]

    return {
        "kpis": {
            "total_matches": total_matches,
            "total_mail": total_mail,
            "match_rate": (total_matches / total_mail) if total_mail else 0,
            "avg_confidence": avg_conf / 100 if avg_conf > 1 else avg_conf,
        },
        "graph": {"labels": labels, "values": values},
        "top_cities": top_cities,
        "top_zips": top_zips,
        "summary": summary,
    }


@api_bp.route("/aggregate", methods=["GET"])
def aggregate() -> ResponseReturnValue:
    uid = session.get("user_id")
    if not uid:
        return jsonify(_build_payload([]))
    rows = db.session.query(Match).filter(Match.user_id == uid).all()
    rows = _dedup_records(rows)
    mail_total = sum([(r.mail_count_in_window or 0) for r in rows])
    return jsonify(_build_payload(rows, mail_total=mail_total))


@api_bp.route("/run_result", methods=["GET"])
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