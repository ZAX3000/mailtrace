# app/services/summary.py
from __future__ import annotations

from typing import Any, Dict, List, Optional, Callable
from datetime import date
from statistics import median

from app.dao import kpi_dao, result_dao


# ---------- small helpers ----------
def _safe_float(v: Any) -> float:
    try:
        return float(v)
    except Exception:
        return 0.0

def _pct(n: int, d: int) -> float:
    return round((n / d * 100.0) if d else 0.0, 2)

def _median_days(vals: List[int]) -> int:
    return int(median(vals)) if vals else 0


# ---------- Core compute (no persistence) ----------
def compute_payload(
    run_id: str,
    on_progress: Optional[Callable[[str, Optional[int], Optional[str]], None]] = None,
) -> Dict[str, Any]:
    def _p(label: str, pct: Optional[int] = None, msg: Optional[str] = None) -> None:
        try:
            if callable(on_progress):
                on_progress(label, pct, msg)
        except Exception:
            pass

    f = {"run_id": run_id}

    # 1) Denominators (all SQL lives in kpi_dao; service picks attributes)
    _p("kpi_fetch_denoms", 90, "Counting mailers / unique addresses / jobs")
    total_mail = kpi_dao.count_distinct(
        table="staging_mail",
        distinct_cols=["full_address", "sent_date"],
        filters=f,
    )
    unique_mail_addresses = kpi_dao.count_distinct(
        table="staging_mail",
        distinct_cols=["full_address"],
        filters=f,
    )
    total_jobs = kpi_dao.count_distinct(
        table="staging_crm",
        distinct_cols=["job_index"],
        filters=f,
    )

    # 2) Matches (already deduped to one row per job by DAO)
    _p("kpi_fetch_matches", 92, "Fetching deduped matches")
    dedup_matches = kpi_dao.fetch_deduped_matches(f)  # one row per matched job (via job_index)

    matches = len(dedup_matches)
    match_revenue = sum(_safe_float(r.get("job_value")) for r in dedup_matches)

    # Convert deltas for median days to convert (per job)
    deltas: List[int] = []
    for r in dedup_matches:
        jd = r.get("crm_job_date")
        md = r.get("matched_mail_date")  # scalar derived in SQL from matched_mail_dates[]
        if isinstance(jd, date) and isinstance(md, date):
            delta = (jd - md).days
            if delta >= 0:
                deltas.append(delta)
    median_days_to_convert = _median_days(deltas)

    # 3) Derived KPIs
    _p("kpi_compute", 94, "Computing KPI metrics")
    # Match rate is matched jobs / total jobs (jobs are deduped by job_index)
    match_rate = _pct(matches, total_jobs)
    revenue_per_mailer = round(match_revenue / total_mail, 2) if total_mail else 0.0
    avg_ticket_per_match = round(match_revenue / matches, 2) if matches else 0.0

    # 4) Graph series
    _p("series", 96, "Building monthly series")
    s_mail = kpi_dao.series_count_distinct_by_month(
        table="staging_mail",
        month_from_col="sent_date",
        distinct_cols=["full_address", "sent_date"],
        filters=f,
    )
    s_jobs = kpi_dao.series_count_distinct_by_month(
        table="staging_crm",
        month_from_col="job_date",
        distinct_cols=["job_index"],
        filters=f,
    )
    s_mat = kpi_dao.series_deduped_matches_by_month(f)

    months = sorted({*(r["ym"] for r in s_mail), *(r["ym"] for r in s_jobs), *(r["ym"] for r in s_mat)})
    m_map = {r["ym"]: int(r["n"]) for r in s_mail}
    j_map = {r["ym"]: int(r["n"]) for r in s_jobs}
    t_map = {r["ym"]: int(r["n"]) for r in s_mat}
    graph = {
        "months": months,
        "mailers": [m_map.get(m, 0) for m in months],
        "jobs":    [j_map.get(m, 0) for m in months],
        "matches": [t_map.get(m, 0) for m in months],
        # Keep interface stable; real YoY overlay can be added later if needed
        "yoy": {
            "mailers": {"months": [], "current": [], "prev": []},
            "jobs":    {"months": [], "current": [], "prev": []},
            "matches": {"months": [], "current": [], "prev": []},
        },
    }

    # 5) Tops (deduped per job, grouped by city/zip)
    _p("tops", 97, "Computing top cities/zips")
    tops_city = kpi_dao.top_from_deduped_matches(f, group_field="crm_city")
    tops_zip  = kpi_dao.top_from_deduped_matches(f, group_field="zip5")

    top_cities_rows = [
        {"city": r["city"], "matches": int(r["matches"]), "match_rate": 0.0}
        for r in tops_city
    ]
    top_zips_rows = [
        {"zip": r["zip5"], "matches": int(r["matches"])}
        for r in tops_zip
    ]

    _p("done", 100, "Summary ready")

    return {
        "kpis": {
            "total_mail": total_mail,
            "unique_mail_addresses": unique_mail_addresses,
            "total_jobs": total_jobs,
            "matches": matches,
            "match_rate": match_rate,
            "match_revenue": round(match_revenue, 2),
            "revenue_per_mailer": revenue_per_mailer,
            "avg_ticket_per_match": avg_ticket_per_match,
            "median_days_to_convert": median_days_to_convert,
        },
        "graph": graph,
        "top_cities": top_cities_rows,
        "top_zips": top_zips_rows,
        "run_id": run_id,
    }


# ---------- Optional: materialize ----------
def build_and_store(
    run_id: str,
    user_id: Optional[str] = None,
    on_progress: Optional[Callable[[str, Optional[int], Optional[str]], None]] = None,
) -> Dict[str, Any]:
    if not user_id:
        user_id = getattr(result_dao, "_resolve_user_id_for_run", lambda rid: None)(run_id) or ""
    payload = compute_payload(run_id, on_progress=on_progress)
    if hasattr(result_dao, "save_all"):
        result_dao.save_all(run_id, user_id, payload)
    return payload

def build_payload(
    run_id: str,
    on_progress: Optional[Callable[[str, Optional[int], Optional[str]], None]] = None,
) -> Dict[str, Any]:
    return compute_payload(run_id, on_progress=on_progress)
