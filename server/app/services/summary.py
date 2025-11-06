# app/services/summary.py
from __future__ import annotations

from typing import Any, Dict, Iterable, List, Tuple, Optional, Callable, TypedDict
from collections import defaultdict
from datetime import date

from app.dao import staging_dao, matches_dao, result_dao
from app.services.matching import normalize_address1

# ---------- small helpers ----------

def _addr_norm_key(addr1: str, city: str, state: str, zip_code: str) -> str:
    return "|".join([
        normalize_address1(addr1 or ""),
        (city or "").strip().lower(),
        (state or "").strip().lower(),
        (str(zip_code or "").strip()[:5]),
    ])

def _addr_date_key(addr1: str, city: str, state: str, zip_code: str, d: date | None) -> str:
    ds = d.isoformat() if isinstance(d, date) else ""
    return _addr_norm_key(addr1, city, state, zip_code) + "|" + ds

def _ym_key(d: date | None) -> str:
    return d.strftime("%Y-%m") if isinstance(d, date) else ""

def _safe_float(v: Any) -> float:
    try:
        return float(v)
    except Exception:
        return 0.0

def _median(values: List[int]) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    n = len(s)
    mid = n // 2
    return float(s[mid]) if n % 2 == 1 else (s[mid - 1] + s[mid]) / 2.0

# ---------- KPI & series helpers (from staging) ----------

def _compute_mail_uniques(
    mail_rows: Iterable[Dict[str, Any]]
) -> Tuple[int, int, Dict[str, int], Dict[str, int]]:
    seen_mail_line = set()
    seen_unique_addr = set()
    mailers_by_month: Dict[str, int] = defaultdict(int)
    unique_addr_by_city: Dict[str, set] = defaultdict(set)

    for r in mail_rows:
        addr1 = r.get("address1", "")
        city  = r.get("city", "")
        state = r.get("state", "")
        zipc  = r.get("zip", "")
        d     = r.get("sent_date")  # already a date

        line_key = _addr_date_key(addr1, city, state, zipc, d)
        if line_key not in seen_mail_line:
            seen_mail_line.add(line_key)
            ym = _ym_key(d)
            if ym:
                mailers_by_month[ym] += 1

        addr_key = _addr_norm_key(addr1, city, state, zipc)
        if addr_key not in seen_unique_addr:
            seen_unique_addr.add(addr_key)
            city_key = (city or "").strip().lower()
            unique_addr_by_city[city_key].add(addr_key)

    total_mail_lines = len(seen_mail_line)
    unique_mail_addresses = len(seen_unique_addr)
    unique_addresses_by_city = {k: len(v) for k, v in unique_addr_by_city.items()}
    return total_mail_lines, unique_mail_addresses, dict(mailers_by_month), unique_addresses_by_city

def _compute_job_uniques(
    crm_rows: Iterable[Dict[str, Any]]
) -> Tuple[int, Dict[str, int]]:
    seen_jobs = set()
    jobs_by_month: Dict[str, int] = defaultdict(int)

    for r in crm_rows:
        addr1 = r.get("address1", "")
        city  = r.get("city", "")
        state = r.get("state", "")
        zipc  = r.get("zip", "")
        d     = r.get("job_date")

        key = _addr_date_key(addr1, city, state, zipc, d)
        if key not in seen_jobs:
            seen_jobs.add(key)
            ym = _ym_key(d)
            if ym:
                jobs_by_month[ym] += 1

    return len(seen_jobs), dict(jobs_by_month)

class YoYOverlay(TypedDict):
    months: List[str]
    current: List[int]
    prev: List[int]

def _yoy_overlay(series_by_month: Dict[str, int]) -> YoYOverlay:
    if not series_by_month:
        return {"months": [], "current": [], "prev": []}
    years = sorted({int(k.split("-")[0]) for k in series_by_month.keys()})
    cur = years[-1]
    prv = cur - 1
    months_labels = [f"{cur}-{m:02d}" for m in range(1, 13)]
    current = [int(series_by_month.get(f"{cur}-{m:02d}", 0)) for m in range(1, 13)]
    prev    = [int(series_by_month.get(f"{prv}-{m:02d}", 0)) for m in range(1, 13)]
    return {"months": months_labels, "current": current, "prev": prev}

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

    # 1) Load rows
    _p("fetch", 91, "Fetching normalized + matches")
    mail_rows = staging_dao.fetch_normalized_mail_rows(run_id)
    crm_rows  = staging_dao.fetch_normalized_crm_rows(run_id)
    match_rows = matches_dao.fetch_for_run(run_id)  # persisted matches
    _p("fetch_done", 92, f"Fetched mail={len(mail_rows)} crm={len(crm_rows)} matches={len(match_rows)}")

    # 2) Denominators from staging
    _p("kpi", 96, "Computing KPIs")
    total_mail_lines, unique_mail_addresses, mail_by_month, unique_addr_by_city = _compute_mail_uniques(mail_rows)
    total_jobs, jobs_by_month = _compute_job_uniques(crm_rows)

    # 3) KPI metrics from matches
    total_matches = len(match_rows)
    match_revenue = 0.0
    matches_by_month: Dict[str, int] = defaultdict(int)
    convert_deltas: List[int] = []

    for m in match_rows:
        match_revenue += _safe_float(m.get("job_value", 0))
        d_job = m.get("crm_job_date")
        ym = _ym_key(d_job)
        if ym:
            matches_by_month[ym] += 1

        d_mail = m.get("last_mail_date")
        if isinstance(d_mail, date) and isinstance(d_job, date):
            convert_deltas.append((d_job - d_mail).days)

    match_rate = (total_matches / unique_mail_addresses * 100.0) if unique_mail_addresses else 0.0
    revenue_per_mailer   = (match_revenue / total_mail_lines) if total_mail_lines else 0.0
    avg_ticket_per_match = (match_revenue / total_matches) if total_matches else 0.0
    median_days_to_convert = _median(convert_deltas)

    # 4) Graph series
    all_months = sorted(set(list(mail_by_month.keys()) + list(jobs_by_month.keys()) + list(matches_by_month.keys())))
    graph = {
        "months": all_months,
        "mailers": [int(mail_by_month.get(ym, 0)) for ym in all_months],
        "jobs":    [int(jobs_by_month.get(ym, 0)) for ym in all_months],
        "matches": [int(matches_by_month.get(ym, 0)) for ym in all_months],
        "yoy": {
            "mailers": _yoy_overlay(mail_by_month),
            "jobs":    _yoy_overlay(jobs_by_month),
            "matches": _yoy_overlay(matches_by_month),
        },
    }

    # 5) Tops — from matches
    city_counts: Dict[str, int] = defaultdict(int)
    zip_counts: Dict[str, int]  = defaultdict(int)
    for m in match_rows:
        city_key = (m.get("crm_city") or "").strip().lower()
        if city_key:
            city_counts[city_key] += 1
        z5 = (m.get("zip5") or m.get("crm_zip") or "").strip()
        if z5:
            zip_counts[str(z5)[:5]] += 1

    top_cities_rows: List[Dict[str, Any]] = []
    for city_key, cnt in sorted(city_counts.items(), key=lambda kv: kv[1], reverse=True):
        denom = max(1, unique_addr_by_city.get(city_key, 0))
        rate = (cnt / denom) * 100.0
        top_cities_rows.append({"city": city_key, "matches": int(cnt), "match_rate": round(rate, 2)})

    top_zips_rows: List[Dict[str, Any]] = [{"zip": z, "matches": int(cnt)}  # <-- 'zip' to match result_dao.save_all
                                           for z, cnt in sorted(zip_counts.items(), key=lambda kv: kv[1], reverse=True)]

    # 6) Build payload object (front-end consumes this)
    return {
        "kpis": {
            "total_mail": total_mail_lines,
            "unique_mail_addresses": unique_mail_addresses,
            "total_jobs": total_jobs,
            "matches": total_matches,
            "match_rate": round(match_rate, 2),
            "match_revenue": round(match_revenue, 2),
            "revenue_per_mailer": round(revenue_per_mailer, 2),
            "avg_ticket_per_match": round(avg_ticket_per_match, 2),
            "median_days_to_convert": int(median_days_to_convert),
        },
        "graph": graph,
        "top_cities": top_cities_rows,
        "top_zips": top_zips_rows,
        "run_id": run_id,
    }

# ---------- Optional: materialize (legacy/batch only) ----------
def build_and_store(
    run_id: str,
    user_id: Optional[str] = None,
    on_progress: Optional[Callable[[str, Optional[int], Optional[str]], None]] = None,
) -> Dict[str, Any]:
    # Resolve user_id if not provided (uses helper in result_dao)
    if not user_id:
        user_id = getattr(result_dao, "_resolve_user_id_for_run", lambda rid: None)(run_id) or ""
    payload = compute_payload(run_id, on_progress=on_progress)
    # Persist via result_dao (single place for stats IO)
    if hasattr(result_dao, "save_all"):
        result_dao.save_all(run_id, user_id, payload)
    return payload

# Backward-compatible entry point (pipeline calls this)
def build_payload(
    run_id: str,
    on_progress: Optional[Callable[[str, Optional[int], Optional[str]], None]] = None,
) -> Dict[str, Any]:
    # Compute-only; no writes (safe to call from GET /result)
    return compute_payload(run_id, on_progress=on_progress)