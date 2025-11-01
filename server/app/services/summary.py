# app/services/summary.py
from __future__ import annotations

from typing import Any, Dict, Iterable, List, Tuple
from collections import defaultdict
from datetime import date, datetime

from app.dao import staging_dao
from app.services.matching import (
    run_matching,
    normalize_address1,
    parse_date_any,
)

# ---------- helpers (no pandas) ----------

def _addr_norm_key(addr1: str, city: str, state: str, zip_code: str) -> str:
    """Normalized address key (no date). Used for Unique Mail Addresses and city/zip denominators."""
    return "|".join([
        normalize_address1(addr1 or ""),
        (city or "").strip().lower(),
        (state or "").strip().lower(),
        (str(zip_code or "").strip()[:5]),
    ])

def _addr_date_key(addr1: str, city: str, state: str, zip_code: str, d: date | None) -> str:
    """Normalized address + date key. Used for Total Mail and mail-by-month counting."""
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
    if n % 2 == 1:
        return float(s[mid])
    return (s[mid - 1] + s[mid]) / 2.0

def _parse_mm_dd_yy(s: Any) -> date | None:
    if not isinstance(s, str) or not s.strip():
        return None
    try:
        parts = s.strip().split("-")
        if len(parts) == 3 and len(parts[2]) == 2:
            return datetime.strptime(s.strip(), "%m-%d-%y").date()
        return datetime.strptime(s.strip(), "%m-%d-%Y").date()
    except Exception:
        return None

# ---------- KPI & series computation ----------

def _compute_mail_uniques(
    mail_rows: Iterable[Dict[str, Any]]
) -> Tuple[int, int, Dict[str, int], Dict[str, int]]:
    """
    Returns:
      total_mail_lines (unique by address+date),
      unique_mail_addresses (unique by address w/o date),
      mailers_by_month (YYYY-MM → count of unique mail lines),
      unique_addresses_by_city (city_key → unique addresses count) for city-level match rate
    """
    seen_mail_line = set()
    seen_unique_addr = set()
    mailers_by_month: Dict[str, int] = defaultdict(int)
    unique_addr_by_city: Dict[str, set] = defaultdict(set)

    for r in mail_rows:
        addr1 = r.get("address1", "")
        city  = r.get("city", "")
        state = r.get("state", "")
        zipc  = r.get("zip", "")
        d     = parse_date_any(r.get("sent_date"))

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
    """
    'Total Jobs' excludes duplicates where two jobs occur at the same address on the same day.
    Dedupe by normalized address + job_date. Also return jobs_by_month for the graph.
    """
    seen_jobs = set()
    jobs_by_month: Dict[str, int] = defaultdict(int)

    for r in crm_rows:
        addr1 = r.get("address1", "")
        city  = r.get("city", "")
        state = r.get("state", "")
        zipc  = r.get("zip", "")
        d     = parse_date_any(r.get("job_date"))

        key = _addr_date_key(addr1, city, state, zipc, d)
        if key not in seen_jobs:
            seen_jobs.add(key)
            ym = _ym_key(d)
            if ym:
                jobs_by_month[ym] += 1

    return len(seen_jobs), dict(jobs_by_month)

def _yoy_overlay(series_by_month: Dict[str, int]) -> Dict[str, List[int]]:
    """
    Build simple YoY overlay arrays:
      months: ["YYYY-01", ... "YYYY-12"] for latest year present in the series
      current: counts for latest year
      prev:    counts for previous year aligned on months
    """
    if not series_by_month:
        return {"months": [], "current": [], "prev": []}

    years = sorted({int(k.split("-")[0]) for k in series_by_month.keys()})
    cur = years[-1]
    prv = cur - 1

    months_labels = [f"{cur}-{m:02d}" for m in range(1, 13)]
    current = [int(series_by_month.get(f"{cur}-{m:02d}", 0)) for m in range(1, 13)]
    prev    = [int(series_by_month.get(f"{prv}-{m:02d}", 0)) for m in range(1, 13)]

    return {"months": months_labels, "current": current, "prev": prev}

# ---------- Public API ----------

def build_payload(run_id: str) -> Dict[str, Any]:

    # ----- Fetch normalized rows -----
    mail_rows = staging_dao.fetch_normalized_mail_rows(run_id)
    crm_rows  = staging_dao.fetch_normalized_crm_rows(run_id)

    # ----- Matching (preserves your algorithm) -----
    matches: List[Dict[str, Any]] = run_matching(mail_rows, crm_rows)

    # ----- KPIs -----
    total_mail_lines, unique_mail_addresses, mail_by_month, unique_addr_by_city = _compute_mail_uniques(mail_rows)
    total_jobs, jobs_by_month = _compute_job_uniques(crm_rows)

    total_matches = len(matches)
    match_rate = (total_matches / unique_mail_addresses * 100.0) if unique_mail_addresses else 0.0
    match_revenue = sum(_safe_float(m.get("job_value", 0)) for m in matches)

    revenue_per_mailer   = (match_revenue / total_mail_lines) if total_mail_lines else 0.0
    avg_ticket_per_match = (match_revenue / total_matches) if total_matches else 0.0

    # Median days to convert (first mail date → job date) across matches
    convert_deltas: List[int] = []
    for m in matches:
        first_mail_str = None
        md = m.get("mail_dates_in_window", "") or ""
        if md and md != "None provided":
            first_mail_str = md.split(",")[0].strip()
        job_date_str = m.get("crm_job_date")
        d0 = _parse_mm_dd_yy(first_mail_str) if first_mail_str else None
        d1 = _parse_mm_dd_yy(job_date_str) if job_date_str else None
        if isinstance(d0, date) and isinstance(d1, date):
            convert_deltas.append((d1 - d0).days)
    median_days_to_convert = _median(convert_deltas)

    # ----- Graph series -----
    matches_by_month: Dict[str, int] = defaultdict(int)
    for m in matches:
        d = _parse_mm_dd_yy(m.get("crm_job_date"))
        ym = _ym_key(d)
        if ym:
            matches_by_month[ym] += 1

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

    # ----- Top cities & zips -----
    city_counts: Dict[str, int] = defaultdict(int)
    zip_counts: Dict[str, int]  = defaultdict(int)

    for m in matches:
        city_key = (m.get("city") or m.get("mail_city") or "").strip().lower()
        if city_key:
            city_counts[city_key] += 1
        z5 = str(m.get("zip") or m.get("mail_zip") or "").strip()[:5]
        if z5:
            zip_counts[z5] += 1

    top_cities: List[Dict[str, Any]] = []
    for city_key, cnt in sorted(city_counts.items(), key=lambda kv: kv[1], reverse=True):
        denom = max(1, unique_addr_by_city.get(city_key, 0))
        rate = (cnt / denom) * 100.0
        top_cities.append({"city": city_key, "matches": int(cnt), "match_rate": round(rate, 2)})

    top_zips: List[Dict[str, Any]] = [
        {"zip": z, "matches": int(cnt)}
        for z, cnt in sorted(zip_counts.items(), key=lambda kv: kv[1], reverse=True)
    ]

    # ----- Summary table rows -----
    summary_rows: List[Dict[str, Any]] = []
    for r in matches:
        summary_rows.append({
            "mail_address1": r.get("matched_mail_full_address", ""),
            "mail_unit": "",
            "crm_address1": r.get("crm_address1_original", ""),
            "crm_unit": r.get("crm_address2_original", ""),
            "city": r.get("mail_city", "") or r.get("city", ""),
            "state": r.get("mail_state", "") or r.get("state", ""),
            "zip": (str(r.get("mail_zip", "") or r.get("zip", ""))[:5]),
            "mail_dates": r.get("mail_dates_in_window", "None provided"),
            "crm_date": r.get("crm_job_date", ""),
            "amount": _safe_float(r.get("job_value", 0)),
            "confidence": int(r.get("confidence_percent", 0)),
            "notes": r.get("match_notes", ""),
        })

    # ----- Final payload -----
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
            "median_days_to_convert": median_days_to_convert,
        },
        "graph": graph,
        "top_cities": top_cities,
        "top_zips": top_zips,
        "summary": summary_rows,
        "run_id": run_id,
    }
