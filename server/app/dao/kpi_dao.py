# app/dao/kpi_dao.py
from __future__ import annotations
from typing import Any, Dict, List, Sequence, Tuple
from sqlalchemy import text
from app.extensions import db

# Whitelist identifiers to keep dynamic SQL safe
ALLOWED = {
    # staging tables
    "staging_mail": {
        "address1", "address2", "city", "state", "zip",
        "sent_date", "run_id", "user_id", "full_address",
    },
    "staging_crm": {
        "address1", "address2", "city", "state", "zip",
        "job_date", "job_value", "run_id", "user_id", "full_address",
        "job_index",
    },
    # matches schema (post-normalization + matching)
    "matches": {
        "run_id", "user_id",
        "crm_id", "crm_city", "crm_state", "crm_zip", "crm_full_address",
        "mail_full_address",
        "mail_ids", "matched_mail_dates",
        "zip5", "job_value", "crm_job_date",
        "job_index",
        # keep common filters minimal/safe; add more if you truly need them
    },
}

def _assert_ident(table: str, cols: Sequence[str]) -> None:
    if table not in ALLOWED:
        raise ValueError(f"table not allowed: {table}")
    bad = [c for c in cols if c not in ALLOWED[table]]
    if bad:
        raise ValueError(f"column(s) not allowed for {table}: {bad}")

def _distinct_tuple(cols: Sequence[str]) -> str:
    return "(" + ", ".join(cols) + ")"

def _where_clause(table: str, filters: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    _assert_ident(table, filters.keys())
    parts = [f"{k} = :{k}" for k in filters.keys()]
    sql = " WHERE " + " AND ".join(parts) if parts else ""
    return sql, filters

# ---------- Generic shapes ----------

def count_distinct(table: str, distinct_cols: Sequence[str], filters: Dict[str, Any]) -> int:
    _assert_ident(table, distinct_cols)
    where_sql, params = _where_clause(table, filters)
    tup = _distinct_tuple(distinct_cols)
    sql = f"SELECT COUNT(DISTINCT {tup}) AS n FROM {table}{where_sql}"
    row = db.session.execute(text(sql), params).first()
    return int((row and row.n) or 0)

def series_count_distinct_by_month(
    table: str,
    month_from_col: str,            # e.g., 'sent_date' or 'job_date'
    distinct_cols: Sequence[str],   # tuple to dedupe per row
    filters: Dict[str, Any],
) -> List[Dict[str, Any]]:
    _assert_ident(table, [month_from_col, *distinct_cols])
    tup = _distinct_tuple(distinct_cols)
    where_sql, params = _where_clause(table, filters)
    sql = f"""
        SELECT to_char({month_from_col}, 'YYYY-MM') AS ym,
               COUNT(DISTINCT {tup}) AS n
        FROM {table}
        {where_sql}
        GROUP BY 1
    """
    res = db.session.execute(text(sql), params)
    return [dict(row._mapping) for row in res]

# ---------- Deduped matches (one row per matched job) ----------

def fetch_deduped_matches(filters: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Returns one row per MATCHED JOB (deduped).
    Preferred identity: job_index; else crm_id; else (crm_full_address | crm_job_date).
    Exposes fields for revenue, timeline (derived from matched_mail_dates[]), and city/zip rollups.
    """
    table = "matches"
    _assert_ident(table, ["run_id"])  # typical filter key
    where_sql, params = _where_clause(table, filters)

    # Derive a scalar matched_mail_date from the array using MAX(UNNEST(...))
    sql = f"""
    WITH raw AS (
      SELECT
        COALESCE(
          job_index::text,
          crm_id::text,
          CONCAT_WS('|', crm_full_address, COALESCE(crm_job_date::text, ''))
        ) AS job_key,
        job_index,
        job_value,
        crm_job_date,
        (
          SELECT MAX(d) FROM UNNEST(m.matched_mail_dates) AS t(d)
        ) AS matched_mail_date,
        LOWER(TRIM(crm_city)) AS crm_city,
        COALESCE(zip5, crm_zip) AS zip5
      FROM matches m
      {where_sql}
    )
    SELECT job_key,
           MAX(job_index)        AS job_index,
           MAX(job_value)        AS job_value,
           MAX(crm_job_date)     AS crm_job_date,
           MAX(matched_mail_date) AS matched_mail_date,
           MAX(crm_city)         AS crm_city,
           MAX(zip5)             AS zip5
    FROM raw
    GROUP BY job_key
    """
    res = db.session.execute(text(sql), params)
    return [dict(row._mapping) for row in res]

def series_deduped_matches_by_month(filters: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Monthly count of deduped matched jobs, bucketing by crm_job_date.
    Uses job_index as primary identity when available.
    """
    where_sql, params = _where_clause("matches", filters)
    sql = f"""
    WITH d AS (
      SELECT COALESCE(
               job_index::text,
               crm_id::text,
               CONCAT_WS('|', crm_full_address, COALESCE(crm_job_date::text, ''))
             ) AS job_key,
             crm_job_date
      FROM matches
      {where_sql}
    ),
    one_per_job AS (
      SELECT job_key, MAX(crm_job_date) AS crm_job_date
      FROM d GROUP BY job_key
    )
    SELECT to_char(crm_job_date, 'YYYY-MM') AS ym, COUNT(*) AS n
    FROM one_per_job
    GROUP BY 1
    ORDER BY 1
    """
    res = db.session.execute(text(sql), params)
    return [dict(row._mapping) for row in res]

def top_from_deduped_matches(filters: Dict[str, Any], group_field: str) -> List[Dict[str, Any]]:
    """
    Top counts from deduped matches (e.g., group_field='crm_city' or 'zip5').
    """
    if group_field not in {"crm_city", "zip5"}:
        raise ValueError("group_field must be 'crm_city' or 'zip5'")
    deds = fetch_deduped_matches(filters)
    counts: Dict[str, int] = {}
    for r in deds:
        key = (r.get(group_field) or "").strip()
        if key:
            counts[key] = counts.get(key, 0) + 1
    rows = sorted(counts.items(), key=lambda kv: kv[1], reverse=True)
    if group_field == "crm_city":
        return [{"city": k, "matches": v} for k, v in rows]
    else:
        return [{"zip5": k[:5], "matches": v} for k, v in rows]