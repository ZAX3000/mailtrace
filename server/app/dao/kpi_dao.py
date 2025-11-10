# app/dao/kpi_dao.py
from __future__ import annotations
from typing import Any, Dict, List, Sequence, Tuple, Mapping
from sqlalchemy import text
from app.extensions import db

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
    # matches schema
    "matches": {
        "run_id", "user_id",
        "crm_city", "crm_state", "crm_zip", "crm_full_address",
        "mail_full_address",
        "mail_ids", "matched_mail_dates",
        "zip5", "job_value", "crm_job_date",
        "job_index",
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

def _where_clause(table: str, filters: Mapping[str, Any]) -> Tuple[str, Dict[str, Any]]:
    _assert_ident(table, list(filters))
    parts = [f"{k} = :{k}" for k in filters.keys()]
    sql = " WHERE " + " AND ".join(parts) if parts else ""
    return sql, dict(filters)

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
    month_from_col: str,
    distinct_cols: Sequence[str],
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

# ---------- Deduped matches ----------

def fetch_deduped_matches(filters: Dict[str, Any]) -> List[Dict[str, Any]]:
    table = "matches"
    _assert_ident(table, list(filters))
    where_sql, params = _where_clause(table, filters)

    and_or_where = " AND " if where_sql else " WHERE "
    sql = f"""
    WITH raw AS (
      SELECT
        m.job_index::text   AS job_key,
        m.job_index         AS job_index,
        m.job_value         AS job_value,
        m.crm_job_date      AS crm_job_date,
        (SELECT MAX(d) FROM UNNEST(m.matched_mail_dates) AS t(d)) AS matched_mail_date,
        LOWER(TRIM(m.crm_city)) AS crm_city,
        COALESCE(m.zip5, m.crm_zip) AS zip5
      FROM matches m
      {where_sql}{and_or_where} m.job_index IS NOT NULL
    )
    SELECT job_key                AS job_index,
           MAX(job_value)         AS job_value,
           MAX(crm_job_date)      AS crm_job_date,
           MAX(matched_mail_date) AS matched_mail_date,
           MAX(crm_city)          AS crm_city,
           MAX(zip5)              AS zip5
    FROM raw
    GROUP BY job_key
    """
    res = db.session.execute(text(sql), params)
    return [dict(row._mapping) for row in res]


def series_deduped_matches_by_month(filters: Dict[str, Any]) -> List[Dict[str, Any]]:
    where_sql, params = _where_clause("matches", filters)
    and_or_where = " AND " if where_sql else " WHERE "
    sql = f"""
    WITH d AS (
      SELECT m.job_index::text AS job_key,
             m.crm_job_date    AS crm_job_date
      FROM matches m
      {where_sql}{and_or_where} m.job_index IS NOT NULL
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