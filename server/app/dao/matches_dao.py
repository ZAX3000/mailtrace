# app/dao/matches_dao.py
from __future__ import annotations

from typing import Dict, Iterable, List, Optional, Any, cast
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine import CursorResult

from app import db


# -----------------------
# Helpers
# -----------------------

def _chunks(seq: List[Dict], size: int = 1000):
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


# -----------------------
# Delete for a run
# -----------------------

def delete_for_run(run_id: str, user_id: str) -> int:
    """
    Hard-delete all rows for (run_id, user_id) to make the next bulk insert idempotent.
    Returns the number of rows deleted (as reported by the DB).
    """
    stmt = text("""
        DELETE FROM matches
         WHERE run_id = :run_id
           AND user_id = :user_id
    """)
    res = db.session.execute(stmt, {"run_id": run_id, "user_id": user_id})
    # In SQLAlchemy 2.x, rowcount may be -1 depending on driver; still commit.
    db.session.commit()
    try:
        cr = cast(CursorResult[Any], res)
        return int(getattr(cr, "rowcount", 0) or 0)
    except Exception:
        return 0


# -----------------------
# Bulk insert
# -----------------------

def bulk_insert(run_id: str, user_id: str, rows: Iterable[Dict]) -> int:
    """
    Insert match rows in bulk. `rows` are the 'transformed' dicts from matching.persist_matches_for_run()
    Returns the count of inserted rows.
    """
    rows_list: List[Dict] = list(rows)
    if not rows_list:
        return 0

    # Attach run/user to each param dict once here so the SQL text can bind them.
    for r in rows_list:
        r["run_id"] = run_id
        r["user_id"] = user_id

        # Ensure expected keys exist even if missing (defensive)
        r.setdefault("crm_line_no", None)
        r.setdefault("mail_line_no", None)
        r.setdefault("crm_id", "")
        r.setdefault("mail_id", "")
        r.setdefault("crm_city", "")
        r.setdefault("crm_state", "")
        r.setdefault("crm_zip", "")
        r.setdefault("job_value", None)
        r.setdefault("mail_full_address", "")
        r.setdefault("crm_full_address", "")
        r.setdefault("mail_count_in_window", 0)
        r.setdefault("crm_job_date", None)
        r.setdefault("last_mail_date", None)
        r.setdefault("confidence_percent", 0)
        r.setdefault("match_notes", "")
        r.setdefault("zip5", "")
        r.setdefault("state", "")

    insert_sql = text("""
        INSERT INTO matches (
            run_id,
            user_id,
            crm_line_no,
            mail_line_no,

            crm_id,
            mail_id,

            crm_job_date,
            last_mail_date,
            job_value,

            crm_city,
            crm_state,
            crm_zip,

            mail_full_address,
            crm_full_address,
            mail_count_in_window,

            confidence_percent,
            match_notes,

            zip5,
            state
        )
        VALUES (
            :run_id,
            :user_id,
            :crm_line_no,
            :mail_line_no,

            :crm_id,
            :mail_id,

            :crm_job_date,
            :last_mail_date,
            :job_value,

            :crm_city,
            :crm_state,
            :crm_zip,

            :crm_full_address,
            :mail_full_address,
            :mail_count_in_window,

            :confidence_percent,
            :match_notes,

            :zip5,
            :state
        )
    """)

    total_inserted = 0
    try:
        for chunk in _chunks(rows_list, size=1000):
            db.session.execute(insert_sql, chunk)  # executemany with list of dicts
            total_inserted += len(chunk)
        db.session.commit()
    except SQLAlchemyError:
        # Critical: clear the failed transaction so callers can continue (e.g., update step status).
        db.session.rollback()
        raise

    return total_inserted

def fetch_for_run(run_id: str, user_id: Optional[str] = None) -> List[Dict]:
    """
    Return persisted matches for a run. If user_id is provided, also filter by it.
    The selected columns mirror those written by bulk_insert().
    """
    base_sql = """
        SELECT
            run_id,
            user_id,
            crm_line_no,
            mail_line_no,
            crm_id,
            mail_id,
            crm_job_date,
            last_mail_date,
            job_value,
            crm_city,
            crm_state,
            crm_zip,
            crm_full_address,
            mail_full_address,
            mail_count_in_window,
            confidence_percent,
            match_notes,
            zip5,
            state
        FROM matches
        WHERE run_id = :run_id
    """
    params: Dict[str, Any] = {"run_id": run_id}
    if user_id:
        base_sql += " AND user_id = :user_id"
        params["user_id"] = user_id

    # Return newest first or by crm_line_no; choose what your UI expects. Here: crm_line_no asc.
    base_sql += " ORDER BY crm_line_no ASC"

    res = db.session.execute(text(base_sql), params)
    # Use mappings() for dict-like rows in SQLAlchemy 2.x
    try:
        rows = [dict(r) for r in res.mappings().all()]
    except AttributeError:
        # Fallback for older SQLAlchemy
        rows = [dict(r) for r in res.fetchall()]
    return rows