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


def _ensure_defaults(row: Dict[str, Any]) -> None:
    """Normalize/defend expected keys for INSERT/UPSERT payload."""
    # required ids
    row.setdefault("crm_line_no", None)
    row.setdefault("job_index", None)

    # dates / money
    row.setdefault("crm_job_date", None)
    row.setdefault("job_value", None)

    # geo / addr
    row.setdefault("crm_city", "")
    row.setdefault("crm_state", "")
    row.setdefault("crm_zip", "")
    row.setdefault("zip5", "")
    row.setdefault("state", "")
    row.setdefault("mail_full_address", "")
    row.setdefault("crm_full_address", "")

    # arrays
    row.setdefault("mail_ids", [])
    row.setdefault("matched_mail_dates", [])

    # scoring / notes
    row.setdefault("confidence_percent", 0)
    row.setdefault("match_notes", "")


# -----------------------
# Delete for a run
# -----------------------

def delete_for_run(run_id: str, user_id: str) -> int:
    """
    Hard-delete all rows for (run_id, user_id) to make the next bulk insert idempotent.
    NOTE: We now upsert on (user_id, job_index), so old rows from prior runs for the same
    (user_id, job_index) will be overwritten even if they belong to a different run_id.
    """
    stmt = text("""
        DELETE FROM matches
         WHERE run_id = :run_id
           AND user_id = :user_id
    """)
    res = db.session.execute(stmt, {"run_id": run_id, "user_id": user_id})
    db.session.commit()
    try:
        cr = cast(CursorResult[Any], res)
        return int(getattr(cr, "rowcount", 0) or 0)
    except Exception:
        return 0


# -----------------------
# Bulk insert (UPSERT on (user_id, job_index))
# -----------------------

def bulk_insert(run_id: str, user_id: str, rows: Iterable[Dict]) -> int:
    """
    Insert/Upsert match rows in bulk.

    We enforce uniqueness at the DB level with (user_id, job_index).
    If the same job_index appears again for the same user on a new run,
    we UPDATE the existing row with the latest values (arrays included).
    """
    rows_list: List[Dict] = list(rows)
    if not rows_list:
        return 0

    for r in rows_list:
        r["run_id"] = run_id
        r["user_id"] = user_id
        _ensure_defaults(r)

    # Column order must match VALUES placeholders.
    # IMPORTANT: keep mail_full_address and crm_full_address in the right order.
    insert_sql = text("""
        INSERT INTO matches (
            run_id,
            user_id,
            crm_line_no,
            job_index,

            crm_job_date,
            job_value,

            crm_city,
            crm_state,
            crm_zip,

            mail_full_address,
            crm_full_address,

            mail_ids,
            matched_mail_dates,

            confidence_percent,
            match_notes,

            zip5,
            state
        )
        VALUES (
            :run_id,
            :user_id,
            :crm_line_no,
            :job_index,

            :crm_job_date,
            :job_value,

            :crm_city,
            :crm_state,
            :crm_zip,

            :mail_full_address,
            :crm_full_address,

            :mail_ids,
            :matched_mail_dates,

            :confidence_percent,
            :match_notes,

            :zip5,
            :state
        )
        ON CONFLICT (user_id, job_index) DO UPDATE
        SET
            run_id             = EXCLUDED.run_id,
            crm_line_no        = EXCLUDED.crm_line_no,
            crm_job_date       = EXCLUDED.crm_job_date,
            job_value          = EXCLUDED.job_value,
            crm_city           = EXCLUDED.crm_city,
            crm_state          = EXCLUDED.crm_state,
            crm_zip            = EXCLUDED.crm_zip,
            mail_full_address  = EXCLUDED.mail_full_address,
            crm_full_address   = EXCLUDED.crm_full_address,
            mail_ids           = EXCLUDED.mail_ids,
            matched_mail_dates = EXCLUDED.matched_mail_dates,
            confidence_percent = EXCLUDED.confidence_percent,
            match_notes        = EXCLUDED.match_notes,
            zip5               = EXCLUDED.zip5,
            state              = EXCLUDED.state
    """)

    total_inserted = 0
    try:
        for chunk in _chunks(rows_list, size=1000):
            db.session.execute(insert_sql, chunk)  # executemany
            total_inserted += len(chunk)
        db.session.commit()
    except SQLAlchemyError:
        db.session.rollback()
        raise

    return total_inserted


# -----------------------
# Read
# -----------------------

def fetch_for_run(run_id: str, user_id: Optional[str] = None) -> List[Dict]:
    """
    Return persisted matches for a run. If user_id is provided, also filter by it.
    Matches the columns written by bulk_insert().
    """
    base_sql = """
        SELECT
            run_id,
            user_id,
            crm_line_no,
            job_index,
            crm_job_date,
            job_value,
            crm_city,
            crm_state,
            crm_zip,
            mail_full_address,
            crm_full_address,
            mail_ids,
            matched_mail_dates,
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

    base_sql += " ORDER BY crm_line_no ASC"

    res = db.session.execute(text(base_sql), params)
    try:
        return [dict(r) for r in res.mappings().all()]
    except AttributeError:
        return [dict(r) for r in res.fetchall()]