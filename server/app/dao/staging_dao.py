# app/dao/staging_dao.py
from __future__ import annotations

from typing import Dict, List, Any, Optional

from sqlalchemy import text
from app.extensions import db


# ---------------------------
# Normalized MAIL
# ---------------------------

def fetch_normalized_mail_rows(
    run_id: str,
    user_id: Optional[str] = None,
    limit: Optional[int] = None
) -> List[Dict[str, Any]]:
    """
    Return normalized MAIL rows for a run (and optionally a user), ordered by line_no.
    NOTE: Use only in small previews/debug flows. For gating, prefer count queries.
    """
    sql = """
        SELECT run_id, user_id, source_id, line_no, address1, address2, city, state, zip, sent_date, full_address
        FROM staging_mail
        WHERE run_id = :rid
    """
    params: Dict[str, Any] = {"rid": run_id}

    if user_id:
        sql += " AND user_id = :uid"
        params["uid"] = user_id

    sql += " ORDER BY line_no"
    if limit is not None:
        sql += " LIMIT :lim"
        params["lim"] = int(limit)

    res = db.session.execute(text(sql), params)
    return [dict(row._mapping) for row in res]


def count_normalized_mail(run_id: str, user_id: Optional[str] = None) -> int:
    """
    Count normalized MAIL rows for the run (optionally scoped to user_id).
    """
    if user_id:
        sql = text("""
            SELECT COUNT(*) AS n
            FROM staging_mail
            WHERE run_id = :rid
              AND user_id = :uid
        """)
        params = {"rid": run_id, "uid": user_id}
    else:
        sql = text("""
            SELECT COUNT(*) AS n
            FROM staging_mail
            WHERE run_id = :rid
        """)
        params = {"rid": run_id}

    row = db.session.execute(sql, params).first()
    return int(row.n) if row and row.n is not None else 0


# ---------------------------
# Normalized CRM
# ---------------------------

def fetch_normalized_crm_rows(
    run_id: str,
    user_id: Optional[str] = None,
    limit: Optional[int] = None
) -> List[Dict[str, Any]]:
    sql = """
        SELECT run_id, user_id, line_no, source_id, job_index,
            address1, address2, city, state, zip,
            job_date, job_value, full_address
        FROM staging_crm
        WHERE run_id = :rid
    """
    params: Dict[str, Any] = {"rid": run_id}

    if user_id:
        sql += " AND user_id = :uid"
        params["uid"] = user_id

    sql += " ORDER BY line_no"
    if limit is not None:
        sql += " LIMIT :lim"
        params["lim"] = int(limit)

    res = db.session.execute(text(sql), params)
    return [dict(row._mapping) for row in res]


def count_normalized_crm(run_id: str, user_id: Optional[str] = None) -> int:
    """
    Count normalized CRM rows for the run (optionally scoped to user_id).
    """
    if user_id:
        sql = text("""
            SELECT COUNT(*) AS n
            FROM staging_crm
            WHERE run_id = :rid
              AND user_id = :uid
        """)
        params = {"rid": run_id, "uid": user_id}
    else:
        sql = text("""
            SELECT COUNT(*) AS n
            FROM staging_crm
            WHERE run_id = :rid
        """)
        params = {"rid": run_id}

    row = db.session.execute(sql, params).first()
    return int(row.n) if row and row.n is not None else 0