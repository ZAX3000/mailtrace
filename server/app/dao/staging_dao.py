# app/dao/staging_dao.py
from __future__ import annotations

from typing import Dict, List, Any, Optional

from sqlalchemy import text
from app.extensions import db


def fetch_normalized_mail_rows(run_id: str, limit: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    Return normalized MAIL rows for a run, ordered by line_no.
    NOTE: Use only in small previews/debug flows. For gating, prefer count queries.
    """
    sql = text(
        """
        SELECT run_id, line_no, id, address1, address2, city, state, zip, sent_date
        FROM staging_mail
        WHERE run_id = :rid
        ORDER BY line_no
        """
        + (" LIMIT :lim" if limit else "")
    )
    params = {"rid": run_id}
    if limit:
        params["lim"] = int(limit)
    res = db.session.execute(sql, params)
    return [dict(row._mapping) for row in res]


def fetch_normalized_crm_rows(run_id: str, limit: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    Return normalized CRM rows for a run, ordered by line_no.
    NOTE: Use only in small previews/debug flows. For gating, prefer count queries.
    """
    sql = text(
        """
        SELECT run_id, line_no, crm_id, address1, address2, city, state, zip, job_date, job_value
        FROM staging_crm
        WHERE run_id = :rid
        ORDER BY line_no
        """
        + (" LIMIT :lim" if limit else "")
    )
    params = {"rid": run_id}
    if limit:
        params["lim"] = int(limit)
    res = db.session.execute(sql, params)
    return [dict(row._mapping) for row in res]


# Optional tiny helpers if you want cheap gates here (you already have count_norm in mapper_dao):
def count_normalized_mail(run_id: str) -> int:
    n = db.session.execute(
        text("SELECT COUNT(*) FROM staging_mail WHERE run_id = :rid"),
        {"rid": run_id},
    ).scalar()
    return int(n or 0)


def count_normalized_crm(run_id: str) -> int:
    n = db.session.execute(
        text("SELECT COUNT(*) FROM staging_crm WHERE run_id = :rid"),
        {"rid": run_id},
    ).scalar()
    return int(n or 0)
