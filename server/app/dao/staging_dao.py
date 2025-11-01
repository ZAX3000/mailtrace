# app/dao/staging_dao.py
from __future__ import annotations
from typing import Any, Dict, List
from sqlalchemy import text
from app.extensions import db

def fetch_normalized_mail_rows(run_id: str) -> List[Dict[str, Any]]:
    sql = text("""
        SELECT
          COALESCE(id, '')        AS id,
          COALESCE(address1, '')  AS address1,
          COALESCE(address2, '')  AS address2,
          COALESCE(city, '')      AS city,
          COALESCE(state, '')     AS state,
          COALESCE(zip, '')       AS zip,
          sent_date
        FROM staging_mail
        WHERE run_id = :rid
    """)
    return [dict(r) for r in db.session.execute(sql, {"rid": run_id})]

def fetch_normalized_crm_rows(run_id: str) -> List[Dict[str, Any]]:
    sql = text("""
        SELECT
          COALESCE(crm_id, '')    AS crm_id,
          COALESCE(address1, '')  AS address1,
          COALESCE(address2, '')  AS address2,
          COALESCE(city, '')      AS city,
          COALESCE(state, '')     AS state,
          COALESCE(zip, '')       AS zip,
          job_date,
          job_value
        FROM staging_crm
        WHERE run_id = :rid
    """)
    return [dict(r) for r in db.session.execute(sql, {"rid": run_id})]