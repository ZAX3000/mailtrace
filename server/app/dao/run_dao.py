from __future__ import annotations

from typing import Any, Dict, Optional, Tuple
from sqlalchemy import text
from app.extensions import db

# ---- creation / retrieval ---------------------------------------------------

def create_run(user_id) -> str:
    """Always create a new run for the user. Returns run_id (UUID string)."""
    run_id = db.session.execute(text("""
        INSERT INTO runs (user_id, status, started_at)
        VALUES (:u, 'queued', NOW())
        RETURNING id::text
    """), {"u": str(user_id)}).scalar_one()
    db.session.commit()
    return run_id


def create_or_get_active_run(user_id) -> str:
    """
    At-most-one active run per user: reuse the latest non-final one, else create new.
    """
    existing = db.session.execute(text("""
        SELECT id::text
        FROM runs
        WHERE user_id = :u
          AND status NOT IN ('completed','failed')
        ORDER BY started_at DESC
        LIMIT 1
    """), {"u": str(user_id)}).scalar_one_or_none()
    if existing:
        return existing
    return create_run(user_id)

# ---- status / progress ------------------------------------------------------

def update_step(run_id: str, *, step: str, pct: int, message: str) -> None:
    sql = text("""
        UPDATE runs
        SET step = :s,
            pct = :p,
            message = :m,
            status = CASE
                       WHEN :s_cmp = 'done'   THEN 'completed'
                       WHEN :s_cmp = 'failed' THEN 'failed'
                       ELSE status
                     END,
            finished_at = CASE
                            WHEN :s_cmp IN ('done','failed') THEN NOW()
                            ELSE finished_at
                          END
        WHERE id = :id
    """)
    db.session.execute(sql, {
        "id": run_id,
        "s": step,          # assigned to runs.step (varchar)
        "s_cmp": step,      # used only in comparisons
        "p": pct,
        "m": message,
    })
    db.session.commit()


def status(run_id: str) -> Dict[str, Any]:
    row = db.session.execute(text("""
        SELECT id::text AS run_id,
               step, pct, message, status,
               started_at, finished_at
        FROM runs
        WHERE id = :id
    """), {"id": str(run_id)}).mappings().one()
    return dict(row)

# ---- file/url/count bookkeeping (optional) ----------------------------------

def update_urls(run_id: str, *, mail_url: Optional[str] = None, crm_url: Optional[str] = None) -> None:
    sets = []
    params = {"id": str(run_id)}
    if mail_url is not None:
        sets.append("mail_csv_url = :mail_url")
        params["mail_url"] = mail_url
    if crm_url is not None:
        sets.append("crm_csv_url = :crm_url")
        params["crm_url"] = crm_url
    if not sets:
        return
    db.session.execute(text(f"""
        UPDATE runs SET {", ".join(sets)} WHERE id = :id
    """), params)
    db.session.commit()


# app/dao/run_dao.py
def update_counts(
    run_id: str,
    mail_count: int | None = None,
    crm_count: int | None = None,
    mail_ready: bool | None = None,
    crm_ready: bool | None = None,
) -> None:
    sets = []
    params = {"rid": run_id}
    if mail_count is not None:
        sets.append("mail_count = :mail_count"); params["mail_count"] = mail_count
    if crm_count is not None:
        sets.append("crm_count = :crm_count"); params["crm_count"] = crm_count
    if mail_ready is not None:
        sets.append("mail_ready = :mail_ready"); params["mail_ready"] = mail_ready
    if crm_ready is not None:
        sets.append("crm_ready = :crm_ready"); params["crm_ready"] = crm_ready
    if sets:
        db.session.execute(text(f"UPDATE runs SET {', '.join(sets)} WHERE id = :rid"), params)
        db.session.commit()


def complete(run_id: str) -> None:
    update_step(run_id, step="done", pct=100, message="Done")

# ---- pairing logic (staging readiness) --------------------------------------

def pair_ready(run_id: str) -> bool:
    """
    True when the staging tables both have at least one row for this run.
    """
    ready = db.session.execute(text("""
        SELECT (SELECT COUNT(*) FROM staging.mail WHERE run_id = :id) > 0
           AND (SELECT COUNT(*) FROM staging.crm  WHERE run_id = :id) > 0
    """), {"id": str(run_id)}).scalar_one()
    return bool(ready)


def get_pair_counts(run_id: str) -> Tuple[int, int]:
    """
    Returns (mail_rows, crm_rows) for the run's staging datasets.
    """
    mail_rows = db.session.execute(text("""
        SELECT COUNT(*) FROM staging.mail WHERE run_id = :id
    """), {"id": str(run_id)}).scalar_one()
    crm_rows = db.session.execute(text("""
        SELECT COUNT(*) FROM staging.crm WHERE run_id = :id
    """), {"id": str(run_id)}).scalar_one()
    return int(mail_rows), int(crm_rows)