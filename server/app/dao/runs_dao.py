# app/dao/runs_dao.py
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
from sqlalchemy import text
from app.extensions import db

# ---- creation / retrieval ---------------------------------------------------

def create_run(user_id: str) -> str:
    """Always create a new run for the user. Returns run_id (UUID string)."""
    run_id = db.session.execute(
        text("""
            INSERT INTO runs (
              user_id, status, started_at,
              mail_ready, crm_ready
            )
            VALUES (:u, 'queued', NOW(), false, false)
            RETURNING id::text
        """),
        {"u": str(user_id)},
    ).scalar_one()
    db.session.commit()
    return run_id


def create_or_get_active_run(user_id: str) -> str:
    """
    At-most-one active run per user: reuse the latest non-final one, else create new.
    """
    existing = db.session.execute(
        text("""
            SELECT id::text
            FROM runs
            WHERE user_id = :u
              AND status NOT IN ('done','failed')
            ORDER BY started_at DESC
            LIMIT 1
        """),
        {"u": str(user_id)},
    ).scalar_one_or_none()
    if existing:
        return existing
    return create_run(user_id)

# ---- strict user resolver ---------------------------------------------------

def get_user_id(run_id: str) -> str:
    """
    Return user_id (as string) for a run. Raises if the run does not exist.
    """
    uid = db.session.execute(
        text("SELECT user_id::text FROM runs WHERE id = :rid"),
        {"rid": str(run_id)},
    ).scalar_one_or_none()
    if not uid:
        raise RuntimeError(f"run not found: {run_id}")
    return uid

# ---- status / progress ------------------------------------------------------

def update_step(run_id: str, *, step: str, pct: int, message: str) -> None:
    sql = text("""
        UPDATE runs
        SET step = :s,
            pct = :p,
            message = :m,
            status = CASE
                       WHEN :s_cmp = 'done'   THEN 'done'
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
        "id": str(run_id),
        "s": step,
        "s_cmp": step,
        "p": int(pct),
        "m": message or "",
    })
    db.session.commit()


def status(run_id: str) -> Optional[Dict[str, Any]]:
    """
    Return a compact snapshot for the UI. Includes user_id for downstream logic.
    None if the run is missing.
    """
    row = db.session.execute(
        text("""
            SELECT
              id::text      AS run_id,
              user_id::text AS user_id,
              step, pct, message, status,
              started_at, finished_at
            FROM runs
            WHERE id = :id
            LIMIT 1
        """),
        {"id": str(run_id)},
    ).mappings().first()
    if not row:
        return None
    return dict(row)

# ---- file/url/count bookkeeping --------------------------------------------

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
    db.session.execute(text(f"UPDATE runs SET {', '.join(sets)} WHERE id = :id"), params)
    db.session.commit()


def update_counts(
    run_id: str,
    mail_count: int | None = None,
    crm_count: int | None = None,
    mail_ready: bool | None = None,
    crm_ready: bool | None = None,
) -> None:
    sets: list[str] = []
    params: Dict[str, Any] = {"rid": str(run_id)}
    if mail_count is not None:
        sets.append("mail_count = :mail_count")
        params["mail_count"] = mail_count
    if crm_count is not None:
        sets.append("crm_count = :crm_count")
        params["crm_count"] = crm_count
    if mail_ready is not None:
        sets.append("mail_ready = :mail_ready") 
        params["mail_ready"] = mail_ready
    if crm_ready is not None:
        sets.append("crm_ready = :crm_ready")
        params["crm_ready"] = crm_ready
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
    ready = db.session.execute(
        text("""
            SELECT (SELECT COUNT(*) FROM staging_mail WHERE run_id = :id) > 0
               AND (SELECT COUNT(*) FROM staging_crm  WHERE run_id = :id) > 0
        """),
        {"id": str(run_id)},
    ).scalar_one()
    return bool(ready)


def get_pair_counts(run_id: str) -> Tuple[int, int]:
    """
    Returns (mail_rows, crm_rows) for the run's staging datasets.
    """
    mail_rows = db.session.execute(
        text("SELECT COUNT(*) FROM staging_mail WHERE run_id = :id"),
        {"id": str(run_id)},
    ).scalar_one()
    crm_rows = db.session.execute(
        text("SELECT COUNT(*) FROM staging_crm WHERE run_id = :id"),
        {"id": str(run_id)},
    ).scalar_one()
    return int(mail_rows), int(crm_rows)

# ---- latest run lookups -----------------------------------------------------

def latest_for_user(user_id: str, only_done: bool = False) -> Optional[Dict[str, Any]]:
    """
    Return the most recent run for a user.
    If only_done=True, return the most recent *completed* run.
    """
    sql = """
        SELECT
          id::text      AS run_id,
          user_id::text AS user_id,
          step, pct, message, status,
          started_at, finished_at
        FROM runs
        WHERE user_id = :u
        {and_done}
        ORDER BY
          -- prefer completed runs by finished_at desc; otherwise by started_at
          (finished_at IS NULL), finished_at DESC NULLS LAST, started_at DESC
        LIMIT 1
    """
    and_done = "AND status = 'done'" if only_done else ""
    row = db.session.execute(text(sql.format(and_done=and_done)), {"u": str(user_id)}).mappings().first()
    return dict(row) if row else None

def latest_done_for_user(user_id: str) -> Optional[Dict[str, Any]]:
    """Shorthand for latest_for_user(user_id, only_done=True)."""
    return latest_for_user(user_id, only_done=True)

def list_for_user(user_id: str, *, limit: int = 25, before_run_id: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Returns recent runs for a user (newest first).
    If before_run_id is provided, returns runs older than that anchor (cursor pagination).
    Shape is compact for dropdowns.
    """
    params: Dict[str, Any] = {"u": str(user_id), "lim": int(max(1, min(limit, 100)))}

    anchor_clause = ""
    if before_run_id:
        # Find the anchor's started_at and then page older than it
        anchor = db.session.execute(
            text("SELECT started_at FROM runs WHERE id = :rid"),
            {"rid": str(before_run_id)},
        ).scalar_one_or_none()
        if anchor:
            anchor_clause = "AND started_at < :anchor"
            params["anchor"] = anchor

    rows = db.session.execute(text(f"""
        SELECT
          id::text      AS id,
          started_at,
          status,
          -- Optional short summary line for UI; keep NULL if you don't compute it server-side
          CONCAT(
            COALESCE(NULLIF(mail_count,0)::text, '0'), ' mail · ',
            COALESCE(NULLIF(crm_count,0)::text, '0'), ' crm'
          ) AS summary
        FROM runs
        WHERE user_id = :u
          {anchor_clause}
        ORDER BY started_at DESC
        LIMIT :lim
    """), params).mappings().all()

    return [dict(r) for r in rows]

def get_by_id_compact(run_id: str) -> Optional[Dict[str, Any]]:
    """
    Compact single-run fetch (id, status, times, counts). Great for detail hover tooltips.
    """
    row = db.session.execute(text("""
        SELECT
          id::text      AS id,
          user_id::text AS user_id,
          status, step, pct, message,
          started_at, finished_at,
          mail_count, crm_count, mail_ready, crm_ready
        FROM runs
        WHERE id = :rid
        LIMIT 1
    """), {"rid": str(run_id)}).mappings().first()
    return dict(row) if row else None