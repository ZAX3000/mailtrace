# app/dao/matches.py
from __future__ import annotations

from datetime import datetime
from typing import Any, Mapping, Optional, Sequence

from sqlalchemy import insert
from sqlalchemy.orm import Session

from app.models import Match, Run


def create_run(session: Session, *, user_id: Optional[Any]) -> Run:
    """
    Create a Run row in 'running' state and return it (id is available via flush()).
    """
    run = Run(
        user_id=user_id,
        status="running",
        started_at=datetime.utcnow(),
    )
    session.add(run)
    session.flush()  # ensure run.id is populated
    return run


def bulk_insert_matches(session: Session, rows: Sequence[Mapping[str, Any]]) -> int:
    """
    Insert many Match rows efficiently. Accepts a sequence of read-only mappings,
    materializes into list[dict[str, Any]] to satisfy SQLAlchemy typing.
    """
    if not rows:
        return 0
    payload: list[dict[str, Any]] = [dict(r) for r in rows]
    session.execute(insert(Match), payload)
    # rely on surrounding transaction for commit
    return len(payload)


def finalize_run(
    session: Session,
    run: Run,
    *,
    mail_count: int,
    match_count: int,
    status: str = "completed",
    error: Optional[str] = None,
) -> None:
    """
    Update the Run row with final stats/status and finish timestamp.
    """
    run.mail_count = mail_count
    run.match_count = match_count
    run.status = status
    run.error = error
    run.finished_at = datetime.utcnow()
    session.add(run)
    # caller controls transaction/commit