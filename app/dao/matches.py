from __future__ import annotations

from datetime import datetime
from typing import Iterable, Mapping, Optional
from uuid import UUID

from sqlalchemy.orm import Session

from app.models import Match, Run


def bulk_insert_matches(sess: Session, rows: Iterable[Mapping]) -> int:
    """Fast insert of match rows (dicts keyed to Match columns)."""
    rows = list(rows)
    if not rows:
        return 0
    sess.bulk_insert_mappings(Match, rows)
    return len(rows)


def create_run(sess: Session, *, user_id: Optional[UUID]) -> Run:
    r = Run(
        user_id=user_id,
        started_at=datetime.utcnow(),
        status="running",
        error=None,
        mail_count=0,
        match_count=0,
    )
    sess.add(r)
    sess.flush()  # populate r.id
    return r


def finalize_run(
    sess: Session,
    run: Run,
    *,
    mail_count: int,
    match_count: int,
    status: str = "completed",
    error: Optional[str] = None,
) -> None:
    run.mail_count = mail_count
    run.match_count = match_count
    run.status = status
    run.error = error
    run.finished_at = datetime.utcnow()
    sess.add(run)