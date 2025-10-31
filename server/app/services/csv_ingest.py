from __future__ import annotations

import os
import tempfile
from typing import Optional

from app.extensions import db
from app.dao import staging_mail, staging_crm


def _tmp_copy_from_stream(file_stream, suffix: str = ".csv") -> str:
    """Persist an uploaded stream to a temp file so we can use COPY FROM path."""
    fd, tmp_path = tempfile.mkstemp(suffix=suffix)
    try:
        with os.fdopen(fd, "wb") as f:
            while True:
                chunk = file_stream.read(1024 * 1024)
                if not chunk:
                    break
                f.write(chunk)
    finally:
        try:
            file_stream.seek(0)
        except Exception:
            pass
    return tmp_path


def ingest_csv_to_staging(kind: str, run_id: str, file_stream, mapping_json: Optional[dict] = None) -> int:
    """
    Streams an uploaded CSV into staging.<kind> using your existing staging DAO.
    Returns the number of attempted rows (as reported by the DAO).
    """
    engine = db.get_engine()

    # Ensure staging tables exist
    if kind == "mail":
        staging_mail.ensure_staging_mail(engine)
    elif kind == "crm":
        staging_crm.ensure_staging_crm(engine)
    else:
        raise ValueError(f"Unknown kind: {kind}")

    # Save to temp path for COPY
    tmp_path = _tmp_copy_from_stream(file_stream)

    try:
        if kind == "mail":
            attempted = staging_mail.copy_mail_csv_path(
                engine, tmp_path, run_id, truncate=False, mapping=mapping_json
            )
        else:
            attempted = staging_crm.copy_crm_csv_path(
                engine, tmp_path, run_id, truncate=False, mapping=mapping_json
            )
    finally:
        try:
            os.remove(tmp_path)
        except Exception:
            pass

    return int(attempted or 0)