from __future__ import annotations

from typing import Optional

from app.dao import run_dao  # new DAO you added (run_dao.py)
from app.services.csv_ingest import ingest_csv_to_staging


def start_upload_pipeline(
    user_id,
    kind: str,                  # "mail" | "crm"
    file_stream,
    filename: str,
    mapping_json: Optional[dict] = None,
) -> str:
    """
    DB-first ingestion (no Pandas). Streams CSV into staging.<kind>,
    updates run progress, and if both sides are present, marks ready for matching.

    Matching is hooked up in Step 2 (SQL matcher).
    """
    # 1) ensure we have a run
    run_id = run_dao.create_or_get_active_run(user_id)

    # 2) validating (lightweight – header/shape checks happen inside ingest)
    run_dao.update_step(run_id, step="validating", pct=5, message=f"Validating {kind} CSV")

    # 3) ingest → staging.<kind>
    run_dao.update_step(run_id, step="ingesting", pct=25, message=f"Ingesting {kind} rows")
    attempted = ingest_csv_to_staging(kind, run_id, file_stream, mapping_json)

    # 4) bookkeeping (optional counts)
    if kind == "mail":
        run_dao.update_counts(run_id, mail_count=attempted)

    # 5) progress: either wait for counterpart, or (in Step 2) run matching
    if run_dao.pair_ready(run_id):
        # Step 2 will actually run the SQL matcher here.
        # from app.services.matching_sql import run_match_for_run
        # run_dao.update_step(run_id, step="matching", pct=60, message="Computing matches")
        # res = run_match_for_run(run_id, mapping_id=None)
        # run_dao.update_step(run_id, step="finalizing", pct=90, artifacts={"match_id": str(res["match_id"])})
        # run_dao.complete(run_id)
        run_dao.update_step(
            run_id,
            step="waiting_for_counterpart",
            pct=60,
            message="Matching step will be triggered in Step 2",
        )
    else:
        run_dao.update_step(
            run_id,
            step="waiting_for_counterpart",
            pct=40,
            message="Waiting for other file",
        )

    return run_id


def get_status(run_id: str):
    return run_dao.status(run_id)


def get_result(match_id: str):
    # Implement in Step 2 via app.dao.matches.read_result(match_id)
    return {"error": "Results not available until the matcher is wired (Step 2)."}