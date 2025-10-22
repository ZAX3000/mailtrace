from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional
from uuid import UUID

import pandas as pd
from sqlalchemy import text
from flask import current_app

from app.extensions import db
from app.dao.matches import bulk_insert_matches, create_run, finalize_run
from app.services.matching import run_matching, parse_date_any  # reuse your helpers


def _fetch_mail_df() -> pd.DataFrame:
    # Only the columns the matcher needs
    sql = """
        SELECT
          COALESCE(id, '')              AS id,
          COALESCE(address1, '')        AS address1,
          COALESCE(address2, '')        AS address2,
          COALESCE(city, '')            AS city,
          COALESCE(state, '')           AS state,
          COALESCE(postal_code, '')     AS postal_code,
          sent_date
        FROM staging.mail
    """
    return pd.read_sql_query(sql, db.engine)


def _fetch_crm_df() -> pd.DataFrame:
    sql = """
        SELECT
          COALESCE(crm_id, '')          AS crm_id,
          COALESCE(address1, '')        AS address1,
          COALESCE(address2, '')        AS address2,
          COALESCE(city, '')            AS city,
          COALESCE(state, '')           AS state,
          COALESCE(postal_code, '')     AS postal_code,
          job_date,
          job_value
        FROM staging.crm
    """
    return pd.read_sql_query(sql, db.engine)


def _parse_mmddyy_to_date(s: Any) -> Optional[date]:
    # run_matching outputs mm-dd-yy strings for crm_job_date / mail_dates list
    if not isinstance(s, str) or not s.strip():
        return None
    try:
        # try many formats using the same helper you already have
        return parse_date_any(s)
    except Exception:
        return None


def _extract_last_mail_date(mail_dates_in_window: str) -> Optional[date]:
    """
    mail_dates_in_window is a comma-separated list of mm-dd-yy (or 'None provided').
    We’ll use the last (most recent) non-empty date.
    """
    if not mail_dates_in_window or "None provided" in mail_dates_in_window:
        return None
    parts = [p.strip() for p in mail_dates_in_window.split(",") if p.strip()]
    # they were sorted ascending when produced; take the last
    for p in reversed(parts):
        d = _parse_mmddyy_to_date(p)
        if d:
            return d
    return None


def _df_to_match_rows(df: pd.DataFrame, *, run_id: UUID, user_id: Optional[UUID]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for _, r in df.iterrows():
        crm_job_date = _parse_mmddyy_to_date(r.get("crm_job_date"))
        job_val_raw = r.get("job_value", None)
        try:
            job_val: Optional[Decimal] = Decimal(str(job_val_raw)) if job_val_raw not in ("", None) else None
        except Exception:
            job_val = None

        # zip5/state convenience from the CRM side
        crm_zip = str(r.get("crm_zip", "") or "")
        crm_state = str(r.get("crm_state", "") or "")
        zip5 = crm_zip[:5] if crm_zip else None
        state2 = crm_state[:2].upper() if crm_state else None
        last_mail_date = _extract_last_mail_date(str(r.get("mail_dates_in_window", "") or ""))

        out.append(
            dict(
                run_id=run_id,
                user_id=user_id,
                crm_id=r.get("crm_id", ""),
                crm_job_date=crm_job_date,
                job_value=job_val,
                matched_mail_full_address=r.get("matched_mail_full_address", ""),
                mail_dates_in_window=r.get("mail_dates_in_window", ""),
                mail_count_in_window=int(r.get("mail_count_in_window", 0) or 0),
                confidence_percent=int(r.get("confidence_percent", 0) or 0),
                match_notes=r.get("match_notes", "") or "",
                crm_city=r.get("crm_city", "") or "",
                crm_state=crm_state or "",
                crm_zip=crm_zip or "",
                zip5=zip5,
                state=state2,
                last_mail_date=last_mail_date,
            )
        )
    return out


def run_matching_from_staging(*, user_id: Optional[UUID]) -> Dict[str, Any]:
    """
    Pulls data from staging.mail & staging.crm, runs your pandas matcher,
    stores results in matches, updates runs, and returns the same
    dashboard JSON shape the UI expects.
    """
    with db.session.begin():
        run = create_run(db.session, user_id=user_id)

    # Read outside of the transaction (no locks needed)
    mail_df = _fetch_mail_df()
    crm_df = _fetch_crm_df()

    # Run pandas matcher (reuses your existing logic)
    df = run_matching(mail_df, crm_df)

    # Build Match rows and insert
    rows = _df_to_match_rows(df, run_id=run.id, user_id=user_id)

    with db.session.begin():
        n_inserted = bulk_insert_matches(db.session, rows)
        finalize_run(
            db.session,
            run,
            mail_count=int(len(mail_df)),
            match_count=int(n_inserted),
            status="completed",
            error=None,
        )

    # Small JSON payload (same shape you were returning from /match_start)
    total_mail = int(len(mail_df))
    total_jobs = int(len(crm_df))
    matches = int(len(df))
    match_rate = (matches / total_mail) * 100.0 if total_mail else 0.0

    try:
        job_value_series = pd.to_numeric(df.get("job_value", pd.Series([], dtype="float64")), errors="coerce")
        match_rev = float(job_value_series.fillna(0).sum())
    except Exception:
        match_rev = 0.0

    # YoY mini-graph
    crm_col = df["crm_job_date"] if "crm_job_date" in df.columns else pd.Series([], dtype="object")
    dt = pd.to_datetime(crm_col, errors="coerce")
    df["_y"] = dt.dt.year
    df["_m"] = dt.dt.strftime("%m-%Y")
    years = df["_y"].dropna().astype(int)
    if not years.empty:
        cur_year = int(years.max())
        prev_year = cur_year - 1
        months_cur = df.loc[df["_y"] == cur_year, "_m"].value_counts().sort_index()
        months_prev = df.loc[df["_y"] == prev_year, "_m"].value_counts().sort_index()
        months_list = [f"{m:02d}-{cur_year}" for m in range(1, 13)]
        graph = {
            "months": months_list,
            "matches": [int(months_cur.get(m, 0)) for m in months_list],
            "prev_year": [int(months_prev.get(m.replace(str(cur_year), str(prev_year)), 0)) for m in months_list],
        }
    else:
        graph = {"months": [], "matches": [], "prev_year": []}

    # Summary table (reuse your existing columns)
    summary: List[Dict[str, Any]] = []
    for _, r in df.iterrows():
        summary.append(
            {
                "mail_address1": r.get("matched_mail_full_address", ""),
                "mail_unit": "",
                "crm_address1": r.get("crm_address1_original", ""),
                "crm_unit": r.get("crm_address2_original", ""),
                "city": r.get("mail_city", ""),
                "state": r.get("mail_state", ""),
                "zip": str(r.get("mail_zip", ""))[:5],
                "mail_dates": r.get("mail_dates_in_window", ""),
                "crm_date": r.get("crm_job_date", ""),
                "amount": r.get("job_value", 0),
                "confidence": r.get("confidence_percent", 0),
                "notes": r.get("match_notes", ""),
            }
        )

    return {
        "kpis": {
            "mail": total_mail,
            "crm": total_jobs,
            "matches": matches,
            "match_rate": round(match_rate, 2),
            "match_revenue": round(match_rev, 2),
            "skipped_existing": 0,  # reserved if you add de-dupe later
            "run_id": str(run.id),
        },
        "graph": graph,
        "summary": summary,
        "top_cities": [],
        "top_zips": [],
    }