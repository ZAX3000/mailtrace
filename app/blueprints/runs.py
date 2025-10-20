# app/blueprints/runs.py
from __future__ import annotations

import io
import os
import time
import tempfile
from datetime import datetime
from typing import List

import pandas as pd
from flask import (
    Blueprint, current_app, redirect, render_template, request, session, url_for
)

from app.extensions import db, stripe
from app.models import Run, Match, Subscription
from .auth import login_required
from app.services.matching import run_matching

from app.services.geocode import geocode_addresses_plain
from app.services.map_cache import append_points

runs_bp = Blueprint("runs", __name__)

# ---------- helpers ----------
def _mail_full_address(row) -> str:
    parts = [str(row.get(c, "")).strip() for c in ("address1","address2","city","state","postal_code")]
    return " ".join([p for p in parts if p]).replace("  ", " ").strip()

def _crm_full_address(row) -> str:
    parts = [str(row.get(c, "")).strip() for c in ("address1","address2","city","state","postal_code")]
    return " ".join([p for p in parts if p]).replace("  ", " ").strip()

def _canon(df: pd.DataFrame, mapping: dict) -> pd.DataFrame:
    d = df.copy()
    d.columns = [c.lower().strip() for c in d.columns]
    for k, alts in mapping.items():
        if k in d.columns:
            continue
        for a in alts:
            if a in d.columns:
                d.rename(columns={a: k}, inplace=True)
                break
    for k in mapping.keys():
        if k not in d.columns:
            d[k] = ""
    return d

def _parse_dt(s):
    try:
        return pd.to_datetime(s, errors="coerce").date()
    except Exception:
        return None


# ---------- routes ----------
@runs_bp.get("/")
@login_required
def landing():
    return redirect(url_for("dashboard.index"))


@runs_bp.get("/runs")
@login_required
def list_runs():
    uid = session.get("user_id")
    rows = db.session.execute(
        db.select(Run).where(Run.user_id == uid).order_by(Run.started_at.desc())
    ).scalars().all()
    items = []
    for r in rows:
        items.append({
            "ts": r.started_at.strftime("%Y-%m-%d %H:%M") if r.started_at else "",
            "mail": r.mail_count or 0,
            "matches": r.match_count or 0,
            # keep this stable in case you add a run detail route later
            "url": url_for("dashboard.index", run_id=r.id) if getattr(r, "id", None) else "#",
        })
    return render_template("runs_list.html", title="Runs", items=items)


@runs_bp.get("/upload")
@login_required
def upload_form():
    return render_template("upload.html", title="Upload")


@runs_bp.get("/list")  # legacy alias
@login_required
def upload_form_alias():
    return render_template("upload.html", title="Upload")


@runs_bp.post("/upload")
@login_required
def upload():
    uid = session.get("user_id")
    mail_file = request.files.get("mail_file")
    crm_file  = request.files.get("crm_file")
    if not mail_file or not crm_file:
        return "Missing files", 400

    storage = getattr(current_app, "storage", None)
    if storage is None:
        from app.services.storage import LocalStorage
        storage = LocalStorage(os.path.join(current_app.instance_path, "uploads"))
        current_app.storage = storage  # stash for future requests

    ts = int(time.time())
    mail_key = f"{uid}/runs/{ts}/mail.csv"
    crm_key  = f"{uid}/runs/{ts}/crm.csv"
    storage.put_blob(mail_key, mail_file.stream)
    storage.put_blob(crm_key, crm_file.stream)
    mail_url = storage.url(mail_key)
    crm_url  = storage.url(crm_key)

    mail_file.stream.seek(0)
    crm_file.stream.seek(0)
    mail_df = pd.read_csv(io.BytesIO(mail_file.read()), dtype=str)
    crm_df  = pd.read_csv(io.BytesIO(crm_file.read()), dtype=str)

    matches_df = run_matching(mail_df, crm_df)

    run = Run(
        user_id=uid,
        mail_csv_url=mail_url,
        crm_csv_url=crm_url,
        mail_count=len(mail_df),
        match_count=len(matches_df),
        status="completed",
        started_at=datetime.utcnow(),
    )
    db.session.add(run)
    db.session.commit()

    rows: List[Match] = []
    for _, r in matches_df.iterrows():
        rows.append(Match(
            run_id=run.id, user_id=uid,
            crm_id=r.get("crm_id"),
            crm_job_date=pd.to_datetime(r.get("crm_job_date"), errors="coerce").date() if r.get("crm_job_date") else None,
            job_value=(float(str(r.get("job_value")).replace(",","")) if r.get("job_value") else None),
            matched_mail_full_address=r.get("matched_mail_full_address"),
            mail_dates_in_window=r.get("mail_dates_in_window"),
            mail_count_in_window=int(r.get("mail_count_in_window") or 0),
            confidence_percent=int(r.get("confidence_percent") or 0),
            match_notes=r.get("match_notes"),
            crm_city=r.get("crm_city"),
            crm_state=r.get("crm_state"),
            crm_zip=r.get("crm_zip"),
            zip5=(str(r.get("crm_zip"))[:5] if r.get("crm_zip") else None),
            state=(str(r.get("crm_state"))[:2] if r.get("crm_state") else None),
            last_mail_date=None,
        ))
    if rows:
        db.session.bulk_save_objects(rows)
        db.session.commit()

    try:
        analytics_key = f"analytics/{uid}/{run.id}.parquet"
        tmp_path = os.path.join(tempfile.gettempdir(), "_mt_matches.parquet")
        matches_df.to_parquet(tmp_path, index=False)
        with open(tmp_path, "rb") as f:
            storage.put_blob(analytics_key, f)
    except Exception as e:
        current_app.logger.error(f"Parquet export error: {e}")

    try:
        sub = db.session.execute(
            db.select(Subscription).where(Subscription.user_id == uid)
        ).scalar_one_or_none()
        qty = len(mail_df)
        if sub and getattr(sub, "metered_item_id", None) and stripe:
            stripe.UsageRecord.create(
                quantity=qty, timestamp=int(time.time()),
                subscription_item=sub.metered_item_id, action="increment"
            )
    except Exception as e:
        current_app.logger.error(f"Stripe usage error: {e}")

    def canon_mail(df):
        return _canon(df, {
            "address1": ["address1","addr1","address","street","line1"],
            "address2": ["address2","addr2","unit","line2"],
            "city": ["city"],
            "state": ["state","st"],
            "postal_code": ["postal_code","zip","zipcode","zip_code"],
            "sent_date": ["sent_date","date","mail_date"],
        })

    def canon_crm(df):
        return _canon(df, {
            "address1": ["address1","addr1","address","street","line1"],
            "address2": ["address2","addr2","unit","line2"],
            "city": ["city"],
            "state": ["state","st"],
            "postal_code": ["postal_code","zip","zipcode","zip_code"],
            "job_date": ["job_date","date","created_at"],
        })

    m = canon_mail(mail_df)
    c = canon_crm(crm_df)

    mail_addrs  = [("mail",  _mail_full_address(r), uid, run.id, _parse_dt(r.get("sent_date"))) for _, r in m.head(200).iterrows()]
    crm_addrs   = [("crm",   _crm_full_address(r),  uid, run.id, _parse_dt(r.get("job_date")))  for _, r in c.head(200).iterrows()]
    match_addrs = [("match", r.get("matched_mail_full_address"), uid, run.id, _parse_dt(r.get("crm_job_date"))) for _, r in matches_df.head(200).iterrows()]

    points = list(geocode_addresses_plain(mail_addrs + crm_addrs + match_addrs))
    append_points(points)

    session["last_run_id"] = run.id
    return redirect(url_for("dashboard.index", run_id=run.id))