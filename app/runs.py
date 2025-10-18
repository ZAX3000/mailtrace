from flask import Blueprint, request, session, redirect, url_for, current_app, render_template, render_template_string
try:
    render_template
except NameError:
    # Fallback in ultra-minimal environments
    from flask import render_template_string as render_template
import io, time, hashlib, requests, tempfile, os
from datetime import datetime
import pandas as pd

from .extensions import db, s3, stripe
from .models import Run, Match, GeoPoint, Subscription
from .auth import login_required
from .matching import run_matching
# removed: 

runs_bp = Blueprint("runs", __name__)

UPLOAD_FORM = """
<h2>Upload</h2>
<form method='POST' enctype='multipart/form-data'>
  <label>Mail CSV</label><br><input type='file' name='mail_file' accept='.csv' required><br><br>
  <label>CRM CSV</label><br><input type='file' name='crm_file' accept='.csv' required><br><br>
  <button type='submit'>Run Matching</button>
</form>
"""

def _mail_full_address(row):
    parts = [str(row.get(c, "")).strip() for c in ("address1","address2","city","state","postal_code")]
    return " ".join([p for p in parts if p]).replace("  "," ").strip()

def _crm_full_address(row):
    parts = [str(row.get(c, "")).strip() for c in ("address1","address2","city","state","postal_code")]
    return " ".join([p for p in parts if p]).replace("  "," ").strip()

def geocode_mapbox(addr: str, token: str):
    if not addr or not token: return None
    try:
        import urllib.parse
        q = urllib.parse.quote(addr)
        url = f"https://api.mapbox.com/geocoding/v5/mapbox.places/{q}.json?limit=1&access_token={token}"
        r = requests.get(url, timeout=7)
        if r.status_code != 200: return None
        js = r.json()
        feats = js.get("features", [])
        if not feats: return None
        lon, lat = feats[0]["center"]
        return lat, lon
    except Exception:
        return None

@runs_bp.get("/upload")
@login_required
def list_runs():
    uid = session.get("user_id")
    rows = db.session.execute(db.select(Run).where(Run.user_id==uid).order_by(Run.started_at.desc())).scalars().all()
    items = []
    for r in rows:
        items.append({
            'ts': r.started_at.strftime('%Y-%m-%d %H:%M') if r.started_at else '',
            'mail': r.mail_count or 0,
            'matches': r.match_count or 0,
            'url': url_for('dashboard.run_detail', run_id=r.id)
        })
    return render_template('runs_list.html', title='Runs', items=items)

@runs_bp.get("/list")
@login_required
def upload_form():
    return render_template('upload.html', title='Upload')


@runs_bp.get("/")
@login_required
def landing():
    
    return redirect(url_for('dashboard.index'))

def upload():
    uid = session.get("user_id")
    mail_file = request.files.get("mail_file")
    crm_file = request.files.get("crm_file")
    if not mail_file or not crm_file:
        return "Missing files", 400

    bucket = (current_app.config.get("AZURE_STORAGE_CONTAINER") or current_app.config.get("AWS_S3_BUCKET"))
    s3c = s3()

    ts = int(time.time())
    mail_key = f"{uid}/runs/{ts}/mail.csv"
    crm_key  = f"{uid}/runs/{ts}/crm.csv"
    s3c.upload_fileobj(mail_file.stream, bucket, mail_key)
    s3c.upload_fileobj(crm_file.stream, bucket, crm_key)

    mail_url = f"s3://{bucket}/{mail_key}"
    crm_url  = f"s3://{bucket}/{crm_key}"

    mail_file.stream.seek(0); crm_file.stream.seek(0)
    mail_df = pd.read_csv(io.BytesIO(mail_file.read()), dtype=str)
    crm_df  = pd.read_csv(io.BytesIO(crm_file.read()), dtype=str)

    matches_df = run_matching(mail_df, crm_df)

    run = Run(user_id=uid, mail_csv_url=mail_url, crm_csv_url=crm_url,
              mail_count=len(mail_df), match_count=len(matches_df), status="completed")
    db.session.add(run); db.session.commit()


    # Write Parquet to S3 (analytics lake)
    try:
        analytics_key = f"analytics/{uid}/{run.id}.parquet"
        tmp_path = os.path.join(tempfile.gettempdir(), "_mt_matches.parquet")
        matches_df.to_parquet(tmp_path, index=False)
        s3c.upload_file(tmp_path, bucket, analytics_key)
    except Exception as e:
        current_app.logger.error(f"Parquet export error: {e}")

    # Persist matches
    rows = []
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
            crm_city=r.get("crm_city"), crm_state=r.get("crm_state"), crm_zip=r.get("crm_zip"),
            zip5=(str(r.get("crm_zip"))[:5] if r.get("crm_zip") else None),
            state=(str(r.get("crm_state"))[:2] if r.get("crm_state") else None),
            last_mail_date=None
        ))
    if rows:
        db.session.bulk_save_objects(rows); db.session.commit()

    # Stripe metered usage report
    sub = db.session.execute(db.select(Subscription).where(Subscription.user_id==uid)).scalar_one_or_none()
    try:
        qty = len(mail_df)
        if sub and sub.metered_item_id:
            stripe.UsageRecord.create(quantity=qty, timestamp=int(time.time()),
                                      subscription_item=sub.metered_item_id, action="increment")
    except Exception as e:
        current_app.logger.error(f"Stripe usage error: {e}")

    # Geocode (best-effort; limited to first 200 each)
    token = current_app.config.get("MAPBOX_TOKEN", "")
    # Mail addresses (use columns if present)
    def _canon(df, mapping):
        d = df.copy()
        d.columns = [c.lower().strip() for c in d.columns]
        for k, alts in mapping.items():
            if k in d.columns: continue
            for a in alts:
                if a in d.columns: d.rename(columns={a:k}, inplace=True); break
        for k in mapping.keys():
            if k not in d.columns: d[k] = ""
        return d

    m = _canon(mail_df, {"address1":["address1","addr1","address","street","line1"],"address2":["address2","addr2","unit","line2"],
                         "city":["city"],"state":["state","st"],"postal_code":["postal_code","zip","zipcode","zip_code"],"sent_date":["sent_date","date","mail_date"]})
    c = _canon(crm_df, {"address1":["address1","addr1","address","street","line1"],"address2":["address2","addr2","unit","line2"],
                        "city":["city"],"state":["state","st"],"postal_code":["postal_code","zip","zipcode","zip_code"],"job_date":["job_date","date","created_at"]})

    def _parse_dt(s):
        try: return pd.to_datetime(s, errors="coerce").date()
        except: return None

    # Save geocoded points
    count = 0
    for _, r in m.head(200).iterrows():
        addr = _mail_full_address(r)
        dt = _parse_dt(r.get("sent_date"))
        if addr and token:
            gl = geocode_mapbox(addr, token)
            if gl:
                gp = GeoPoint(user_id=uid, run_id=run.id, kind="mail", label="Mail", address=addr,
                              lat=gl[0], lon=gl[1], event_date=dt)
                db.session.add(gp); count += 1
    db.session.commit()

    count = 0
    for _, r in c.head(200).iterrows():
        addr = _crm_full_address(r)
        dt = _parse_dt(r.get("job_date"))
        if addr and token:
            gl = geocode_mapbox(addr, token)
            if gl:
                gp = GeoPoint(user_id=uid, run_id=run.id, kind="crm", label="CRM", address=addr,
                              lat=gl[0], lon=gl[1], event_date=dt)
                db.session.add(gp); count += 1
    db.session.commit()

    # Matches: try to geocode matched mail addresses
    for _, r in matches_df.head(200).iterrows():
        addr = r.get("matched_mail_full_address")
        dt = _parse_dt(r.get("crm_job_date"))
        if addr and token:
            gl = geocode_mapbox(addr, token)
            if gl:
                gp = GeoPoint(user_id=uid, run_id=run.id, kind="match", label="Match", address=addr,
                              lat=gl[0], lon=gl[1], event_date=dt)
                db.session.add(gp)
    db.session.commit()

    # Render run dashboard
    # Store run_id in session for convenience and redirect to server-rendered dashboard
    session['last_run_id'] = run.id
    return redirect(url_for('dashboard.index', run_id=run.id))