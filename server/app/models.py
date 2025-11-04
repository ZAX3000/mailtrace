# app/models.py
from __future__ import annotations

from datetime import datetime
import sqlalchemy as sa
from sqlalchemy import Index, CheckConstraint, text, func, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID, JSONB
from .extensions import db

# --------------------------------------------------------------------
# USERS
# --------------------------------------------------------------------
class User(db.Model):
    __tablename__ = "users"

    id = db.Column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
        nullable=False,
    )
    email = db.Column(db.String, unique=True, nullable=False)

    provider = db.Column(db.String, default="auth0")
    full_name = db.Column(db.String)
    website_url = db.Column(db.String)
    industry = db.Column(db.String)
    crm = db.Column(db.String)
    mail_provider = db.Column(db.String)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)


# --------------------------------------------------------------------
# SUBSCRIPTIONS (optional but kept since you had it)
# --------------------------------------------------------------------
class Subscription(db.Model):
    __tablename__ = "subscriptions"

    id = db.Column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
        nullable=False,
    )
    user_id = db.Column(UUID(as_uuid=True), db.ForeignKey("users.id"), nullable=False)

    stripe_customer_id = db.Column(db.String, unique=True)
    stripe_subscription_id = db.Column(db.String, unique=True)
    metered_item_id = db.Column(db.String)
    status = db.Column(db.String)
    current_period_end = db.Column(db.DateTime)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)

    __table_args__ = (
        Index("idx_subscriptions_user", "user_id"),
    )


# --------------------------------------------------------------------
# RUNS (lifecycle + progress)
# --------------------------------------------------------------------
class Run(db.Model):
    __tablename__ = "runs"

    id = db.Column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
        nullable=False,
    )
    user_id = db.Column(UUID(as_uuid=True), db.ForeignKey("users.id"), nullable=False)

    # Progress + state
    status = db.Column(db.String, default="queued", nullable=False)
    step = db.Column(db.String)          # free-form step label
    pct = db.Column(db.Integer)          # 0..100
    message = db.Column(db.Text)         # human-friendly status

    # Timestamps
    started_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    finished_at = db.Column(db.DateTime)

    # Book-keeping (optional counters the pipeline may set)
    mail_count = db.Column(db.Integer)
    crm_count = db.Column(db.Integer)
    match_count = db.Column(db.Integer)

    # Whether each source has been normalized
    mail_ready = db.Column(db.Boolean, default=False, nullable=False)
    crm_ready = db.Column(db.Boolean, default=False, nullable=False)

    # Optional blob for quick UI fetches (e.g., KPI snapshots)
    artifacts = db.Column(JSONB)

    __table_args__ = (
        Index("idx_runs_user", "user_id"),
        Index("idx_runs_status", "status"),
        Index("idx_runs_started_at", "started_at"),
    )


# --------------------------------------------------------------------
# MAPPINGS (per-run, per-source)
# Unique (run_id, source); source ∈ {'mail','crm'}
# --------------------------------------------------------------------
class Mapping(db.Model):
    __tablename__ = "mappings"

    id = db.Column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
        nullable=False,
    )
    run_id = db.Column(UUID(as_uuid=True), db.ForeignKey("runs.id"), nullable=False)
    user_id = db.Column(UUID(as_uuid=True), db.ForeignKey("users.id"))

    # 'mail' or 'crm'
    source = db.Column(db.String, nullable=False)
    mapping = db.Column(JSONB, nullable=False)     # {canonical_field: "source_key" | { key, transform? }}
    created_at = db.Column(db.DateTime, server_default=text("NOW()"), nullable=False)

    __table_args__ = (
        CheckConstraint("source IN ('mail','crm')", name="ck_mappings_source"),
        db.UniqueConstraint("run_id", "source", name="uq_mappings_run_source"),
        Index("idx_mappings_run", "run_id"),
    )


# --------------------------------------------------------------------
# MATCHES (summary rows for UI/exports)
# --------------------------------------------------------------------
# app/models.py (or wherever Match lives)
import sqlalchemy as sa
from sqlalchemy import UniqueConstraint, Index
from sqlalchemy.dialects.postgresql import UUID
from app import db

class Match(db.Model):
    __tablename__ = "matches"

    id = db.Column(sa.BigInteger, primary_key=True, autoincrement=True)

    run_id  = db.Column(UUID(as_uuid=True), db.ForeignKey("runs.id"), nullable=False)
    user_id = db.Column(UUID(as_uuid=True), db.ForeignKey("users.id"), nullable=False)

    # exact pair (line numbers from staging tables)
    crm_line_no  = db.Column(sa.BigInteger, nullable=False)
    mail_line_no = db.Column(sa.BigInteger, nullable=False)

    # traceability (renamed to match DAO)
    crm_id  = db.Column(sa.Text)   # was crm_source_id
    mail_id = db.Column(sa.Text)   # was mail_source_id

    # CRM denorm
    crm_job_date = db.Column(sa.Date)
    job_value    = db.Column(sa.Numeric(12, 2))
    crm_city     = db.Column(sa.String)
    crm_state    = db.Column(sa.String(2))
    crm_zip      = db.Column(sa.String)

    # Mail context (winner + window)
    mail_full_address     = db.Column(sa.Text)
    crm_full_address      = db.Column(sa.Text)
    mail_count_in_window  = db.Column(sa.Integer)
    last_mail_date        = db.Column(sa.Date)

    # scoring/notes
    confidence_percent = db.Column(sa.Integer)
    match_notes        = db.Column(sa.Text)

    # helpers
    zip5  = db.Column(sa.String(5))
    state = db.Column(sa.String(2))

    __table_args__ = (
        # Keep your uniqueness rules
        UniqueConstraint("run_id", "crm_line_no", "mail_line_no", name="uq_match_pair"),
        UniqueConstraint("run_id", "crm_line_no", name="uq_match_one_mail_per_crm"),
        Index("idx_matches_user", "user_id"),
        Index("idx_matches_run", "run_id"),
        Index("idx_matches_user_date", "user_id", "crm_job_date"),
        Index("idx_matches_zip5", "zip5"),
        Index("idx_matches_state", "state"),
    )


# --------------------------------------------------------------------
# GEO POINTS (optional plotting cache)
# --------------------------------------------------------------------
class GeoPoint(db.Model):
    __tablename__ = "geo_points"

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    user_id = db.Column(UUID(as_uuid=True), db.ForeignKey("users.id"), nullable=False)
    run_id = db.Column(UUID(as_uuid=True), db.ForeignKey("runs.id"), nullable=False)

    source = db.Column(db.String, nullable=False)  # 'mail' | 'crm' | 'match'
    label = db.Column(db.String)
    address = db.Column(db.String)
    lat = db.Column(db.Float)
    lon = db.Column(db.Float)
    event_date = db.Column(db.Date)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)

    __table_args__ = (
        Index("idx_geo_user_kind_date", "user_id", "source", "event_date"),
        Index("idx_geo_run_kind", "run_id", "source"),
    )

# --------------------------------------------------------------------
# RUN KPIS (summary stats per run)
# --------------------------------------------------------------------
class RunKPI(db.Model):
    __tablename__ = "run_kpis"

    run_id  = db.Column(UUID(as_uuid=True), db.ForeignKey("runs.id"), primary_key=True)
    user_id = db.Column(UUID(as_uuid=True), db.ForeignKey("users.id"), nullable=False)

    # Scalar KPIs (pure columns so FE does zero parsing)
    total_mail               = db.Column(db.Integer, nullable=False, default=0)
    unique_mail_addresses    = db.Column(db.Integer, nullable=False, default=0)
    total_jobs               = db.Column(db.Integer, nullable=False, default=0)
    matches                  = db.Column(db.Integer, nullable=False, default=0)
    match_rate               = db.Column(db.Numeric(7,2), nullable=False, default=0)   # %
    match_revenue            = db.Column(db.Numeric(14,2), nullable=False, default=0)
    revenue_per_mailer       = db.Column(db.Numeric(14,2), nullable=False, default=0)
    avg_ticket_per_match     = db.Column(db.Numeric(14,2), nullable=False, default=0)
    median_days_to_convert   = db.Column(db.Integer, nullable=False, default=0)

    # Optional denorms that help filters
    first_job_date           = db.Column(db.Date)
    last_job_date            = db.Column(db.Date)

    created_at = db.Column(db.DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = db.Column(db.DateTime(timezone=True), server_default=func.now(),
                           onupdate=func.now(), nullable=False)

    __table_args__ = (
        Index("idx_run_kpis_user", "user_id"),
        Index("idx_run_kpis_dates", "first_job_date", "last_job_date"),
    )

# --------------------------------------------------------------------
# RUN SERIES (monthly aggregates per run)
# --------------------------------------------------------------------
class RunSeries(db.Model):
    __tablename__ = "run_series"

    id     = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    run_id = db.Column(UUID(as_uuid=True), db.ForeignKey("runs.id"), nullable=False)
    # series in {"mailers","jobs","matches"} or "yoy_mailers"/"yoy_jobs"/"yoy_matches" if desired
    series = db.Column(db.String(32), nullable=False)
    ym     = db.Column(db.String(7), nullable=False)  # "YYYY-MM"
    value  = db.Column(db.Integer, nullable=False, default=0)

    __table_args__ = (
        Index("idx_run_series_run", "run_id"),
        Index("idx_run_series_unique", "run_id", "series", "ym", unique=True),
    )

# --------------------------------------------------------------------
# RUN TOP CITIES / ZIPS (for breakdowns)
# --------------------------------------------------------------------
class RunTopCity(db.Model):
    __tablename__ = "run_top_cities"
    id     = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    run_id = db.Column(UUID(as_uuid=True), db.ForeignKey("runs.id"), nullable=False)
    city   = db.Column(db.String, nullable=False)  # lower-cased key already is fine
    matches = db.Column(db.Integer, nullable=False, default=0)
    match_rate = db.Column(db.Numeric(7,2), nullable=False, default=0)
    __table_args__ = (Index("idx_run_top_cities_run", "run_id"),)

class RunTopZip(db.Model):
    __tablename__ = "run_top_zips"
    id     = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    run_id = db.Column(UUID(as_uuid=True), db.ForeignKey("runs.id"), nullable=False)
    zip5   = db.Column(db.String(5), nullable=False)
    matches = db.Column(db.Integer, nullable=False, default=0)
    __table_args__ = (
        Index("idx_run_top_zips_run", "run_id"),
        Index("idx_run_top_zips_zip", "zip5"),
    )
