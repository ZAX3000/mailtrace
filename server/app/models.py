# app/models.py
from __future__ import annotations

from datetime import datetime
from sqlalchemy import Index, CheckConstraint, text
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
class Match(db.Model):
    __tablename__ = "matches"

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    run_id = db.Column(UUID(as_uuid=True), db.ForeignKey("runs.id"), nullable=False)
    user_id = db.Column(UUID(as_uuid=True), db.ForeignKey("users.id"), nullable=False)

    # CRM source
    crm_id = db.Column(db.String)
    crm_job_date = db.Column(db.Date)
    job_value = db.Column(db.Numeric(12, 2))
    crm_city = db.Column(db.String)
    crm_state = db.Column(db.String(2))
    crm_zip = db.Column(db.String)

    # Mail (winner) + context window
    matched_mail_full_address = db.Column(db.Text)
    mail_dates_in_window = db.Column(db.Text)
    mail_count_in_window = db.Column(db.Integer)
    last_mail_date = db.Column(db.Date)

    # Scoring / notes
    confidence_percent = db.Column(db.Integer)
    match_notes = db.Column(db.Text)

    # Denormalized helpers
    zip5 = db.Column(db.String(5))
    state = db.Column(db.String(2))

    __table_args__ = (
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
