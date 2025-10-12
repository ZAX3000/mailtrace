from .extensions import db
from sqlalchemy.dialects.postgresql import UUID
import uuid
from datetime import datetime, date
from sqlalchemy import Index

class User(db.Model):
    __tablename__ = "users"
    id = db.Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    email = db.Column(db.String, unique=True, nullable=False)
    provider = db.Column(db.String, default="auth0")
    full_name = db.Column(db.String)
    website_url = db.Column(db.String)
    industry = db.Column(db.String)
    crm = db.Column(db.String)
    mail_provider = db.Column(db.String)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class Subscription(db.Model):
    __tablename__ = "subscriptions"
    id = db.Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = db.Column(UUID(as_uuid=True), db.ForeignKey("users.id"))
    stripe_customer_id = db.Column(db.String, unique=True)
    stripe_subscription_id = db.Column(db.String, unique=True)
    metered_item_id = db.Column(db.String)  # subscription_item id for metered line
    status = db.Column(db.String)
    current_period_end = db.Column(db.DateTime)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class Run(db.Model):
    __tablename__ = "runs"
    id = db.Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = db.Column(UUID(as_uuid=True), db.ForeignKey("users.id"))
    mail_csv_url = db.Column(db.String, nullable=False)
    crm_csv_url = db.Column(db.String, nullable=False)
    started_at = db.Column(db.DateTime, default=datetime.utcnow)
    finished_at = db.Column(db.DateTime)
    mail_count = db.Column(db.Integer)
    match_count = db.Column(db.Integer)
    status = db.Column(db.String, default="completed")
    error = db.Column(db.String)

class Match(db.Model):
    __tablename__ = "matches"
    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    run_id = db.Column(UUID(as_uuid=True), db.ForeignKey("runs.id"))
    user_id = db.Column(UUID(as_uuid=True), db.ForeignKey("users.id"))
    crm_id = db.Column(db.String)
    crm_job_date = db.Column(db.Date)
    job_value = db.Column(db.Numeric(12,2))
    matched_mail_full_address = db.Column(db.Text)
    mail_dates_in_window = db.Column(db.Text)
    mail_count_in_window = db.Column(db.Integer)
    confidence_percent = db.Column(db.Integer)
    match_notes = db.Column(db.Text)
    crm_city = db.Column(db.String)
    crm_state = db.Column(db.String)
    crm_zip = db.Column(db.String)
    zip5 = db.Column(db.String(5))
    state = db.Column(db.String(2))
    last_mail_date = db.Column(db.Date)

Index("idx_matches_user", Match.user_id)
Index("idx_matches_run", Match.run_id)
Index("idx_matches_date", Match.user_id, Match.crm_job_date)

class GeoPoint(db.Model):
    __tablename__ = "geo_points"
    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    user_id = db.Column(UUID(as_uuid=True), db.ForeignKey("users.id"))
    run_id = db.Column(UUID(as_uuid=True), db.ForeignKey("runs.id"))
    kind = db.Column(db.String)  # 'mail' | 'crm' | 'match'
    label = db.Column(db.String)
    address = db.Column(db.String)
    lat = db.Column(db.Float)
    lon = db.Column(db.Float)
    event_date = db.Column(db.Date)   # mail sent date or crm job date
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

Index("idx_geo_user_kind_date", GeoPoint.user_id, GeoPoint.kind, GeoPoint.event_date)
