"""baseline schema

Revision ID: b70d1b1b1581
Revises:
Create Date: 2025-11-08 14:07:52.635394
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# ---- Revision identifiers ----
revision = "b70d1b1b1581"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    # Ensure extension for gen_random_uuid()
    op.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")

    # ---- Sequences used by composite PK staging tables ----
    op.execute("CREATE SEQUENCE IF NOT EXISTS staging_mail_line_no_seq")
    op.execute("CREATE SEQUENCE IF NOT EXISTS staging_crm_line_no_seq")

    # =========================
    # USERS
    # =========================
    op.create_table(
        "users",
        sa.Column("id", postgresql.UUID(as_uuid=True), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("email", sa.String(), nullable=False),
        sa.Column("provider", sa.String(), nullable=True),
        sa.Column("full_name", sa.String(), nullable=True),
        sa.Column("website_url", sa.String(), nullable=True),
        sa.Column("industry", sa.String(), nullable=True),
        sa.Column("crm", sa.String(), nullable=True),
        sa.Column("mail_provider", sa.String(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("email"),
    )

    # =========================
    # SUBSCRIPTIONS
    # =========================
    op.create_table(
        "subscriptions",
        sa.Column("id", postgresql.UUID(as_uuid=True), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("user_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("stripe_customer_id", sa.String(), nullable=True),
        sa.Column("stripe_subscription_id", sa.String(), nullable=True),
        sa.Column("metered_item_id", sa.String(), nullable=True),
        sa.Column("status", sa.String(), nullable=True),
        sa.Column("current_period_end", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("stripe_customer_id"),
        sa.UniqueConstraint("stripe_subscription_id"),
    )
    op.create_index("idx_subscriptions_user", "subscriptions", ["user_id"], unique=False)

    # =========================
    # RUNS
    # =========================
    op.create_table(
        "runs",
        sa.Column("id", postgresql.UUID(as_uuid=True), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("user_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("status", sa.String(), nullable=False, server_default=sa.text("'queued'")),
        sa.Column("step", sa.String(), nullable=True),
        sa.Column("pct", sa.Integer(), nullable=True),
        sa.Column("message", sa.Text(), nullable=True),
        sa.Column("started_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
        sa.Column("finished_at", sa.DateTime(), nullable=True),
        sa.Column("mail_count", sa.Integer(), nullable=True),
        sa.Column("crm_count", sa.Integer(), nullable=True),
        sa.Column("match_count", sa.Integer(), nullable=True),
        # keep NOT NULL, add server defaults to avoid NULL inserts
        sa.Column("mail_ready", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("crm_ready", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("artifacts", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("idx_runs_user", "runs", ["user_id"], unique=False)
    op.create_index("idx_runs_status", "runs", ["status"], unique=False)
    op.create_index("idx_runs_started_at", "runs", ["started_at"], unique=False)

    # =========================
    # GEO POINTS
    # =========================
    op.create_table(
        "geo_points",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("user_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("run_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("runs.id"), nullable=False),
        sa.Column("source", sa.String(), nullable=False),  # 'mail' | 'crm' | 'match'
        sa.Column("label", sa.String(), nullable=True),
        sa.Column("address", sa.String(), nullable=True),
        sa.Column("lat", sa.Float(), nullable=True),
        sa.Column("lon", sa.Float(), nullable=True),
        sa.Column("event_date", sa.Date(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("idx_geo_run_kind", "geo_points", ["run_id", "source"], unique=False)
    op.create_index("idx_geo_user_kind_date", "geo_points", ["user_id", "source", "event_date"], unique=False)

    # =========================
    # MAPPINGS
    # =========================
    op.create_table(
        "mappings",
        sa.Column("id", postgresql.UUID(as_uuid=True), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("run_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("runs.id"), nullable=False),
        sa.Column("user_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("users.id"), nullable=True),
        sa.Column("source", sa.String(), nullable=False),
        sa.Column("mapping", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("created_at", sa.DateTime(), server_default=sa.text("now()"), nullable=False),
        sa.CheckConstraint("source IN ('mail','crm')", name="ck_mappings_source"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("run_id", "source", name="uq_mappings_run_source"),
    )
    op.create_index("idx_mappings_run", "mappings", ["run_id"], unique=False)

    # =========================
    # MATCHES
    # =========================
    op.create_table(
        "matches",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("run_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("runs.id"), nullable=False),
        sa.Column("user_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("crm_line_no", sa.BigInteger(), nullable=False),
        sa.Column("job_index", sa.Text(), nullable=True),
        sa.Column("mail_ids", postgresql.ARRAY(sa.Text()), nullable=True),
        sa.Column("matched_mail_dates", postgresql.ARRAY(sa.Date()), nullable=True),
        sa.Column("crm_job_date", sa.Date(), nullable=True),
        sa.Column("job_value", sa.Numeric(12, 2), nullable=True),
        sa.Column("crm_city", sa.String(), nullable=True),
        sa.Column("crm_state", sa.String(length=2), nullable=True),
        sa.Column("crm_zip", sa.String(), nullable=True),
        sa.Column("crm_full_address", sa.Text(), nullable=True),
        sa.Column("mail_full_address", sa.Text(), nullable=True),
        sa.Column("confidence_percent", sa.Integer(), nullable=True),
        sa.Column("match_notes", sa.Text(), nullable=True),
        sa.Column("zip5", sa.String(length=5), nullable=True),
        sa.Column("state", sa.String(length=2), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("user_id", "job_index", name="uq_matches_per_job"),
    )
    op.create_index("idx_matches_user", "matches", ["user_id"], unique=False)
    op.create_index("idx_matches_run", "matches", ["run_id"], unique=False)
    op.create_index("idx_matches_user_date", "matches", ["user_id", "crm_job_date"], unique=False)
    op.create_index("idx_matches_zip5", "matches", ["zip5"], unique=False)
    op.create_index("idx_matches_state", "matches", ["state"], unique=False)

    # =========================
    # RUN KPIs
    # =========================
    op.create_table(
        "run_kpis",
        sa.Column("run_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("runs.id"), nullable=False),
        sa.Column("user_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("total_mail", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("unique_mail_addresses", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("total_jobs", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("matches", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("match_rate", sa.Numeric(7, 2), nullable=False, server_default=sa.text("0")),
        sa.Column("match_revenue", sa.Numeric(14, 2), nullable=False, server_default=sa.text("0")),
        sa.Column("revenue_per_mailer", sa.Numeric(14, 2), nullable=False, server_default=sa.text("0")),
        sa.Column("avg_ticket_per_match", sa.Numeric(14, 2), nullable=False, server_default=sa.text("0")),
        sa.Column("median_days_to_convert", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("first_job_date", sa.Date(), nullable=True),
        sa.Column("last_job_date", sa.Date(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.PrimaryKeyConstraint("run_id"),
    )
    op.create_index("idx_run_kpis_user", "run_kpis", ["user_id"], unique=False)
    op.create_index("idx_run_kpis_dates", "run_kpis", ["first_job_date", "last_job_date"], unique=False)

    # =========================
    # RUN SERIES
    # =========================
    op.create_table(
        "run_series",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("run_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("runs.id"), nullable=False),
        sa.Column("series", sa.String(length=32), nullable=False),
        sa.Column("ym", sa.String(length=7), nullable=False),
        sa.Column("value", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("idx_run_series_run", "run_series", ["run_id"], unique=False)
    op.create_index("idx_run_series_unique", "run_series", ["run_id", "series", "ym"], unique=True)

    # =========================
    # RUN TOP CITIES / ZIPS
    # =========================
    op.create_table(
        "run_top_cities",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("run_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("runs.id"), nullable=False),
        sa.Column("city", sa.String(), nullable=False),
        sa.Column("matches", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("match_rate", sa.Numeric(7, 2), nullable=False, server_default=sa.text("0")),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("idx_run_top_cities_run", "run_top_cities", ["run_id"], unique=False)

    op.create_table(
        "run_top_zips",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("run_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("runs.id"), nullable=False),
        sa.Column("zip5", sa.String(length=5), nullable=False),
        sa.Column("matches", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("idx_run_top_zips_run", "run_top_zips", ["run_id"], unique=False)
    op.create_index("idx_run_top_zips_zip", "run_top_zips", ["zip5"], unique=False)

    # =========================
    # STAGING (RAW)
    # =========================

    # staging_raw_mail
    op.create_table(
        "staging_raw_mail",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True, nullable=False),
        sa.Column("run_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("runs.id"), nullable=False),
        sa.Column("user_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("users.id"), nullable=True),
        sa.Column("rownum", sa.Integer(), nullable=False),
        sa.Column("data", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(), server_default=sa.text("now()"), nullable=True),
    )
    op.create_index("idx_raw_mail_run_row", "staging_raw_mail", ["run_id", "rownum"], unique=False)
    op.create_index("idx_raw_mail_run", "staging_raw_mail", ["run_id"], unique=False)

    # staging_raw_crm
    op.create_table(
        "staging_raw_crm",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True, nullable=False),
        sa.Column("run_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("runs.id"), nullable=False),
        sa.Column("user_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("users.id"), nullable=True),
        sa.Column("rownum", sa.Integer(), nullable=False),
        sa.Column("data", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(), server_default=sa.text("now()"), nullable=True),
    )
    op.create_index("idx_raw_crm_run_row", "staging_raw_crm", ["run_id", "rownum"], unique=False)
    op.create_index("idx_raw_crm_run", "staging_raw_crm", ["run_id"], unique=False)

    # =========================
    # STAGING (NORMALIZED)
    # =========================

    # staging_mail
    op.create_table(
        "staging_mail",
        sa.Column("source_id", sa.Text(), nullable=True),  # optional external id
        sa.Column("run_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("runs.id"), nullable=False),
        sa.Column("user_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("users.id"), nullable=True),

        sa.Column("address1", sa.Text(), nullable=True),
        sa.Column("address2", sa.Text(), nullable=True),
        sa.Column("city", sa.Text(), nullable=True),
        sa.Column("state", sa.Text(), nullable=True),
        sa.Column("zip", sa.Text(), nullable=True),
        sa.Column("full_address", sa.Text(), nullable=True),

        sa.Column("sent_date", sa.Date(), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(), server_default=sa.text("now()"), nullable=True),

        sa.Column("mail_key", sa.Text(), nullable=False),

        sa.Column("line_no", sa.BigInteger(),
                server_default=sa.text("nextval('staging_mail_line_no_seq'::regclass)"),
                nullable=False),

        sa.PrimaryKeyConstraint("run_id", "line_no", name="staging_mail_pkey"),
    )
    op.create_index("ix_staging_mail_run_source_id", "staging_mail", ["run_id", "source_id"], unique=False)
    op.create_index("idx_stg_mail_run", "staging_mail", ["run_id"], unique=False)
    # Single global unique per user
    op.create_index("uq_stg_mail_user_mail_key", "staging_mail", ["user_id", "mail_key"], unique=True)


    # staging_crm
    op.create_table(
        "staging_crm",
        sa.Column("source_id", sa.Text(), nullable=True),

        sa.Column("run_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("runs.id"), nullable=False),
        sa.Column("user_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("users.id"), nullable=True),

        # REQUIRED: AND key for jobs
        sa.Column("job_index", sa.Text(), nullable=False),

        sa.Column("address1", sa.Text(), nullable=True),
        sa.Column("address2", sa.Text(), nullable=True),
        sa.Column("city", sa.Text(), nullable=True),
        sa.Column("state", sa.Text(), nullable=True),
        sa.Column("zip", sa.Text(), nullable=True),
        sa.Column("full_address", sa.Text(), nullable=True),

        sa.Column("job_date", sa.Date(), nullable=True),
        sa.Column("job_value", sa.Numeric(12, 2), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(), server_default=sa.text("now()"), nullable=True),

        sa.Column("line_no", sa.BigInteger(),
                server_default=sa.text("nextval('staging_crm_line_no_seq'::regclass)"),
                nullable=False),

        sa.PrimaryKeyConstraint("run_id", "line_no", name="staging_crm_pkey"),
    )
    op.create_index("ix_staging_crm_run_source_id", "staging_crm", ["run_id", "source_id"], unique=False)
    op.create_index("idx_stg_crm_run", "staging_crm", ["run_id"], unique=False)
    # Single global unique per user
    op.create_index("uq_stg_crm_user_job_index", "staging_crm", ["user_id", "job_index"], unique=True)

def downgrade():
    # Drop in reverse dependency order

    op.drop_index("idx_run_top_zips_zip", table_name="run_top_zips")
    op.drop_index("idx_run_top_zips_run", table_name="run_top_zips")
    op.drop_table("run_top_zips")

    op.drop_index("idx_run_top_cities_run", table_name="run_top_cities")
    op.drop_table("run_top_cities")

    op.drop_index("idx_run_series_unique", table_name="run_series")
    op.drop_index("idx_run_series_run", table_name="run_series")
    op.drop_table("run_series")

    op.drop_index("idx_run_kpis_dates", table_name="run_kpis")
    op.drop_index("idx_run_kpis_user", table_name="run_kpis")
    op.drop_table("run_kpis")

    op.drop_index("idx_matches_state", table_name="matches")
    op.drop_index("idx_matches_zip5", table_name="matches")
    op.drop_index("idx_matches_user_date", table_name="matches")
    op.drop_index("idx_matches_run", table_name="matches")
    op.drop_index("idx_matches_user", table_name="matches")
    op.drop_table("matches")

    op.drop_index("idx_mappings_run", table_name="mappings")
    op.drop_table("mappings")

    op.drop_index("idx_geo_user_kind_date", table_name="geo_points")
    op.drop_index("idx_geo_run_kind", table_name="geo_points")
    op.drop_table("geo_points")

    op.drop_index("idx_runs_started_at", table_name="runs")
    op.drop_index("idx_runs_status", table_name="runs")
    op.drop_index("idx_runs_user", table_name="runs")
    op.drop_table("runs")

    op.drop_index("idx_subscriptions_user", table_name="subscriptions")
    op.drop_table("subscriptions")

    op.drop_table("users")

    # Staging (raw then normalized)
    op.drop_index("uq_stg_crm_user_job_index", table_name="staging_crm")
    op.drop_index("ix_staging_crm_run_source_id", table_name="staging_crm")
    op.drop_index("idx_stg_crm_run", table_name="staging_crm")
    op.drop_table("staging_crm")

    op.drop_index("uq_stg_mail_user_mail_key", table_name="staging_mail")
    op.drop_index("ix_staging_mail_run_source_id", table_name="staging_mail")
    op.drop_index("idx_stg_mail_run", table_name="staging_mail")
    op.drop_table("staging_mail")

    op.drop_index("idx_raw_crm_run", table_name="staging_raw_crm")
    op.drop_index("idx_raw_crm_run_row", table_name="staging_raw_crm")
    op.drop_table("staging_raw_crm")

    op.drop_index("idx_raw_mail_run", table_name="staging_raw_mail")
    op.drop_index("idx_raw_mail_run_row", table_name="staging_raw_mail")
    op.drop_table("staging_raw_mail")

    # Sequences
    op.execute("DROP SEQUENCE IF EXISTS staging_crm_line_no_seq")
    op.execute("DROP SEQUENCE IF EXISTS staging_mail_line_no_seq")

    # (Leave pgcrypto installed; safe to keep. Remove if you must.)
    # op.execute("DROP EXTENSION IF NOT EXISTS pgcrypto")