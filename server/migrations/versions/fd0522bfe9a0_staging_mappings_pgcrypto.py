"""staging + mappings + pgcrypto

Revision ID: fd0522bfe9a0
Revises: e704aed01709
Create Date: 2025-11-01 11:33:06.779970

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as pg

# revision identifiers, used by Alembic.
revision = 'fd0522bfe9a0'
down_revision = 'e704aed01709'
branch_labels = None
depends_on = None


def upgrade():
    # --- ensure pgcrypto for gen_random_uuid() ---
    op.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")

    # --- mappings (per-run, per-source) ---
    # NOTE: table name is 'mappings' (as requested), aligned with mapper_dao.py
    op.create_table(
        "mappings",
        sa.Column("id", pg.UUID(as_uuid=True), server_default=sa.text("gen_random_uuid()"), primary_key=True, nullable=False),
        sa.Column("run_id", pg.UUID(as_uuid=True), sa.ForeignKey("runs.id"), nullable=False),
        sa.Column("user_id", pg.UUID(as_uuid=True), sa.ForeignKey("users.id"), nullable=True),
        sa.Column("source", sa.String(), nullable=False),
        sa.Column("mapping", pg.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("created_at", sa.DateTime(), server_default=sa.text("NOW()"), nullable=False),
    )
    op.create_check_constraint("ck_mappings_source", "mappings", "source IN ('mail','crm')")
    op.create_unique_constraint("uq_mappings_run_source", "mappings", ["run_id", "source"])
    op.create_index("idx_mappings_run", "mappings", ["run_id"])

    # --- staging schema & tables expected by DAO ---

    # raw JSONB landing tables
    op.execute("CREATE SCHEMA IF NOT EXISTS staging")

    op.execute("""
        CREATE TABLE IF NOT EXISTS staging_raw_mail (
          id BIGSERIAL PRIMARY KEY,
          run_id UUID NOT NULL REFERENCES runs(id),
          user_id UUID REFERENCES users(id),
          rownum INT NOT NULL,
          data JSONB NOT NULL,
          created_at TIMESTAMP DEFAULT NOW()
        )
    """)
    op.execute("CREATE INDEX IF NOT EXISTS idx_raw_mail_run ON staging_raw_mail(run_id)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_raw_mail_run_row ON staging_raw_mail(run_id, rownum)")

    op.execute("""
        CREATE TABLE IF NOT EXISTS staging_raw_crm (
          id BIGSERIAL PRIMARY KEY,
          run_id UUID NOT NULL REFERENCES runs(id),
          user_id UUID REFERENCES users(id),
          rownum INT NOT NULL,
          data JSONB NOT NULL,
          created_at TIMESTAMP DEFAULT NOW()
        )
    """)
    op.execute("CREATE INDEX IF NOT EXISTS idx_raw_crm_run ON staging_raw_crm(run_id)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_raw_crm_run_row ON staging_raw_crm(run_id, rownum)")

    # normalized staging tables (canonical CSV shape)
    op.execute("""
        CREATE TABLE IF NOT EXISTS staging_mail (
          id BIGSERIAL PRIMARY KEY,
          run_id UUID NOT NULL REFERENCES runs(id),
          user_id UUID REFERENCES users(id),
          -- canonical columns
          address1 TEXT,
          address2 TEXT,
          city TEXT,
          state TEXT,
          zip TEXT,
          sent_date DATE,
          created_at TIMESTAMP DEFAULT NOW()
        )
    """)
    op.execute("CREATE INDEX IF NOT EXISTS idx_stg_mail_run ON staging_mail(run_id)")

    op.execute("""
        CREATE TABLE IF NOT EXISTS staging_crm (
          id BIGSERIAL PRIMARY KEY,
          run_id UUID NOT NULL REFERENCES runs(id),
          user_id UUID REFERENCES users(id),
          -- canonical columns
          crm_id TEXT,
          address1 TEXT,
          address2 TEXT,
          city TEXT,
          state TEXT,
          zip TEXT,
          job_date DATE,
          job_value NUMERIC(12,2),
          created_at TIMESTAMP DEFAULT NOW()
        )
    """)
    op.execute("CREATE INDEX IF NOT EXISTS idx_stg_crm_run ON staging_crm(run_id)")


def downgrade():
    # drop staging tables (order matters for FK safety)
    op.execute("DROP TABLE IF EXISTS staging_crm")
    op.execute("DROP TABLE IF EXISTS staging_mail")
    op.execute("DROP TABLE IF EXISTS staging_raw_crm")
    op.execute("DROP TABLE IF EXISTS staging_raw_mail")
    # keep schema; or drop if you prefer:
    # op.execute("DROP SCHEMA IF EXISTS staging")

    # drop mappings
    op.drop_index("idx_mappings_run", table_name="mappings")
    op.drop_constraint("uq_mappings_run_source", "mappings", type_="unique")
    op.drop_constraint("ck_mappings_source", "mappings", type_="check")
    op.drop_table("mappings")

    # do not drop pgcrypto extension on downgrade (safe to leave installed)
