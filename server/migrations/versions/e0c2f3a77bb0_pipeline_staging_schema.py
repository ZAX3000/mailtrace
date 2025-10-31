"""pipeline + staging schema"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as pg

# --- IDs ---
revision = "002_pipeline_and_staging"
down_revision = "b853d9061cf3"  # <-- set to your current revision id
branch_labels = None
depends_on = None


def upgrade():
    # runs: progress/status fields
    with op.batch_alter_table("runs") as b:
        b.add_column(sa.Column("step", sa.String(), nullable=True))
        b.add_column(sa.Column("pct", sa.Integer(), nullable=True))
        b.add_column(sa.Column("message", sa.Text(), nullable=True))

    # relax NOT NULL on URLs (we upload one file at a time now)
    conn = op.get_bind()
    insp = sa.inspect(conn)
    cols = {c["name"]: c for c in insp.get_columns("runs")}
    if cols.get("mail_csv_url", {}).get("nullable") is False:
        op.alter_column("runs", "mail_csv_url", existing_type=sa.String(), nullable=True)
    if cols.get("crm_csv_url", {}).get("nullable") is False:
        op.alter_column("runs", "crm_csv_url", existing_type=sa.String(), nullable=True)

    # optional: a mappings table for user-defined column maps
    op.create_table(
        "mappings",
        sa.Column("id", pg.UUID(as_uuid=True), primary_key=True),
        sa.Column("user_id", pg.UUID(as_uuid=True), sa.ForeignKey("users.id")),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("data", pg.JSONB(), nullable=False),  # {canonical: [aliases...]}
        sa.Column("created_at", sa.DateTime(), server_default=sa.text("NOW()"), nullable=False),
        schema="public",
    )

    # staging schema + tables
    op.execute("CREATE SCHEMA IF NOT EXISTS staging")
    op.execute("""
        CREATE TABLE IF NOT EXISTS staging.mail (
          id BIGSERIAL PRIMARY KEY,
          run_id UUID NOT NULL,
          address1 TEXT, address2 TEXT, city TEXT, state TEXT, postal_code TEXT,
          sent_date DATE,
          created_at TIMESTAMP DEFAULT NOW()
        )
    """)
    op.execute("CREATE INDEX IF NOT EXISTS idx_stg_mail_run ON staging.mail(run_id)")
    op.execute("""
        CREATE TABLE IF NOT EXISTS staging.crm (
          id BIGSERIAL PRIMARY KEY,
          run_id UUID NOT NULL,
          crm_id TEXT,
          address1 TEXT, address2 TEXT, city TEXT, state TEXT, postal_code TEXT,
          job_date DATE,
          job_value NUMERIC(12,2),
          created_at TIMESTAMP DEFAULT NOW()
        )
    """)
    op.execute("CREATE INDEX IF NOT EXISTS idx_stg_crm_run ON staging.crm(run_id)")


def downgrade():
    op.execute("DROP TABLE IF EXISTS staging.crm")
    op.execute("DROP TABLE IF EXISTS staging.mail")
    op.execute("DROP SCHEMA IF EXISTS staging")
    with op.batch_alter_table("runs") as b:
        b.drop_column("message")
        b.drop_column("pct")
        b.drop_column("step")
    op.drop_table("mappings", schema="public")