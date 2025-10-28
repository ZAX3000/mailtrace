from alembic import op
import sqlalchemy as sa

revision = "4ea04dc719a2"
down_revision = "530f2d68dd3e"
branch_labels = None
depends_on = None

def upgrade():
    # Create schema and table for staged address ingest
    op.execute("CREATE SCHEMA IF NOT EXISTS staging")
    op.create_table(
        "addresses",
        sa.Column("email", sa.Text()),
        sa.Column("line1", sa.Text()),
        sa.Column("city", sa.Text()),
        sa.Column("state", sa.Text()),
        sa.Column("zip", sa.Text()),
        schema="staging",
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_staging_addresses_email "
        "ON staging.addresses (lower(email))"
    )

def downgrade():
    op.drop_table("addresses", schema="staging")
    op.execute("DROP SCHEMA IF EXISTS staging CASCADE")