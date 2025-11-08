"""add job index to staging crm

Revision ID: 6c0c1261671a
Revises: 089e49e5bc66
Create Date: 2025-11-07 17:54:23.081757

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '6c0c1261671a'
down_revision = '089e49e5bc66'
branch_labels = None
depends_on = None

SCHEMA = "public"
TABLE = "staging_crm"

def upgrade():
    # 1) add column
    op.add_column(
        TABLE,
        sa.Column("job_index", sa.Text(), nullable=False),
        schema=SCHEMA,
    )
    # 2) unique index across runs per user
    op.create_index(
        "ux_staging_crm_user_jobindex",
        TABLE,
        ["user_id", "job_index"],
        unique=True,
        schema=SCHEMA,
    )

def downgrade():
    op.drop_index("ux_staging_crm_user_jobindex", table_name=TABLE, schema=SCHEMA)
    op.drop_column(TABLE, "job_index", schema=SCHEMA)
