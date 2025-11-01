"""fix runs ready defaults

Revision ID: 8fb49351c8dd
Revises: fd0522bfe9a0
Create Date: 2025-11-01 12:07:25.164917

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '8fb49351c8dd'
down_revision = 'fd0522bfe9a0'
branch_labels = None
depends_on = None

def upgrade():
    op.execute("ALTER TABLE runs ALTER COLUMN mail_ready SET DEFAULT false;")
    op.execute("ALTER TABLE runs ALTER COLUMN crm_ready  SET DEFAULT false;")
    op.execute("UPDATE runs SET mail_ready=false WHERE mail_ready IS NULL;")
    op.execute("UPDATE runs SET crm_ready=false  WHERE crm_ready  IS NULL;")
    # optional, make status default too (defensive)
    op.execute("ALTER TABLE runs ALTER COLUMN status SET DEFAULT 'queued';")

def downgrade():
    op.execute("ALTER TABLE runs ALTER COLUMN mail_ready DROP DEFAULT;")
    op.execute("ALTER TABLE runs ALTER COLUMN crm_ready  DROP DEFAULT;")
    op.execute("ALTER TABLE runs ALTER COLUMN status DROP DEFAULT;")
