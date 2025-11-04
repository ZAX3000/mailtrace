"""baseline

Revision ID: ba871d5d2dfc
Revises:
Create Date: 2025-11-04 09:33:06.947322
"""
from alembic import op
import sqlalchemy as sa

revision = "ba871d5d2dfc"
down_revision = None
branch_labels = None
depends_on = None

def upgrade():
    # Baseline: no changes; use `alembic stamp head` to mark this version.
    pass

def downgrade():
    # Nothing to undo for a no-op baseline.
    pass
