"""Initial schema (SQLite-safe stub)

Revision ID: 0001_init
Revises: None
Create Date: 2025-10-10
"""

from alembic import op
import sqlalchemy as sa

revision = '0001_init'
down_revision = None
branch_labels = None
depends_on = None

def upgrade():
    bind = op.get_bind()
    is_pg = bind.dialect.name == "postgresql"
    if is_pg:
        op.execute('CREATE EXTENSION IF NOT EXISTS "uuid-ossp";')
    # Tables are created by SQLAlchemy metadata in env.py for local dev.

def downgrade():
    pass
