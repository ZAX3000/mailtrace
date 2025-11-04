"""drop mail_dates_in_window

Revision ID: b5c56c10551b
Revises: 10870f770636
Create Date: 2025-11-03 21:23:05.185374

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'b5c56c10551b'
down_revision = '10870f770636'
branch_labels = None
depends_on = None

def upgrade():
    op.drop_column("matches", "mail_dates_in_window")

def downgrade():
    op.add_column("matches", sa.Column("mail_dates_in_window", sa.Text(), nullable=True))
