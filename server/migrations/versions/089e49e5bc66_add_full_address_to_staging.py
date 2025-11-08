"""add full address to staging

Revision ID: 089e49e5bc66
Revises: ba871d5d2dfc
Create Date: 2025-11-06 16:05:33.239249

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '089e49e5bc66'
down_revision = 'ba871d5d2dfc'
branch_labels = None
depends_on = None

def upgrade():
    # staging_mail
    op.add_column("staging_mail", sa.Column("full_address", sa.Text(), nullable=True))

    # staging_crm
    op.add_column("staging_crm", sa.Column("full_address", sa.Text(), nullable=True))

def downgrade():
    op.drop_column("staging_crm", "full_address")
    op.drop_column("staging_mail", "full_address")
