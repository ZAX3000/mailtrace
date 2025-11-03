"""Rename legacy matches cols to new schema

Revision ID: f969ba545323
Revises: 8c9b51d537f5
Create Date: 2025-11-03 11:19:12.718893

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'f969ba545323'
down_revision = '8c9b51d537f5'
branch_labels = None
depends_on = None

def upgrade():
    # Rename legacy columns to match DAO/model
    op.alter_column("matches", "crm_source_id",  new_column_name="crm_id")
    op.alter_column("matches", "mail_source_id", new_column_name="mail_id")
    op.alter_column("matches", "matched_mail_full_address", new_column_name="mail_full_address")

    # If you don’t already have created_at and want it:
    # import sqlalchemy as sa
    # op.add_column("matches", sa.Column("created_at", sa.DateTime(timezone=True),
    #                                    server_default=sa.text("NOW()"), nullable=False))

def downgrade():
    # Revert names
    op.alter_column("matches", "crm_id",  new_column_name="crm_source_id")
    op.alter_column("matches", "mail_id", new_column_name="mail_source_id")
    op.alter_column("matches", "mail_full_address", new_column_name="matched_mail_full_address")

    # If you added created_at above:
    # op.drop_column("matches", "created_at")
