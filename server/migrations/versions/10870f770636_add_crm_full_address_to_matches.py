"""add crm_full_address to matches

Revision ID: 10870f770636
Revises: f969ba545323
Create Date: 2025-11-03 13:47:38.604288

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '10870f770636'
down_revision = 'f969ba545323'
branch_labels = None
depends_on = None

def upgrade():
    op.add_column(
        "matches",
        sa.Column("crm_full_address", sa.Text(), nullable=True),
    )

    # OPTIONAL: backfill from existing columns if you have them in matches
    # (only run if `crm_address1/2` exist; safe-guarded with CASE WHEN EXISTS)
    op.execute("""
    DO $$
    BEGIN
      IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='matches' AND column_name='crm_address1'
      ) THEN
        UPDATE matches
        SET crm_full_address = NULLIF(
          TRIM(
            CONCAT_WS(' ',
              NULLIF(TRIM(crm_address1), ''),
              NULLIF(TRIM(crm_address2), ''),
              NULLIF(TRIM(crm_city), ''),
              NULLIF(TRIM(crm_state), ''),
              NULLIF(TRIM(crm_zip), '')
            )
          ), ''
        )
        WHERE crm_full_address IS NULL;
      END IF;
    END
    $$;
    """)

def downgrade():
    op.drop_column("matches", "crm_full_address")
