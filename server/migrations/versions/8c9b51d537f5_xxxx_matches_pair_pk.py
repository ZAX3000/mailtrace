"""XXXX_matches_pair_pk

Revision ID: 8c9b51d537f5
Revises: b9123ecf4830
Create Date: 2025-11-02 21:59:51.681671

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as psql

# revision identifiers, used by Alembic.
revision = '8c9b51d537f5'
down_revision = 'b9123ecf4830'
branch_labels = None
depends_on = None

def upgrade():
    # ----------------------------
    # A) staging_crm: crm_id -> source_id (TEXT)
    # ----------------------------
    op.execute("ALTER TABLE staging_crm ADD COLUMN IF NOT EXISTS source_id TEXT")

    # backfill from crm_id if crm_id exists
    op.execute("""
    DO $$
    BEGIN
      IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='staging_crm' AND column_name='crm_id'
      ) THEN
        UPDATE staging_crm
        SET source_id = COALESCE(source_id, crm_id::text)
        WHERE source_id IS NULL;
      END IF;
    END $$;
    """)

    op.execute("DROP INDEX IF EXISTS ix_staging_crm_run_id_crm_id")
    op.execute("CREATE INDEX IF NOT EXISTS ix_staging_crm_run_id_source_id ON staging_crm (run_id, source_id)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_stg_crm_run ON staging_crm (run_id)")
    op.execute("ALTER TABLE staging_crm DROP COLUMN IF EXISTS crm_id")

    # ----------------------------
    # B) matches: one row per (crm_line_no, mail_line_no) with traceability
    # ----------------------------
    # drop any old PK
    op.execute("ALTER TABLE matches DROP CONSTRAINT IF EXISTS matches_pkey")

    # add line_no if missing
    op.execute("ALTER TABLE matches ADD COLUMN IF NOT EXISTS line_no bigint")

    # ensure a sequence + defaults for line_no, then make NOT NULL and PK
    op.execute("""
    DO $$
    BEGIN
      IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'matches_line_no_seq') THEN
        CREATE SEQUENCE matches_line_no_seq OWNED BY matches.line_no;
      END IF;
    END $$;
    """)
    op.execute("ALTER TABLE matches ALTER COLUMN line_no SET DEFAULT nextval('matches_line_no_seq')")
    op.execute("UPDATE matches SET line_no = nextval('matches_line_no_seq') WHERE line_no IS NULL")
    op.execute("ALTER TABLE matches ALTER COLUMN line_no SET NOT NULL")
    op.execute("ALTER TABLE matches ADD CONSTRAINT matches_pkey PRIMARY KEY (line_no)")

    # traceability columns (safe add)
    op.execute("ALTER TABLE matches ADD COLUMN IF NOT EXISTS crm_source_id TEXT")
    op.execute("ALTER TABLE matches ADD COLUMN IF NOT EXISTS mail_source_id TEXT")
    op.execute("ALTER TABLE matches ADD COLUMN IF NOT EXISTS crm_line_no bigint")
    op.execute("ALTER TABLE matches ADD COLUMN IF NOT EXISTS mail_line_no bigint")

    # make pair columns required (only if they exist)
    op.execute("ALTER TABLE matches ALTER COLUMN crm_line_no SET NOT NULL")
    op.execute("ALTER TABLE matches ALTER COLUMN mail_line_no SET NOT NULL")

    # unique constraints via unique indexes (idempotent)
    op.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_match_pair ON matches (run_id, crm_line_no, mail_line_no)")
    op.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_match_one_mail_per_crm ON matches (run_id, crm_line_no)")

    # helpful secondary indexes (idempotent)
    op.execute("CREATE INDEX IF NOT EXISTS idx_matches_user ON matches (user_id)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_matches_run ON matches (run_id)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_matches_user_date ON matches (user_id, crm_job_date)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_matches_zip5 ON matches (zip5)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_matches_state ON matches (state)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_matches_run_crm_sid ON matches (run_id, crm_source_id)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_matches_run_mail_sid ON matches (run_id, mail_source_id)")



def downgrade():
    # matches
    op.execute("DROP INDEX IF EXISTS idx_matches_run_mail_sid")
    op.execute("DROP INDEX IF EXISTS idx_matches_run_crm_sid")
    op.execute("DROP INDEX IF EXISTS idx_matches_state")
    op.execute("DROP INDEX IF EXISTS idx_matches_zip5")
    op.execute("DROP INDEX IF EXISTS idx_matches_user_date")
    op.execute("DROP INDEX IF EXISTS idx_matches_run")
    op.execute("DROP INDEX IF EXISTS idx_matches_user")
    op.execute("DROP INDEX IF EXISTS uq_match_one_mail_per_crm")
    op.execute("DROP INDEX IF EXISTS uq_match_pair")

    op.execute("ALTER TABLE matches DROP CONSTRAINT IF EXISTS matches_pkey")
    op.execute("ALTER TABLE matches ALTER COLUMN line_no DROP DEFAULT")
    op.execute("ALTER TABLE matches DROP COLUMN IF EXISTS mail_line_no")
    op.execute("ALTER TABLE matches DROP COLUMN IF EXISTS crm_line_no")
    op.execute("ALTER TABLE matches DROP COLUMN IF EXISTS mail_source_id")
    op.execute("ALTER TABLE matches DROP COLUMN IF EXISTS crm_source_id")
    op.execute("ALTER TABLE matches DROP COLUMN IF EXISTS line_no")
    op.execute("DROP SEQUENCE IF EXISTS matches_line_no_seq")

    # staging_crm
    op.execute("CREATE INDEX IF NOT EXISTS ix_staging_crm_run_id_crm_id ON staging_crm (run_id, crm_id)")
    op.execute("ALTER TABLE staging_crm ADD COLUMN IF NOT EXISTS crm_id TEXT")
    op.execute("UPDATE staging_crm SET crm_id = COALESCE(crm_id, source_id)")
    op.execute("DROP INDEX IF EXISTS ix_staging_crm_run_id_source_id")
    op.execute("DROP INDEX IF EXISTS idx_stg_crm_run")
    op.execute("ALTER TABLE staging_crm DROP COLUMN IF EXISTS source_id")
