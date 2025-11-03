"""staging_*: use TEXT source_id; drop crm.id BIGINT; fix indexes

Revision ID: b9123ecf4830
Revises: adc7a7f57f77
Create Date: 2025-11-02 21:12:45.697586

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'b9123ecf4830'
down_revision = 'adc7a7f57f77'
branch_labels = None
depends_on = None

def upgrade():
    # ---- staging_mail: id -> source_id (TEXT NULL) ----
    with op.batch_alter_table("staging_mail") as batch:
        # rename id -> source_id if it exists
        try:
            batch.alter_column("id", new_column_name="source_id")
        except Exception:
            # already renamed in this env
            pass

    # swap index (run_id, id) -> (run_id, source_id)
    try:
        op.drop_index("ix_staging_mail_run_id_id", table_name="staging_mail")
    except Exception:
        pass
    op.create_index("ix_staging_mail_run_id_source_id", "staging_mail", ["run_id", "source_id"], unique=False)

    # ---- staging_crm: drop wrong BIGINT id; add source_id TEXT ----
    with op.batch_alter_table("staging_crm") as batch:
        # drop the stray bigint id if present
        try:
            batch.drop_column("id")
        except Exception:
            pass
        # add source_id TEXT if missing
        try:
            batch.add_column(sa.Column("source_id", sa.Text(), nullable=True))
        except Exception:
            pass

    # swap index (run_id, crm_id) -> (run_id, source_id)
    try:
        op.drop_index("ix_staging_crm_run_id_crm_id", table_name="staging_crm")
    except Exception:
        pass
    op.create_index("ix_staging_crm_run_id_source_id", "staging_crm", ["run_id", "source_id"], unique=False)

def downgrade():
    # Reverse (make it harmless on downgrade)
    # staging_mail: source_id -> id (keep TEXT, NULL)
    with op.batch_alter_table("staging_mail") as batch:
        try:
            batch.alter_column("source_id", new_column_name="id")
        except Exception:
            pass
    try:
        op.drop_index("ix_staging_mail_run_id_source_id", table_name="staging_mail")
    except Exception:
        pass
    op.create_index("ix_staging_mail_run_id_id", "staging_mail", ["run_id", "id"], unique=False)

    # staging_crm: remove source_id, recreate nullable id (to avoid blocking inserts on downgrade)
    with op.batch_alter_table("staging_crm") as batch:
        try:
            batch.drop_column("source_id")
        except Exception:
            pass
        try:
            batch.add_column(sa.Column("id", sa.BigInteger(), nullable=True))
        except Exception:
            pass
    try:
        op.drop_index("ix_staging_crm_run_id_source_id", table_name="staging_crm")
    except Exception:
        pass
    op.create_index("ix_staging_crm_run_id_crm_id", "staging_crm", ["run_id", "crm_id"], unique=False)
