"""matches arrays and job uc

Revision ID: 387d478ba53b
Revises: 5482169c815b
Create Date: 2025-11-08 08:38:44.851007
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "387d478ba53b"
down_revision = "5482169c815b"
branch_labels = None
depends_on = None


def upgrade():
    conn = op.get_bind()

    # 0) Drop legacy uniques if present
    if conn.execute(sa.text("""
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'public.matches'::regclass
          AND conname = 'uq_match_pair'
    """)).fetchone():
        op.drop_constraint("uq_match_pair", "matches", schema="public", type_="unique")

    if conn.execute(sa.text("""
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'public.matches'::regclass
          AND conname = 'uq_match_one_per_job'
    """)).fetchone():
        op.drop_constraint("uq_match_one_per_job", "matches", schema="public", type_="unique")

    if conn.execute(sa.text("""
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'public.matches'::regclass
          AND conname = 'uq_match_one_mail_per_crm'
    """)).fetchone():
        op.drop_constraint("uq_match_one_mail_per_crm", "matches", schema="public", type_="unique")

    # 1) Ensure job_index column exists BEFORE creating uniques
    has_job_index = conn.execute(sa.text("""
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema='public' AND table_name='matches' AND column_name='job_index'
    """)).fetchone()
    if not has_job_index:
        op.add_column(
            "matches",
            sa.Column("job_index", sa.Text(), nullable=True),
            schema="public",
        )

    # 2) Drop legacy columns if present
    cols = conn.execute(sa.text("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema='public' AND table_name='matches'
    """)).fetchall()
    colnames = {c[0] for c in cols}

    with op.batch_alter_table("matches", schema="public") as b:
        if "mail_line_no" in colnames:
            b.drop_column("mail_line_no")
        if "mail_count_in_window" in colnames:
            b.drop_column("mail_count_in_window")
        if "matched_mail_date" in colnames:
            b.drop_column("matched_mail_date")

    # 3) mail_id -> mail_ids (TEXT[])
    if "mail_ids" not in colnames and "mail_id" in colnames:
        op.alter_column(
            "matches",
            "mail_id",
            type_=postgresql.ARRAY(sa.Text()),
            postgresql_using="CASE WHEN mail_id IS NULL THEN ARRAY[]::text[] ELSE ARRAY[mail_id] END",
            schema="public",
        )
        op.alter_column("matches", "mail_id", new_column_name="mail_ids", schema="public")

    # 4) Add matched_mail_dates (DATE[]) (bootstrap default then drop)
    if "matched_mail_dates" not in colnames:
        op.add_column(
            "matches",
            sa.Column(
                "matched_mail_dates",
                postgresql.ARRAY(sa.Date()),
                nullable=False,
                server_default=sa.text("'{}'::date[]"),
            ),
            schema="public",
        )
        op.alter_column("matches", "matched_mail_dates", server_default=None, schema="public")

    # 5) One row per (user_id, job_index)
    op.create_unique_constraint(
        "uq_matches_per_job", "matches", ["user_id", "job_index"], schema="public"
    )


def downgrade():
    # Drop new unique
    op.drop_constraint("uq_matches_per_job", "matches", schema="public", type_="unique")

    # Remove matched_mail_dates[]
    op.drop_column("matches", "matched_mail_dates", schema="public")

    # mail_ids[] -> mail_id
    op.alter_column("matches", "mail_ids", new_column_name="mail_id", schema="public")
    op.alter_column(
        "matches",
        "mail_id",
        type_=sa.Text(),
        postgresql_using=(
            "CASE WHEN mail_id IS NULL OR array_length(mail_id,1)=0 "
            "THEN NULL ELSE mail_id[1] END"
        ),
        schema="public",
    )

    # Recreate dropped legacy columns (nullable)
    with op.batch_alter_table("matches", schema="public") as b:
        b.add_column(sa.Column("mail_line_no", sa.BigInteger(), nullable=True))
        b.add_column(sa.Column("mail_count_in_window", sa.Integer(), nullable=True))
        b.add_column(sa.Column("matched_mail_date", sa.Date(), nullable=True))

    # Drop job_index (added in this migration)
    op.drop_column("matches", "job_index", schema="public")

    # Restore legacy uniques
    op.create_unique_constraint(
        "uq_match_pair", "matches", ["run_id", "crm_line_no", "mail_line_no"], schema="public"
    )
    op.create_unique_constraint(
        "uq_match_one_mail_per_crm", "matches", ["user_id", "crm_line_no"], schema="public"
    )