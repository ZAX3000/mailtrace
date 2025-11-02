# migrations/versions/ade8c0e1f67e_ids_to_text_in_staging_mail_staging_crm.py
from alembic import op
import sqlalchemy as sa

revision = "ade8c0e1f67e"
down_revision = "8fb49351c8dd"
branch_labels = None
depends_on = None

def upgrade():
    # ---------- staging_mail ----------
    # 0) create a sequence for the new line_no
    op.execute("CREATE SEQUENCE IF NOT EXISTS staging_mail_line_no_seq")

    # 1) add line_no with default nextval() so existing rows are populated
    op.add_column(
        "staging_mail",
        sa.Column(
            "line_no",
            sa.BigInteger(),
            server_default=sa.text("nextval('staging_mail_line_no_seq')"),
            nullable=False,
        ),
    )

    # 2) drop old PK (likely on id) and create composite PK(run_id, line_no)
    op.drop_constraint("staging_mail_pkey", "staging_mail", type_="primary")
    op.create_primary_key("staging_mail_pkey", "staging_mail", ["run_id", "line_no"])

    # 3) now we can safely change external id type/nullability
    op.alter_column(
        "staging_mail",
        "id",
        existing_type=sa.BigInteger(),
        type_=sa.Text(),
        postgresql_using="id::text",
        existing_nullable=False,
        nullable=True,
    )

    # 4) optional: keep default for future inserts, or drop it:
    # op.alter_column("staging_mail", "line_no", server_default=None)

    # 5) helpful index for lookups within a run
    op.create_index(
        "ix_staging_mail_run_id_id",
        "staging_mail",
        ["run_id", "id"],
        unique=False,
        if_not_exists=True,
    )

    # ---------- staging_crm ----------
    op.execute("CREATE SEQUENCE IF NOT EXISTS staging_crm_line_no_seq")
    op.add_column(
        "staging_crm",
        sa.Column(
            "line_no",
            sa.BigInteger(),
            server_default=sa.text("nextval('staging_crm_line_no_seq')"),
            nullable=False,
        ),
    )

    op.drop_constraint("staging_crm_pkey", "staging_crm", type_="primary")
    op.create_primary_key("staging_crm_pkey", "staging_crm", ["run_id", "line_no"])

    op.alter_column(
        "staging_crm",
        "crm_id",
        existing_type=sa.BigInteger(),
        type_=sa.Text(),
        postgresql_using="crm_id::text",
        existing_nullable=False,
        nullable=True,
    )

    # op.alter_column("staging_crm", "line_no", server_default=None)

    op.create_index(
        "ix_staging_crm_run_id_crm_id",
        "staging_crm",
        ["run_id", "crm_id"],
        unique=False,
        if_not_exists=True,
    )


def downgrade():
    # best-effort rollback
    op.drop_index("ix_staging_crm_run_id_crm_id", table_name="staging_crm")
    op.drop_constraint("staging_crm_pkey", "staging_crm", type_="primary")
    op.alter_column(
        "staging_crm",
        "crm_id",
        existing_type=sa.Text(),
        type_=sa.BigInteger(),
        postgresql_using="NULLIF(regexp_replace(crm_id, '\\D', '', 'g'), '')::bigint",
        nullable=True,
    )
    op.create_primary_key("staging_crm_pkey", "staging_crm", ["run_id"])  # adjust if you had a different old PK
    # You can also drop line_no/sequence if you truly need to:
    # op.drop_column("staging_crm", "line_no")
    # op.execute("DROP SEQUENCE IF EXISTS staging_crm_line_no_seq")

    op.drop_index("ix_staging_mail_run_id_id", table_name="staging_mail")
    op.drop_constraint("staging_mail_pkey", "staging_mail", type_="primary")
    op.alter_column(
        "staging_mail",
        "id",
        existing_type=sa.Text(),
        type_=sa.BigInteger(),
        postgresql_using="NULLIF(regexp_replace(id, '\\D', '', 'g'), '')::bigint",
        nullable=True,
    )
    op.create_primary_key("staging_mail_pkey", "staging_mail", ["run_id"])  # adjust if different before
    # op.drop_column("staging_mail", "line_no")
    # op.execute("DROP SEQUENCE IF EXISTS staging_mail_line_no_seq")