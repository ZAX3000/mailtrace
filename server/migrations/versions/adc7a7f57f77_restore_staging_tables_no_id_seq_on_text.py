"""restore staging tables (no id seq on TEXT)

Revision ID: adc7a7f57f77
Revises: c72900118a42
Create Date: 2025-11-02 19:39:44.032834

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = 'adc7a7f57f77'
down_revision = 'c72900118a42'
branch_labels = None
depends_on = None


def upgrade():
    # --- sequences used by line_no autoincrement ---
    op.execute(sa.text("CREATE SEQUENCE IF NOT EXISTS staging_mail_line_no_seq AS BIGINT START 1"))
    op.execute(sa.text("CREATE SEQUENCE IF NOT EXISTS staging_crm_line_no_seq  AS BIGINT START 1"))

    # ========== RAW TABLES ==========
    op.create_table(
        'staging_raw_mail',
        sa.Column('id', sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column('run_id', sa.UUID(), nullable=False),
        sa.Column('user_id', sa.UUID(), nullable=True),
        sa.Column('rownum', sa.Integer(), nullable=False),
        sa.Column('data', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column('created_at', sa.TIMESTAMP(), server_default=sa.text('now()'), nullable=True),
        sa.ForeignKeyConstraint(['run_id'], ['runs.id']),
        sa.ForeignKeyConstraint(['user_id'], ['users.id']),
    )
    op.create_index('idx_raw_mail_run_row', 'staging_raw_mail', ['run_id', 'rownum'])
    op.create_index('idx_raw_mail_run', 'staging_raw_mail', ['run_id'])

    op.create_table(
        'staging_raw_crm',
        sa.Column('id', sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column('run_id', sa.UUID(), nullable=False),
        sa.Column('user_id', sa.UUID(), nullable=True),
        sa.Column('rownum', sa.Integer(), nullable=False),
        sa.Column('data', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column('created_at', sa.TIMESTAMP(), server_default=sa.text('now()'), nullable=True),
        sa.ForeignKeyConstraint(['run_id'], ['runs.id']),
        sa.ForeignKeyConstraint(['user_id'], ['users.id']),
    )
    op.create_index('idx_raw_crm_run_row', 'staging_raw_crm', ['run_id', 'rownum'])
    op.create_index('idx_raw_crm_run', 'staging_raw_crm', ['run_id'])

    # ========== NORMALIZED TABLES ==========
    op.create_table(
        'staging_mail',
        sa.Column('id', sa.Text(), nullable=True),  # <-- NO sequence here
        sa.Column('run_id', sa.UUID(), nullable=False),
        sa.Column('user_id', sa.UUID(), nullable=True),
        sa.Column('address1', sa.Text(), nullable=True),
        sa.Column('address2', sa.Text(), nullable=True),
        sa.Column('city', sa.Text(), nullable=True),
        sa.Column('state', sa.Text(), nullable=True),
        sa.Column('zip', sa.Text(), nullable=True),
        sa.Column('sent_date', sa.Date(), nullable=True),
        sa.Column('created_at', sa.TIMESTAMP(), server_default=sa.text('now()'), nullable=True),

        # composite PK: (run_id, line_no) with explicit server_default from our sequence
        sa.Column('line_no', sa.BigInteger(),
                  server_default=sa.text("nextval('staging_mail_line_no_seq'::regclass)"),
                  nullable=False),

        sa.ForeignKeyConstraint(['run_id'], ['runs.id']),
        sa.ForeignKeyConstraint(['user_id'], ['users.id']),
        sa.PrimaryKeyConstraint('run_id', 'line_no', name='staging_mail_pkey'),
    )
    op.create_index('ix_staging_mail_run_id_id', 'staging_mail', ['run_id', 'id'])
    op.create_index('idx_stg_mail_run', 'staging_mail', ['run_id'])

    op.create_table(
        'staging_crm',
        sa.Column('id', sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column('run_id', sa.UUID(), nullable=False),
        sa.Column('user_id', sa.UUID(), nullable=True),

        sa.Column('crm_id', sa.Text(), nullable=True),
        sa.Column('address1', sa.Text(), nullable=True),
        sa.Column('address2', sa.Text(), nullable=True),
        sa.Column('city', sa.Text(), nullable=True),
        sa.Column('state', sa.Text(), nullable=True),
        sa.Column('zip', sa.Text(), nullable=True),
        sa.Column('job_date', sa.Date(), nullable=True),
        sa.Column('job_value', sa.Numeric(12, 2), nullable=True),
        sa.Column('created_at', sa.TIMESTAMP(), server_default=sa.text('now()'), nullable=True),

        sa.Column('line_no', sa.BigInteger(),
                  server_default=sa.text("nextval('staging_crm_line_no_seq'::regclass)"),
                  autoincrement=True, nullable=False),

        sa.ForeignKeyConstraint(['run_id'], ['runs.id']),
        sa.ForeignKeyConstraint(['user_id'], ['users.id']),
        sa.PrimaryKeyConstraint('run_id', 'line_no', name='staging_crm_pkey'),
    )
    op.create_index('ix_staging_crm_run_id_crm_id', 'staging_crm', ['run_id', 'crm_id'])
    op.create_index('idx_stg_crm_run', 'staging_crm', ['run_id'])


def downgrade():
    # drop normalized
    op.drop_index('idx_stg_crm_run', table_name='staging_crm')
    op.drop_index('ix_staging_crm_run_id_crm_id', table_name='staging_crm')
    op.drop_table('staging_crm')
    op.execute(sa.text("DROP SEQUENCE IF EXISTS staging_crm_line_no_seq"))

    op.drop_index('idx_stg_mail_run', table_name='staging_mail')
    op.drop_index('ix_staging_mail_run_id_id', table_name='staging_mail')
    op.drop_table('staging_mail')
    op.execute(sa.text("DROP SEQUENCE IF EXISTS staging_mail_line_no_seq"))

    # drop raw
    op.drop_index('idx_raw_crm_run', table_name='staging_raw_crm')
    op.drop_index('idx_raw_crm_run_row', table_name='staging_raw_crm')
    op.drop_table('staging_raw_crm')

    op.drop_index('idx_raw_mail_run', table_name='staging_raw_mail')
    op.drop_index('idx_raw_mail_run_row', table_name='staging_raw_mail')
    op.drop_table('staging_raw_mail')
