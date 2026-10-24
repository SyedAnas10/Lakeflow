"""initial schema

Revision ID: 0001
Revises: 
Create Date: 2026-10-25 03:28:16.096897
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = '0001'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Generated with --autogenerate, then reviewed by hand.
    op.create_table('ingestion_job',
    sa.Column('id', sa.String(length=36), nullable=False),
    sa.Column('content_hash', sa.String(length=64), nullable=False),
    sa.Column('source', sa.String(length=100), nullable=False),
    sa.Column('filename', sa.Unicode(length=255), nullable=False),
    sa.Column('landing_key', sa.Unicode(length=1024), nullable=False),
    sa.Column('size_bytes', sa.BigInteger(), nullable=False),
    sa.Column('status', sa.Enum('QUEUED', 'RUNNING', 'SUCCEEDED', 'FAILED', name='jobstatus', native_enum=False, length=20), nullable=False),
    sa.Column('stage', sa.String(length=50), nullable=True),
    sa.Column('progress_pct', sa.Integer(), nullable=False),
    sa.Column('attempts', sa.Integer(), nullable=False),
    sa.Column('lease_until', sa.DateTime(), nullable=True),
    sa.Column('file_format', sa.String(length=20), nullable=True),
    sa.Column('encoding', sa.String(length=40), nullable=True),
    sa.Column('delimiter', sa.String(length=4), nullable=True),
    sa.Column('rows_raw', sa.Integer(), nullable=True),
    sa.Column('rows_exact_duplicate', sa.Integer(), nullable=True),
    sa.Column('rows_rejected', sa.Integer(), nullable=True),
    sa.Column('rows_key_duplicate', sa.Integer(), nullable=True),
    sa.Column('rows_valid', sa.Integer(), nullable=True),
    sa.Column('replaced_chars', sa.Integer(), nullable=True),
    sa.Column('error', sa.Unicode(), nullable=True),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('content_hash')
    )
    with op.batch_alter_table('ingestion_job', schema=None) as batch_op:
        batch_op.create_index(batch_op.f('ix_ingestion_job_status'), ['status'], unique=False)

    op.create_table('column_profile',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('job_id', sa.String(length=36), nullable=False),
    sa.Column('column_name', sa.Unicode(length=255), nullable=False),
    sa.Column('position', sa.Integer(), nullable=False),
    sa.Column('row_count', sa.Integer(), nullable=False),
    sa.Column('null_count', sa.Integer(), nullable=False),
    sa.Column('null_rate', sa.Float(), nullable=False),
    sa.Column('distinct_approx', sa.Integer(), nullable=False),
    sa.Column('min_length', sa.Integer(), nullable=True),
    sa.Column('max_length', sa.Integer(), nullable=True),
    sa.Column('inferred_type', sa.String(length=20), nullable=False),
    sa.Column('top_values', sa.JSON(), nullable=False),
    sa.Column('top_patterns', sa.JSON(), nullable=False),
    sa.ForeignKeyConstraint(['job_id'], ['ingestion_job.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('job_id', 'column_name', name='uq_profile_job_column')
    )
    with op.batch_alter_table('column_profile', schema=None) as batch_op:
        batch_op.create_index(batch_op.f('ix_column_profile_job_id'), ['job_id'], unique=False)



def downgrade() -> None:
    # Generated with --autogenerate, then reviewed by hand.
    with op.batch_alter_table('column_profile', schema=None) as batch_op:
        batch_op.drop_index(batch_op.f('ix_column_profile_job_id'))

    op.drop_table('column_profile')
    with op.batch_alter_table('ingestion_job', schema=None) as batch_op:
        batch_op.drop_index(batch_op.f('ix_ingestion_job_status'))

    op.drop_table('ingestion_job')
