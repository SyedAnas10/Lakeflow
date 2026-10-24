"""add declared encoding

Revision ID: 0002
Revises: 0001
Create Date: 2026-10-25 03:30:40.889614
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = '0002'
down_revision = '0001'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Nullable column: existing rows stay valid, and old app versions keep working (expand step).
    with op.batch_alter_table('ingestion_job', schema=None) as batch_op:
        batch_op.add_column(sa.Column('declared_encoding', sa.String(length=40), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table('ingestion_job', schema=None) as batch_op:
        batch_op.drop_column('declared_encoding')

