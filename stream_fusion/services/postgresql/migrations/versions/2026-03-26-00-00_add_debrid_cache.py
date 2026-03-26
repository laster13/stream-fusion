"""add debrid_cache table for persistent availability caching

Revision ID: add_debrid_cache
Revises: add_tmdb_id
Create Date: 2026-03-26 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = 'add_debrid_cache'
down_revision = 'add_tmdb_id'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'debrid_cache',
        sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column('info_hash', sa.String(40), nullable=False),
        sa.Column('service', sa.String(20), nullable=False),
        sa.Column('cached_data', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column('checked_at', sa.BigInteger(), nullable=False),
        sa.Column('expires_at', sa.BigInteger(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('info_hash', 'service', name='uq_debrid_cache_hash_service'),
    )
    op.create_index('ix_debrid_cache_hash_service', 'debrid_cache', ['info_hash', 'service'])
    op.create_index('ix_debrid_cache_expires_at', 'debrid_cache', ['expires_at'])


def downgrade() -> None:
    op.drop_index('ix_debrid_cache_expires_at', table_name='debrid_cache')
    op.drop_index('ix_debrid_cache_hash_service', table_name='debrid_cache')
    op.drop_table('debrid_cache')
