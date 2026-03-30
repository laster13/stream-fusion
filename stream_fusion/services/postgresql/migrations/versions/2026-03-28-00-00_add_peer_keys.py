"""add peer_keys table for per-peer authenticated cache access

Revision ID: add_peer_keys
Revises: add_debrid_cache
Create Date: 2026-03-28 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa

revision = 'add_peer_keys'
down_revision = 'add_debrid_cache'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'peer_keys',
        sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column('key_id', sa.String(36), nullable=False),
        sa.Column('secret', sa.String(128), nullable=False),
        sa.Column('name', sa.String(100), nullable=False),
        sa.Column('is_active', sa.Boolean(), nullable=False, server_default='true'),
        sa.Column('rate_limit', sa.Integer(), nullable=False, server_default='60'),
        sa.Column('rate_window', sa.Integer(), nullable=False, server_default='60'),
        sa.Column('expires_at', sa.BigInteger(), nullable=True),
        sa.Column('last_used_at', sa.BigInteger(), nullable=True),
        sa.Column('total_queries', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('created_at', sa.BigInteger(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('key_id', name='uq_peer_keys_key_id'),
    )
    op.create_index('ix_peer_keys_key_id', 'peer_keys', ['key_id'])


def downgrade() -> None:
    op.drop_index('ix_peer_keys_key_id', table_name='peer_keys')
    op.drop_table('peer_keys')
