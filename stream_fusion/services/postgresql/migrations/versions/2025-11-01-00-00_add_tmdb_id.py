"""add tmdb_id column to torrent_items

Revision ID: add_tmdb_id
Revises: add_proxied_links
Create Date: 2025-11-01 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa

revision = 'add_tmdb_id'
down_revision = 'add_proxied_links'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column('torrent_items', sa.Column('tmdb_id', sa.Integer(), nullable=True))
    op.create_index(op.f('ix_torrent_items_tmdb_id'), 'torrent_items', ['tmdb_id'], unique=False)


def downgrade() -> None:
    op.drop_index(op.f('ix_torrent_items_tmdb_id'), table_name='torrent_items')
    op.drop_column('torrent_items', 'tmdb_id')
