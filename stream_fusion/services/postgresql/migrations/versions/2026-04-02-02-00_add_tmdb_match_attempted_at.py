"""add tmdb_match_attempted_at column on torrent_items

Revision ID: add_tmdb_match_attempted_at
Revises: add_torrent_groups
Branch Labels: None
Depends On: None
Create Date: 2026-04-02 02:00:00.000000

"""
from alembic import op
import sqlalchemy as sa

revision = 'add_tmdb_match_attempted_at'
down_revision = 'add_torrent_groups'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        'torrent_items',
        sa.Column('tmdb_match_attempted_at', sa.BigInteger(), nullable=True),
    )
    op.create_index(
        'ix_torrent_items_tmdb_match_attempted_at',
        'torrent_items',
        ['tmdb_match_attempted_at'],
    )


def downgrade() -> None:
    op.drop_index('ix_torrent_items_tmdb_match_attempted_at', table_name='torrent_items')
    op.drop_column('torrent_items', 'tmdb_match_attempted_at')
