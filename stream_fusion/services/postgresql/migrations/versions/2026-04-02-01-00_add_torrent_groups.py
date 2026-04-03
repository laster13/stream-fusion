"""add torrent_groups table and group_id column on torrent_items

Revision ID: add_torrent_groups
Revises: add_app_settings
Branch Labels: None
Depends On: None
Create Date: 2026-04-02 01:00:00.000000

"""
from alembic import op
import sqlalchemy as sa

revision = 'add_torrent_groups'
down_revision = 'add_app_settings'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # 1. Create the torrent_groups table
    op.create_table(
        'torrent_groups',
        sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column('canonical_info_hash', sa.String(40), nullable=True),
        sa.Column('canonical_title', sa.String(), nullable=True),
        sa.Column('tmdb_id', sa.Integer(), nullable=True),
        sa.Column('item_count', sa.Integer(), server_default='0', nullable=False),
        sa.Column('created_at', sa.BigInteger(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('ix_torrent_groups_canonical_info_hash', 'torrent_groups', ['canonical_info_hash'])
    op.create_index('ix_torrent_groups_tmdb_id', 'torrent_groups', ['tmdb_id'])

    # 2. Add group_id FK column to torrent_items
    op.add_column('torrent_items', sa.Column('group_id', sa.BigInteger(), nullable=True))
    op.create_index('ix_torrent_items_group_id', 'torrent_items', ['group_id'])
    op.create_foreign_key(
        'fk_torrent_items_group_id',
        'torrent_items', 'torrent_groups',
        ['group_id'], ['id'],
        ondelete='SET NULL',
    )


def downgrade() -> None:
    op.drop_constraint('fk_torrent_items_group_id', 'torrent_items', type_='foreignkey')
    op.drop_index('ix_torrent_items_group_id', table_name='torrent_items')
    op.drop_column('torrent_items', 'group_id')

    op.drop_index('ix_torrent_groups_tmdb_id', table_name='torrent_groups')
    op.drop_index('ix_torrent_groups_canonical_info_hash', table_name='torrent_groups')
    op.drop_table('torrent_groups')
