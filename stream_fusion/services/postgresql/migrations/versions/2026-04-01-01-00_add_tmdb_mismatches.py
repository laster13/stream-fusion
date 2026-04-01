"""add tmdb_mismatches table and parsed_title partial index

Revision ID: add_tmdb_mismatches
Revises: add_title_matching_rules
Branch Labels: None
Depends On: None
Create Date: 2026-04-01 01:00:00.000000

"""
from alembic import op
import sqlalchemy as sa

revision = 'add_tmdb_mismatches'
down_revision = 'add_title_matching_rules'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'tmdb_mismatches',
        sa.Column('id', sa.BigInteger(), nullable=False, autoincrement=True),
        sa.Column('info_hash', sa.String(length=40), nullable=False),
        sa.Column('tmdb_id', sa.Integer(), nullable=False),
        sa.Column('raw_title', sa.String(), nullable=False),
        sa.Column('indexer', sa.String(), nullable=False),
        sa.Column('notes', sa.Text(), nullable=True),
        sa.Column('created_at', sa.BigInteger(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('info_hash', 'tmdb_id', name='uq_mismatch_hash_tmdb'),
    )
    op.create_index('ix_tmdb_mismatches_info_hash', 'tmdb_mismatches', ['info_hash'])
    op.create_index('ix_tmdb_mismatches_tmdb_id', 'tmdb_mismatches', ['tmdb_id'])

    # Partial functional index on parsed_data->>'parsed_title' for unmatched torrents.
    # Supports both search_unmatched() ORDER BY and assign_tmdb_by_info_hashes() lookups.
    op.execute("""
        CREATE INDEX ix_torrent_items_parsed_title_unmatched
        ON torrent_items ((parsed_data->>'parsed_title'))
        WHERE tmdb_id IS NULL
    """)


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_torrent_items_parsed_title_unmatched")
    op.drop_index('ix_tmdb_mismatches_tmdb_id', table_name='tmdb_mismatches')
    op.drop_index('ix_tmdb_mismatches_info_hash', table_name='tmdb_mismatches')
    op.drop_table('tmdb_mismatches')
