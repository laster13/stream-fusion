"""add metadata_mappings table for admin-managed IMDb/TMDB/title overrides

Revision ID: add_metadata_mappings
Revises: add_peer_keys
Create Date: 2026-03-28 01:00:00.000000

"""
from alembic import op
import sqlalchemy as sa

revision = 'add_metadata_mappings'
down_revision = 'add_peer_keys'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'metadata_mappings',
        sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column('imdb_id', sa.String(20), nullable=False),
        sa.Column('tmdb_id', sa.String(20), nullable=True),
        sa.Column('title_override', sa.String(500), nullable=True),
        sa.Column('media_type', sa.String(10), nullable=False, server_default='series'),
        sa.Column('notes', sa.Text(), nullable=True),
        sa.Column('created_at', sa.BigInteger(), nullable=False),
        sa.Column('updated_at', sa.BigInteger(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('imdb_id', name='uq_metadata_mappings_imdb_id'),
    )
    op.create_index('ix_metadata_mappings_imdb_id', 'metadata_mappings', ['imdb_id'])


def downgrade() -> None:
    op.drop_index('ix_metadata_mappings_imdb_id', table_name='metadata_mappings')
    op.drop_table('metadata_mappings')
