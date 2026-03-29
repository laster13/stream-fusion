"""extend metadata_mappings with search_titles and year_override

Revision ID: extend_metadata_mappings
Revises: add_metadata_mappings
Create Date: 2026-03-28 02:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = 'extend_metadata_mappings'
down_revision = 'add_metadata_mappings'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column('metadata_mappings', sa.Column('search_titles', postgresql.ARRAY(sa.String()), nullable=True))
    op.add_column('metadata_mappings', sa.Column('year_override', sa.Integer(), nullable=True))
    op.create_index('ix_metadata_mappings_tmdb_id', 'metadata_mappings', ['tmdb_id'])


def downgrade() -> None:
    op.drop_index('ix_metadata_mappings_tmdb_id', table_name='metadata_mappings')
    op.drop_column('metadata_mappings', 'year_override')
    op.drop_column('metadata_mappings', 'search_titles')
