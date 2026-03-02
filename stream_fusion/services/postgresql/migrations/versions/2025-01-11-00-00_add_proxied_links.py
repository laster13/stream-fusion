"""add proxied_links column

Revision ID: add_proxied_links
Revises: df288f2cf1fa
Create Date: 2025-01-11 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa

revision = 'add_proxied_links'
down_revision = 'df288f2cf1fa'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column('api_keys', sa.Column('proxied_links', sa.Boolean(), nullable=False, server_default='false'))


def downgrade() -> None:
    op.drop_column('api_keys', 'proxied_links')
