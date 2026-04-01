"""add title_normalization_rules and language_rules tables

Revision ID: add_title_matching_rules
Revises: extend_metadata_mappings
Branch Labels: None
Depends On: None
Create Date: 2026-04-01 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = 'add_title_matching_rules'
down_revision = 'extend_metadata_mappings'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'title_normalization_rules',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('rule_type', sa.String(length=20), nullable=False),
        sa.Column('pattern', sa.String(length=200), nullable=False),
        sa.Column('replacement', sa.String(length=200), nullable=False, server_default=''),
        sa.Column('description', sa.String(length=500), nullable=True),
        sa.Column('is_active', sa.Boolean(), nullable=False, server_default='true'),
        sa.Column('created_at', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('updated_at', sa.Integer(), nullable=False, server_default='0'),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('ix_title_normalization_rules_rule_type', 'title_normalization_rules', ['rule_type'])

    op.create_table(
        'language_rules',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('rule_type', sa.String(length=20), nullable=False),
        sa.Column('key', sa.String(length=100), nullable=False),
        sa.Column('value', sa.String(length=500), nullable=False),
        sa.Column('extra', postgresql.JSONB(), nullable=True),
        sa.Column('description', sa.String(length=500), nullable=True),
        sa.Column('is_active', sa.Boolean(), nullable=False, server_default='true'),
        sa.Column('created_at', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('updated_at', sa.Integer(), nullable=False, server_default='0'),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('ix_language_rules_rule_type', 'language_rules', ['rule_type'])


def downgrade() -> None:
    op.drop_index('ix_language_rules_rule_type', table_name='language_rules')
    op.drop_table('language_rules')
    op.drop_index('ix_title_normalization_rules_rule_type', table_name='title_normalization_rules')
    op.drop_table('title_normalization_rules')
