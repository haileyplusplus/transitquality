"""Add synthetic trip id field to current train

Revision ID: 063077385ef7
Revises: 79f9089039df
Create Date: 2025-02-19 17:20:07.736187

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '063077385ef7'
down_revision: Union[str, None] = '79f9089039df'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('current_train_state', sa.Column('synthetic_trip_id', sa.Integer(), nullable=True))


def downgrade() -> None:
    op.drop_column('current_train_state', 'synthetic_trip_id')
