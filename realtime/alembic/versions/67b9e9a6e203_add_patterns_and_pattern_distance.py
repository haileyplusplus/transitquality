"""Add patterns and pattern distance

Revision ID: 67b9e9a6e203
Revises: 063077385ef7
Create Date: 2025-02-19 21:44:33.038028

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from geoalchemy2 import Geometry
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '67b9e9a6e203'
down_revision: Union[str, None] = '063077385ef7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('current_train_state', sa.Column('pattern_distance', sa.Integer(), nullable=True))
    op.add_column('train_position', sa.Column('pattern', sa.Integer(), nullable=True))
    op.add_column('train_position', sa.Column('synthetic_trip_id', sa.Integer(), nullable=True))
    op.add_column('train_position', sa.Column('pattern_distance', sa.Integer(), nullable=True))


def downgrade() -> None:
    op.drop_column('train_position', 'pattern_distance')
    op.drop_column('train_position', 'synthetic_trip_id')
    op.drop_column('train_position', 'pattern')
    op.drop_column('current_train_state', 'pattern_distance')

