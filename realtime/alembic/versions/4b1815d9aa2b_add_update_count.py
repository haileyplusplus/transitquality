"""Add update count

Revision ID: 4b1815d9aa2b
Revises: 67b9e9a6e203
Create Date: 2025-02-20 10:49:42.414691

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from geoalchemy2 import Geometry
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '4b1815d9aa2b'
down_revision: Union[str, None] = '67b9e9a6e203'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('current_train_state', sa.Column('update_count', sa.Integer(), nullable=True))


def downgrade() -> None:
    op.drop_column('current_train_state', 'update_count')
