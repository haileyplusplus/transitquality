"""Track crow-flies distance to next stop

Revision ID: 0ede1b68d88d
Revises: 4b1815d9aa2b
Create Date: 2025-02-28 11:47:59.724102

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from geoalchemy2 import Geometry
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '0ede1b68d88d'
down_revision: Union[str, None] = '4b1815d9aa2b'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('train_position', sa.Column('next_stop_distance', sa.Integer(), nullable=True))


def downgrade() -> None:
    op.drop_column('train_position', 'next_stop_distance')
