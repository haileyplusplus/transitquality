"""add stop_headsign field to pattern_stop

Revision ID: 6def7d3064d9
Revises: 0b08b3255556
Create Date: 2025-02-17 21:12:57.962592

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '6def7d3064d9'
down_revision: Union[str, None] = '0b08b3255556'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('pattern_stop', sa.Column('stop_headsign', sa.String, nullable=True))


def downgrade() -> None:
    op.drop_column('pattern_stop', 'stop_headsign')
