"""add direction_change field to pattern_stop

Revision ID: 0b08b3255556
Revises: d37603a63945
Create Date: 2025-02-17 17:34:29.356880

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '0b08b3255556'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('pattern_stop', sa.Column('direction_change', sa.Integer, nullable=True))


def downgrade() -> None:
    op.drop_column('pattern_stop', 'direction_change')
