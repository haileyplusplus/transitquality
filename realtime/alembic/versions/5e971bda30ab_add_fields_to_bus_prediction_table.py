"""Add fields to bus prediction table

Revision ID: 5e971bda30ab
Revises: 0ede1b68d88d
Create Date: 2025-02-28 16:00:57.062219

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from geoalchemy2 import Geometry
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '5e971bda30ab'
down_revision: Union[str, None] = '0ede1b68d88d'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('bus_prediction', sa.Column('prediction_type', sa.String(), nullable=True))
    op.add_column('bus_prediction', sa.Column('origin', sa.String(), nullable=True))
    op.add_column('bus_prediction', sa.Column('vehicle_id', sa.Integer(), nullable=True))
    op.add_column('bus_prediction', sa.Column('direction', sa.String(), nullable=True))
    op.add_column('bus_prediction', sa.Column('block_id', sa.String(), nullable=True))
    op.add_column('bus_prediction', sa.Column('delay', sa.Boolean(), nullable=True))


def downgrade() -> None:
    op.drop_column('bus_prediction', 'delay')
    op.drop_column('bus_prediction', 'block_id')
    op.drop_column('bus_prediction', 'direction')
    op.drop_column('bus_prediction', 'vehicle_id')
    op.drop_column('bus_prediction', 'origin')
    op.drop_column('bus_prediction', 'prediction_type')
