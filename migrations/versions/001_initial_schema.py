"""Initial schema

Revision ID: 001
Revises: 
Create Date: 2025-01-06 12:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '001'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create people table
    op.create_table('people',
        sa.Column('nconst', sa.String(length=20), nullable=False),
        sa.Column('primary_name', sa.String(length=255), nullable=False),
        sa.Column('birth_year', sa.Integer(), nullable=True),
        sa.Column('death_year', sa.Integer(), nullable=True),
        sa.Column('primary_profession', sa.Text(), nullable=True),
        sa.Column('known_for_titles', sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint('nconst')
    )
    op.create_index(op.f('ix_people_primary_name'), 'people', ['primary_name'], unique=False)

    # Create titles table
    op.create_table('titles',
        sa.Column('tconst', sa.String(length=20), nullable=False),
        sa.Column('title_type', sa.String(length=50), nullable=True),
        sa.Column('primary_title', sa.String(length=500), nullable=False),
        sa.Column('original_title', sa.String(length=500), nullable=True),
        sa.Column('is_adult', sa.Boolean(), nullable=True),
        sa.Column('start_year', sa.Integer(), nullable=True),
        sa.Column('end_year', sa.Integer(), nullable=True),
        sa.Column('runtime_minutes', sa.Integer(), nullable=True),
        sa.Column('genres', sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint('tconst')
    )
    op.create_index(op.f('ix_titles_primary_title'), 'titles', ['primary_title'], unique=False)

    # Create ratings table
    op.create_table('ratings',
        sa.Column('tconst', sa.String(length=20), nullable=False),
        sa.Column('average_rating', sa.Float(), nullable=False),
        sa.Column('num_votes', sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint('tconst')
    )

    # Create principals table
    op.create_table('principals',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('tconst', sa.String(length=20), nullable=False),
        sa.Column('ordering', sa.Integer(), nullable=False),
        sa.Column('nconst', sa.String(length=20), nullable=False),
        sa.Column('category', sa.String(length=50), nullable=True),
        sa.Column('job', sa.Text(), nullable=True),
        sa.Column('characters', sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_principals_nconst'), 'principals', ['nconst'], unique=False)
    op.create_index(op.f('ix_principals_tconst'), 'principals', ['tconst'], unique=False)

    # Create ETL runs table
    op.create_table('etl_runs',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('started_at', sa.DateTime(), nullable=False),
        sa.Column('finished_at', sa.DateTime(), nullable=True),
        sa.Column('status', sa.String(length=20), nullable=False),
        sa.Column('records_processed', sa.Integer(), nullable=True),
        sa.Column('error_message', sa.Text(), nullable=True),
        sa.Column('duration_seconds', sa.Integer(), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )

    # Create actor ratings table
    op.create_table('actor_ratings',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('primary_name', sa.String(length=255), nullable=False),
        sa.Column('profession', sa.String(length=50), nullable=False),
        sa.Column('score', sa.Float(), nullable=False),
        sa.Column('number_of_titles', sa.Integer(), nullable=False),
        sa.Column('total_runtime_minutes', sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_actor_ratings_primary_name'), 'actor_ratings', ['primary_name'], unique=False)
    op.create_index(op.f('ix_actor_ratings_profession'), 'actor_ratings', ['profession'], unique=False)
    op.create_index(op.f('ix_actor_ratings_score'), 'actor_ratings', ['score'], unique=False)


def downgrade() -> None:
    op.drop_table('actor_ratings')
    op.drop_table('etl_runs')
    op.drop_table('principals')
    op.drop_table('ratings')
    op.drop_table('titles')
    op.drop_table('people')