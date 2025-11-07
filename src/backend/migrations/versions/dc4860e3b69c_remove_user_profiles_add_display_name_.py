"""remove_user_profiles_add_display_name_to_users

Revision ID: dc4860e3b69c
Revises: 8cae7b637c8b
Create Date: 2025-09-21 15:04:23.041515

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'dc4860e3b69c'
down_revision: Union[str, None] = '8cae7b637c8b'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add display_name column to users table
    op.add_column('users', sa.Column('display_name', sa.String(), nullable=True))

    # Migrate existing display_name data from user_profiles to users
    op.execute("""
        UPDATE users
        SET display_name = (
            SELECT user_profiles.display_name
            FROM user_profiles
            WHERE user_profiles.user_id = users.id
        )
        WHERE EXISTS (
            SELECT 1 FROM user_profiles WHERE user_profiles.user_id = users.id
        )
    """)

    # Set display_name to username for users without profiles
    op.execute("""
        UPDATE users
        SET display_name = username
        WHERE display_name IS NULL
    """)

    # Drop user_profiles table
    op.drop_table('user_profiles')


def downgrade() -> None:
    # Recreate user_profiles table
    op.create_table('user_profiles',
        sa.Column('id', sa.String(), nullable=False),
        sa.Column('user_id', sa.String(), nullable=True),
        sa.Column('display_name', sa.String(), nullable=True),
        sa.Column('avatar_url', sa.String(), nullable=True),
        sa.Column('preferences', sa.String(), nullable=True),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('user_id')
    )

    # Migrate display_name data back to user_profiles
    op.execute("""
        INSERT INTO user_profiles (id, user_id, display_name)
        SELECT
            REPLACE(CAST(RANDOM() * 1000000000 AS TEXT), '.', '') || '-' ||
            SUBSTR('abcdefghijklmnopqrstuvwxyz', CAST(RANDOM() * 26 + 1 AS INT), 1) ||
            SUBSTR('abcdefghijklmnopqrstuvwxyz', CAST(RANDOM() * 26 + 1 AS INT), 1) ||
            SUBSTR('abcdefghijklmnopqrstuvwxyz', CAST(RANDOM() * 26 + 1 AS INT), 1) ||
            SUBSTR('abcdefghijklmnopqrstuvwxyz', CAST(RANDOM() * 26 + 1 AS INT), 1),
            id,
            display_name
        FROM users
        WHERE display_name IS NOT NULL
    """)

    # Remove display_name column from users table
    op.drop_column('users', 'display_name') 