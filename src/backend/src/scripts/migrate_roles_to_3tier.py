#!/usr/bin/env python3
"""
Script to migrate existing database roles from 4-tier to 3-tier system.

Run this script to update all existing role data:
- manager -> editor
- user -> operator
- viewer -> operator
- admin stays admin
"""

import asyncio
import logging
from sqlalchemy import text
from src.db.session import async_session_factory

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def migrate_roles():
    """Migrate all role data to 3-tier system."""

    async with async_session_factory() as session:
        try:
            # Update group_users table
            update_group_users = text("""
                UPDATE group_users
                SET role = CASE
                    WHEN role = 'manager' THEN 'editor'
                    WHEN role = 'user' THEN 'operator'
                    WHEN role = 'viewer' THEN 'operator'
                    ELSE role
                END
                WHERE role IN ('manager', 'user', 'viewer')
            """)

            result = await session.execute(update_group_users)
            group_users_updated = result.rowcount
            logger.info(f"Updated {group_users_updated} group_users records")

            # Check for user_roles table and update if exists
            check_user_roles = text("""
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_name = 'user_roles'
            """)

            result = await session.execute(check_user_roles)
            has_user_roles = result.scalar() > 0

            if has_user_roles:
                update_user_roles = text("""
                    UPDATE user_roles
                    SET role_name = CASE
                        WHEN role_name = 'manager' THEN 'editor'
                        WHEN role_name = 'user' THEN 'operator'
                        WHEN role_name = 'viewer' THEN 'operator'
                        ELSE role_name
                    END
                    WHERE role_name IN ('manager', 'user', 'viewer')
                """)

                result = await session.execute(update_user_roles)
                user_roles_updated = result.rowcount
                logger.info(f"Updated {user_roles_updated} user_roles records")

            # Check for roles table and update if exists
            check_roles = text("""
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_name = 'roles'
            """)

            result = await session.execute(check_roles)
            has_roles = result.scalar() > 0

            if has_roles:
                # Delete old roles
                delete_old_roles = text("""
                    DELETE FROM roles
                    WHERE name IN ('manager', 'user', 'viewer')
                """)

                result = await session.execute(delete_old_roles)
                roles_deleted = result.rowcount
                logger.info(f"Deleted {roles_deleted} old role definitions")

                # Ensure new roles exist
                check_editor = text("SELECT COUNT(*) FROM roles WHERE name = 'editor'")
                result = await session.execute(check_editor)

                if result.scalar() == 0:
                    insert_editor = text("""
                        INSERT INTO roles (id, name, description, created_at, updated_at)
                        VALUES (
                            gen_random_uuid(),
                            'editor',
                            'Workflow developer - Build and modify AI agent tasks',
                            NOW(),
                            NOW()
                        )
                    """)
                    await session.execute(insert_editor)
                    logger.info("Created 'editor' role")

                check_operator = text("SELECT COUNT(*) FROM roles WHERE name = 'operator'")
                result = await session.execute(check_operator)

                if result.scalar() == 0:
                    insert_operator = text("""
                        INSERT INTO roles (id, name, description, created_at, updated_at)
                        VALUES (
                            gen_random_uuid(),
                            'operator',
                            'Execution operator - Execute workflows and monitor',
                            NOW(),
                            NOW()
                        )
                    """)
                    await session.execute(insert_operator)
                    logger.info("Created 'operator' role")

            # Commit all changes
            await session.commit()
            logger.info("Migration completed successfully!")

        except Exception as e:
            logger.error(f"Migration failed: {e}")
            await session.rollback()
            raise


async def check_current_roles():
    """Check current role distribution before migration."""

    async with async_session_factory() as session:
        try:
            # Check group_users distribution
            check_roles = text("""
                SELECT role, COUNT(*) as count
                FROM group_users
                GROUP BY role
                ORDER BY role
            """)

            result = await session.execute(check_roles)
            rows = result.fetchall()

            logger.info("\nCurrent role distribution in group_users:")
            for row in rows:
                logger.info(f"  {row[0]}: {row[1]} users")

            return rows

        except Exception as e:
            logger.error(f"Failed to check roles: {e}")
            raise


async def main():
    """Main migration function."""
    logger.info("Starting role migration to 3-tier system...")

    # Check current state
    logger.info("\n=== Before Migration ===")
    await check_current_roles()

    # Run migration
    logger.info("\n=== Running Migration ===")
    await migrate_roles()

    # Check new state
    logger.info("\n=== After Migration ===")
    await check_current_roles()

    logger.info("\n✅ Migration completed successfully!")


if __name__ == "__main__":
    asyncio.run(main())