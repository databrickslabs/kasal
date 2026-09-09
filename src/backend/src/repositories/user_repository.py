from datetime import datetime, timezone
from typing import List, Optional

from sqlalchemy import func, or_, select, update

from src.core.base_repository import BaseRepository
from src.models.user import User


class UserRepository(BaseRepository[User]):
    """Repository for User model"""

    async def get_by_email(self, email: str) -> Optional[User]:
        """Get a user by email"""
        query = select(self.model).where(self.model.email == email)
        result = await self.session.execute(query)
        return result.scalars().first()

    async def get_by_personal_group_id(self, personal_group_id: str) -> Optional[User]:
        """The user whose personal workspace carries this id, if any."""
        query = select(self.model).where(
            self.model.personal_group_id == personal_group_id
        )
        result = await self.session.execute(query)
        return result.scalars().first()

    async def allocate_personal_group_id(
        self, user_id: str, candidate: str
    ) -> Optional[str]:
        """Only the first allocation wins, including across application workers."""
        await self.session.execute(
            update(self.model)
            .where(self.model.id == user_id, self.model.personal_group_id.is_(None))
            .values(personal_group_id=candidate)
        )
        await self.session.flush()
        result = await self.session.execute(
            select(self.model.personal_group_id).where(self.model.id == user_id)
        )
        return result.scalar_one_or_none()

    async def has_legacy_personal_collision(self, user_id: str, legacy: str) -> bool:
        normalized = self.model.email
        for separator in ("@", ".", "-", "+"):
            normalized = func.replace(normalized, separator, "_")
        result = await self.session.execute(
            select(self.model.id)
            .where(
                self.model.id != user_id,
                func.lower(normalized) == legacy.removeprefix("user_"),
                or_(
                    self.model.personal_group_id.is_(None),
                    self.model.personal_group_id == legacy,
                    self.model.personal_group_id.startswith(
                        legacy + "_", autoescape=True
                    ),
                ),
            )
            .limit(1)
        )
        return result.scalar_one_or_none() is not None

    async def get_by_username(self, username: str) -> Optional[User]:
        """Get a user by username"""
        query = select(self.model).where(self.model.username == username)
        result = await self.session.execute(query)
        return result.scalars().first()

    async def insert(self, user: User) -> User:
        """Persist a new user and flush so its ``id`` is available.

        Flush, not commit: this runs mid-transaction when adding someone to a
        group — the user row and the group association must land together.
        """
        self.session.add(user)
        await self.session.flush()
        return user

    async def update_last_login(self, user_id: str) -> None:
        """Update user's last login timestamp"""
        query = (
            update(self.model)
            .where(self.model.id == user_id)
            .values(last_login=datetime.now(timezone.utc))
        )
        await self.session.execute(query)

    async def search_users(
        self,
        search_term: str,
        limit: int = 10,
        skip: int = 0,
        filters: Optional[dict] = None,
    ) -> List[User]:
        """Search users by email or username"""
        query = (
            select(self.model)
            .where(
                or_(
                    self.model.email.icontains(search_term, autoescape=True),
                    self.model.username.icontains(search_term, autoescape=True),
                    self.model.display_name.icontains(search_term, autoescape=True),
                )
            )
            .order_by(self.model.email, self.model.id)
            .offset(skip)
            .limit(limit)
        )
        for field in ("role", "status"):
            if filters and field in filters:
                query = query.where(getattr(self.model, field) == filters[field])
        result = await self.session.execute(query)
        return list(result.scalars().all())

    async def count_system_admins(self) -> int:
        """How many users hold system-admin privileges."""
        result = await self.session.execute(
            select(func.count(self.model.id)).where(
                self.model.is_system_admin.is_(True)
            )
        )
        return result.scalar() or 0

    async def count(self) -> int:
        """Get total count of users"""
        query = select(func.count(self.model.id))
        result = await self.session.execute(query)
        return result.scalar() or 0


# UserProfileRepository removed - display_name moved to User model


# Legacy compatibility - maintain old names for backward compatibility during migration
ExternalIdentityRepository = None  # Removed - using simplified auth
RoleRepository = None  # Removed - using simplified group-based roles
PrivilegeRepository = None  # Removed - using simplified group-based roles
RolePrivilegeRepository = None  # Removed - using simplified group-based roles
UserRoleRepository = None  # Removed - using simplified group-based roles
IdentityProviderRepository = None  # Removed - using simplified auth
UserProfileRepository = None  # Removed - display_name moved to User model
