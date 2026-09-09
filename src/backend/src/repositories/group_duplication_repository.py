"""Copy the explicit teamspace configuration set into a fresh group identity.

History, schedules, publications, user identities and global settings are not
configuration templates. Keep the list explicit so new runtime tables cannot
accidentally become part of duplication.
"""

from copy import deepcopy
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.a2a_agent import A2AAgent
from src.models.api_key import ApiKey
from src.models.databricks_config import DatabricksConfig
from src.models.group import GroupUser
from src.models.group_tool import GroupTool
from src.models.mcp_server import MCPServer
from src.models.memory_backend import MemoryBackend
from src.models.mlflow_config import MLflowConfig
from src.models.model_billing_rate import ModelBillingRate
from src.models.model_config import ModelConfig
from src.models.powerbi_config import PowerBIConfig
from src.models.powerbi_context_config import (
    PowerBIBusinessMapping,
    PowerBIFieldSynonym,
)
from src.models.skill import Skill, SkillFile
from src.models.template import PromptTemplate
from src.models.tool import Tool
from src.models.ui_config import UIConfig


CONFIGURATION_MODELS = (
    ApiKey,
    DatabricksConfig,
    PowerBIConfig,
    MLflowConfig,
    ModelConfig,
    ModelBillingRate,
    UIConfig,
    MCPServer,
    A2AAgent,
    MemoryBackend,
    PromptTemplate,
    PowerBIBusinessMapping,
    PowerBIFieldSynonym,
)


class GroupDuplicationRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def _rows(self, model, source_id: str):
        result = await self.session.execute(
            select(model).where(model.group_id == source_id)
        )
        return result.scalars().unique().all()

    def _copy(self, row, target_id: str, actor_email: str, **overrides):
        model = type(row)
        data = {
            column.key: deepcopy(getattr(row, column.key))
            for column in model.__table__.columns
            if column.key
            not in {"id", "created_at", "updated_at", "created_by_email", "group_id"}
        }
        if "group_id" in model.__table__.columns:
            data["group_id"] = target_id
        if "created_by_email" in model.__table__.columns:
            data["created_by_email"] = actor_email
        for field in ("created_at", "updated_at"):
            if field in model.__table__.columns:
                data[field] = datetime.now(timezone.utc)
        data.update(overrides)
        copied = model(**data)
        self.session.add(copied)
        return copied

    async def copy_configuration(
        self, source_id: str, target_id: str, actor_email: str
    ):
        # Preserve encrypted credentials as ciphertext; duplication never returns
        # them to a client or forwards them to an external service.
        for model in CONFIGURATION_MODELS:
            for row in await self._rows(model, source_id):
                overrides = {}
                if model is A2AAgent:
                    overrides = {
                        "cached_card": None,
                        "card_fetched_at": None,
                        "last_error": None,
                    }
                self._copy(row, target_id, actor_email, **overrides)

        # Workspace-owned tools get new IDs; catalog tool mappings keep their
        # shared catalog IDs. A source mapping must never point to the old
        # workspace's tool after copying.
        tool_ids = {}
        for tool in await self._rows(Tool, source_id):
            copied = self._copy(tool, target_id, actor_email)
            await self.session.flush()
            tool_ids[tool.id] = copied.id
        for mapping in await self._rows(GroupTool, source_id):
            self._copy(
                mapping,
                target_id,
                actor_email,
                tool_id=tool_ids.get(mapping.tool_id, mapping.tool_id),
                credentials_status="unknown",
            )

        for skill in await self._rows(Skill, source_id):
            copied = self._copy(skill, target_id, actor_email)
            await self.session.flush()
            result = await self.session.execute(
                select(SkillFile).where(SkillFile.skill_id == skill.id)
            )
            for file in result.scalars():
                self._copy(file, target_id, actor_email, skill_id=copied.id)
        await self.session.flush()

    async def copy_members(
        self,
        source_id: str,
        target_id: str,
        actor_id: str,
        actor_email: str,
        include_members: bool,
    ):
        if include_members:
            for membership in await self._rows(GroupUser, source_id):
                if membership.user_id == actor_id:
                    continue
                self._copy(
                    membership,
                    target_id,
                    actor_email,
                    auto_created=False,
                    joined_at=datetime.now(timezone.utc),
                )
        # The system-admin creator can select the new teamspace, including when
        # their membership in the source is inactive or members were excluded.
        self.session.add(
            GroupUser(
                group_id=target_id,
                user_id=actor_id,
                role="admin",
                status="active",
                auto_created=False,
                joined_at=datetime.now(timezone.utc),
                created_at=datetime.now(timezone.utc),
                updated_at=datetime.now(timezone.utc),
            )
        )
        await self.session.flush()
