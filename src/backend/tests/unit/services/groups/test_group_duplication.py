"""A duplicate is independent configuration, committed as a single unit."""

import os
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.schema import CreateSchema, DropSchema

from src.core.exceptions import BadRequestError, NotFoundError
from src.db.base import Base
from src.models.agent import Agent
from src.models.group import Group, GroupUser
from src.models.group_tool import GroupTool
from src.models.skill import Skill, SkillFile
from src.models.tool import Tool
from src.models.user import User
from src.repositories.group_duplication_repository import (
    CONFIGURATION_MODELS,
    GroupDuplicationRepository,
    A2AAgent,
    ApiKey,
    DatabricksConfig,
    MCPServer,
    MemoryBackend,
    MLflowConfig,
    ModelBillingRate,
    ModelConfig,
    PowerBIConfig,
    PowerBIBusinessMapping,
    PowerBIFieldSynonym,
    PromptTemplate,
    UIConfig,
)
from src.schemas.group import GroupDuplicateRequest
from src.services.groups.duplication import GroupDuplicationService


@pytest_asyncio.fixture(params=["sqlite", "postgres"])
async def database(request):
    schema = None
    if request.param == "postgres":
        url = os.getenv("KASAL_TEST_DUPLICATION_POSTGRES_URL")
        if not url:
            pytest.skip(
                "Set KASAL_TEST_DUPLICATION_POSTGRES_URL to run against PostgreSQL"
            )
        # Each test owns a fresh schema; never drop or modify pre-existing tables.
        schema = f"test_group_copy_{uuid4().hex}"
        engine = create_async_engine(
            url, connect_args={"server_settings": {"search_path": schema}}
        )
    else:
        engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    models = (
        *CONFIGURATION_MODELS,
        User,
        Group,
        GroupUser,
        Tool,
        GroupTool,
        Skill,
        SkillFile,
        Agent,
    )
    async with engine.begin() as connection:
        if schema:
            await connection.execute(CreateSchema(schema))
        await connection.run_sync(
            lambda sync: Base.metadata.create_all(
                sync, tables=[model.__table__ for model in models]
            )
        )
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session:
        session.add_all(
            [
                User(
                    id="admin",
                    username="admin",
                    email="admin@example.com",
                    is_system_admin=True,
                ),
                User(id="member", username="member", email="member@example.com"),
                Group(id="source", name="Source", status="active"),
                Group(id="other", name="Other", status="active"),
            ]
        )
        await session.flush()
        session.add_all(
            [
                GroupUser(
                    group_id="source",
                    user_id="member",
                    role="operator",
                    status="active",
                    allow_agent_builder=True,
                    allow_flow_builder=False,
                ),
                Tool(
                    title="Catalog",
                    description="Global",
                    icon="tool",
                    group_id=None,
                ),
                Tool(
                    title="Private",
                    description="Owned",
                    icon="tool",
                    group_id="source",
                    config={"nested": {"value": 1}},
                ),
                Tool(
                    title="Other",
                    description="Other",
                    icon="tool",
                    group_id="other",
                ),
                Skill(
                    group_id="source",
                    name="team-guide",
                    description="Guide",
                    body="Instructions",
                ),
            ]
        )
        await session.flush()
        session.add_all(
            [
                GroupTool(
                    tool_id=1,
                    group_id="source",
                    enabled=False,
                    config={"api_key": "stored-secret", "require_approval": True},
                    credentials_status="valid",
                ),
                GroupTool(
                    tool_id=2, group_id="source", enabled=True, config={"setting": 42}
                ),
                SkillFile(skill_id=1, path="references/guide.md", content="Reference"),
                ApiKey(name="KEY", encrypted_value="ciphertext", group_id="source"),
                ApiKey(
                    name="OTHER_KEY",
                    encrypted_value="other-ciphertext",
                    group_id="other",
                ),
                DatabricksConfig(
                    group_id="source",
                    warehouse_id="warehouse",
                    catalog="catalog",
                    schema="schema",
                    encrypted_personal_access_token="encrypted-pat",
                ),
                PowerBIConfig(
                    group_id="source",
                    tenant_id="tenant",
                    client_id="client",
                    encrypted_client_secret="encrypted-secret",
                ),
                MLflowConfig(group_id="source", enabled=True, evaluation_enabled=True),
                ModelConfig(
                    group_id="source",
                    key="model",
                    name="Model",
                    reasoning_effort="high",
                ),
                ModelBillingRate(
                    group_id="source",
                    model="model",
                    input_per_million=1,
                    output_per_million=2,
                ),
                UIConfig(group_id="source", catalog_type="minimal"),
                MCPServer(
                    group_id="source",
                    name="MCP",
                    server_url="https://example.com/mcp",
                    encrypted_api_key="encrypted-mcp",
                    enabled=True,
                ),
                A2AAgent(
                    group_id="source",
                    name="Remote",
                    card_url="https://example.com/agent",
                    cached_card={"stale": True},
                ),
                MemoryBackend(
                    group_id="source",
                    name="Memory",
                    cognitive_config={"recall_max_depth": 4},
                ),
                PromptTemplate(
                    group_id="source", name="Prompt", template="Team prompt"
                ),
                PowerBIBusinessMapping(
                    group_id="source",
                    semantic_model_id="dataset",
                    natural_term="Revenue",
                    dax_expression="[Revenue]",
                ),
                PowerBIFieldSynonym(
                    group_id="source",
                    semantic_model_id="dataset",
                    field_name="Revenue",
                    synonyms=["Sales"],
                ),
                Agent(
                    group_id="source",
                    name="Saved agent",
                    role="Researcher",
                    goal="Research",
                ),
            ]
        )
        await session.commit()
    try:
        yield factory
    finally:
        if schema:
            async with engine.begin() as connection:
                await connection.execute(DropSchema(schema, cascade=True))
        await engine.dispose()


ACTOR = SimpleNamespace(id="admin", email="admin@example.com")


async def rows(session, model, group_id):
    return (
        (await session.execute(select(model).where(model.group_id == group_id)))
        .scalars()
        .unique()
        .all()
    )


@pytest.mark.asyncio
async def test_group_tool_copy_inserts_timestamp_values(database):
    async with database() as session:
        original = (await rows(session, GroupTool, "source"))[0]
        copied = GroupDuplicationRepository(session)._copy(
            original, "other", ACTOR.email, credentials_status="unknown"
        )
        # Check the driver input too: SQLite silently accepts an aware value
        # here, while asyncpg rejects it before executing the INSERT.
        assert copied.created_at.tzinfo is None
        assert copied.updated_at.tzinfo is None
        await session.commit()
        await session.refresh(copied)
        assert copied.created_at is not None
        assert copied.updated_at is not None
        assert copied.credentials_status == "unknown"


@pytest.mark.asyncio
async def test_copy_configuration_and_members_with_independent_ids(database):
    async with database() as session:
        target = await GroupDuplicationService(session).duplicate(
            "source", GroupDuplicateRequest(name="New Team"), ACTOR
        )
    async with database() as session:
        assert target.id != "source"
        assert target.user_count == 2
        for model in CONFIGURATION_MODELS:
            copies = await rows(session, model, target.id)
            assert len(copies) == 1, model.__name__
            if hasattr(model, "id"):
                assert copies[0].id != (await rows(session, model, "source"))[0].id
        assert (await rows(session, ApiKey, target.id))[
            0
        ].encrypted_value == "ciphertext"
        memberships = {
            member.user_id: member
            for member in await rows(session, GroupUser, target.id)
        }
        assert memberships["member"].role == "operator"
        assert memberships["member"].allow_agent_builder is True
        assert memberships["member"].allow_flow_builder is False
        assert memberships["admin"].role == "admin"
        assert await session.scalar(select(func.count(User.id))) == 2
        tool = (await rows(session, Tool, target.id))[0]
        assert tool.title == "Private" and tool.id != 2
        mappings = {
            mapping.tool_id: mapping
            for mapping in await rows(session, GroupTool, target.id)
        }
        assert set(mappings) == {1, tool.id}
        assert mappings[1].enabled is False
        assert mappings[1].config["require_approval"] is True
        assert mappings[1].credentials_status == "unknown"
        skill = (await rows(session, Skill, target.id))[0]
        assert skill.id != 1
        assert skill.files[0].content == "Reference"
        assert skill.files[0].skill_id == skill.id
        assert (await rows(session, A2AAgent, target.id))[0].cached_card is None
        assert await rows(session, Agent, target.id) == []
        tool.config = {"nested": {"value": 2}}
        await session.commit()
    async with database() as session:
        assert (await session.get(Tool, 2)).config == {"nested": {"value": 1}}
        assert len(await rows(session, ApiKey, "other")) == 1


@pytest.mark.asyncio
async def test_copy_without_members_still_adds_creator(database):
    async with database() as session:
        target = await GroupDuplicationService(session).duplicate(
            "source", GroupDuplicateRequest(name="Solo", include_members=False), ACTOR
        )
        assert [
            member.user_id for member in await rows(session, GroupUser, target.id)
        ] == ["admin"]


@pytest.mark.asyncio
async def test_creator_with_inactive_source_membership_can_select_copy(database):
    async with database() as session:
        session.add(
            GroupUser(
                group_id="source", user_id="admin", role="operator", status="inactive"
            )
        )
        await session.commit()
        target = await GroupDuplicationService(session).duplicate(
            "source", GroupDuplicateRequest(name="Active copy"), ACTOR
        )
        copied = {
            member.user_id: member
            for member in await rows(session, GroupUser, target.id)
        }
        assert copied["admin"].role == "admin"
        assert copied["admin"].status == "active"
        assert len(copied) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["copy_members", "commit"])
async def test_failed_copy_rolls_back_every_created_row(database, failure):
    async with database() as session:
        service = GroupDuplicationService(session)
        owner = service.repository if failure == "copy_members" else session
        with patch.object(
            owner, failure, AsyncMock(side_effect=RuntimeError("failed"))
        ):
            with pytest.raises(RuntimeError, match="failed"):
                await service.duplicate(
                    "source", GroupDuplicateRequest(name="Failure"), ACTOR
                )
    async with database() as session:
        assert await session.scalar(select(func.count(Group.id))) == 2
        assert await session.scalar(select(func.count(ApiKey.id))) == 2
        assert await session.scalar(select(func.count(Tool.id))) == 3
        assert await session.scalar(select(func.count(Skill.id))) == 1


@pytest.mark.asyncio
async def test_missing_and_personal_sources_are_rejected(database):
    async with database() as session:
        service = GroupDuplicationService(session)
        with pytest.raises(NotFoundError):
            await service.duplicate("missing", GroupDuplicateRequest(name="New"), ACTOR)
        admin = await session.get(User, "admin")
        admin.personal_group_id = "source"
        await session.commit()
        with pytest.raises(BadRequestError):
            await service.duplicate("source", GroupDuplicateRequest(name="New"), ACTOR)
