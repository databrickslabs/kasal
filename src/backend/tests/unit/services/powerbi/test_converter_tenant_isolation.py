"""Cross-tenant isolation of the PowerBI/UC converter (audit H1).

Runs ``ConverterService`` over the real repositories on in-memory SQLite, with
one row per tenant, and checks tenant B can neither read nor change tenant A's
history, jobs or saved configurations by guessing IDs.
"""

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src.core.exceptions import BadRequestError, ForbiddenError, NotFoundError
from src.db.base import Base
from src.models.conversion import (
    ConversionHistory,
    ConversionJob,
    SavedConverterConfiguration,
)
from src.models.tool import Tool
from src.schemas.conversion import (
    ConversionHistoryCreate,
    ConversionHistoryFilter,
    ConversionHistoryUpdate,
    ConversionJobCreate,
    ConversionJobStatusUpdate,
    ConversionJobUpdate,
    SavedConfigurationCreate,
    SavedConfigurationFilter,
    SavedConfigurationUpdate,
)
from src.services.powerbi.conversions import ConverterService
from src.utils.user_context import GroupContext


def _ctx(group, email):
    return GroupContext(group_ids=[group], group_email=email, email_domain="x.com")


A = _ctx("tenant-a", "alice@x.com")
A2 = _ctx("tenant-a", "arthur@x.com")  # same tenant, other user
B = _ctx("tenant-b", "bob@x.com")


@pytest_asyncio.fixture
async def session():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    tables = [
        Tool.__table__,
        ConversionHistory.__table__,
        ConversionJob.__table__,
        SavedConverterConfiguration.__table__,
    ]
    async with engine.begin() as conn:
        await conn.run_sync(lambda c: Base.metadata.create_all(c, tables=tables))
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as s:
        yield s
    await engine.dispose()


@pytest_asyncio.fixture
async def seeded(session):
    svc_a = ConverterService(session, group_context=A)
    history = await svc_a.create_history(
        ConversionHistoryCreate(
            source_format="powerbi", target_format="dax", execution_id="exec-a"
        )
    )
    job = await svc_a.create_job(
        ConversionJobCreate(
            source_format="powerbi", target_format="dax", configuration={"k": "v"}
        )
    )
    private = await svc_a.create_saved_config(
        SavedConfigurationCreate(
            name="secret",
            source_format="powerbi",
            target_format="dax",
            configuration={"client_secret": "s3cr3t"},
        )
    )
    public = await svc_a.create_saved_config(
        SavedConfigurationCreate(
            name="shared",
            source_format="powerbi",
            target_format="dax",
            configuration={},
            is_public=True,
        )
    )
    return {"history": history, "job": job, "private": private, "public": public}


@pytest.mark.asyncio
async def test_other_tenant_cannot_read_or_change_history(session, seeded):
    svc_b = ConverterService(session, group_context=B)
    hid = seeded["history"].id
    with pytest.raises(NotFoundError):
        await svc_b.get_history(hid)
    with pytest.raises(NotFoundError):
        await svc_b.update_history(hid, ConversionHistoryUpdate(status="failed"))
    listed = await svc_b.list_history(ConversionHistoryFilter(execution_id="exec-a"))
    assert listed.count == 0
    assert (await svc_b.list_history()).count == 0
    assert (await svc_b.get_statistics()).total_conversions == 0

    svc_a = ConverterService(session, group_context=A)
    assert (await svc_a.get_history(hid)).status != "failed"
    listed_a = await svc_a.list_history(ConversionHistoryFilter(execution_id="exec-a"))
    assert listed_a.count == 1


@pytest.mark.asyncio
async def test_other_tenant_cannot_read_or_change_jobs(session, seeded):
    svc_b = ConverterService(session, group_context=B)
    jid = seeded["job"].id
    with pytest.raises(NotFoundError):
        await svc_b.get_job(jid)
    with pytest.raises(NotFoundError):
        await svc_b.update_job(jid, ConversionJobUpdate(status="running"))
    with pytest.raises(NotFoundError):
        await svc_b.update_job_status(jid, ConversionJobStatusUpdate(status="failed"))
    with pytest.raises(BadRequestError):
        await svc_b.cancel_job(jid)
    assert (await svc_b.list_jobs()).count == 0

    job = await ConverterService(session, group_context=A).get_job(jid)
    assert job.status == "pending"


@pytest.mark.asyncio
async def test_other_tenant_cannot_read_use_or_change_configs(session, seeded):
    svc_b = ConverterService(session, group_context=B)
    for cfg in (seeded["private"], seeded["public"]):
        with pytest.raises(NotFoundError):
            await svc_b.get_saved_config(cfg.id)
        with pytest.raises(NotFoundError):
            await svc_b.use_saved_config(cfg.id)
        with pytest.raises(NotFoundError):
            await svc_b.update_saved_config(
                cfg.id, SavedConfigurationUpdate(name="pwned")
            )
        with pytest.raises(NotFoundError):
            await svc_b.delete_saved_config(cfg.id)
    listed = await svc_b.list_saved_configs(SavedConfigurationFilter(is_public=True))
    assert listed.count == 0

    svc_a = ConverterService(session, group_context=A)
    assert (await svc_a.get_saved_config(seeded["private"].id)).name == "secret"


@pytest.mark.asyncio
async def test_same_tenant_sees_public_but_not_private_and_cannot_edit(session, seeded):
    svc = ConverterService(session, group_context=A2)
    assert (await svc.get_saved_config(seeded["public"].id)).name == "shared"
    with pytest.raises(NotFoundError):
        await svc.get_saved_config(seeded["private"].id)
    with pytest.raises(ForbiddenError):
        await svc.update_saved_config(
            seeded["public"].id, SavedConfigurationUpdate(name="x")
        )
    with pytest.raises(ForbiddenError):
        await svc.delete_saved_config(seeded["public"].id)


@pytest.mark.asyncio
async def test_no_group_context_gets_nothing(session, seeded):
    svc = ConverterService(session, group_context=None)
    with pytest.raises(NotFoundError):
        await svc.get_history(seeded["history"].id)
    with pytest.raises(NotFoundError):
        await svc.get_job(seeded["job"].id)
    with pytest.raises(NotFoundError):
        await svc.get_saved_config(seeded["public"].id)
    with pytest.raises(NotFoundError):
        await svc.update_saved_config(
            seeded["public"].id, SavedConfigurationUpdate(name="x")
        )
    assert (await svc.list_history()).count == 0
    assert (await svc.list_jobs()).count == 0
    assert (await svc.get_statistics()).total_conversions == 0
    listed = await svc.list_saved_configs(SavedConfigurationFilter(is_public=True))
    assert listed.count == 0


@pytest.mark.asyncio
async def test_owner_can_still_update_and_delete(session, seeded):
    svc_a = ConverterService(session, group_context=A)
    cid = seeded["private"].id
    updated = await svc_a.update_saved_config(
        cid, SavedConfigurationUpdate(name="renamed")
    )
    assert updated.name == "renamed"
    used = await svc_a.use_saved_config(cid)
    assert used.use_count == 1
    await svc_a.delete_saved_config(cid)
    with pytest.raises(NotFoundError):
        await svc_a.get_saved_config(cid)
