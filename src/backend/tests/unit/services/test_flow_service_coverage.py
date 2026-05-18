"""
Additional coverage tests for flow_service.py targeting uncovered lines.
Missing: create_flow_with_group (invalid listener/action, crew not found),
get_flow_with_group_check, get_all_flows_for_group (no groups),
update_flow_with_group_check, delete_all_flows_for_group,
get_flows_by_crew (invalid uuid), update_flow (nodes/edges),
force_delete_flow_with_executions, force_delete_flow_with_executions_with_group_check,
create_flow (invalid listener/action), delete_flow, validate_flow_data error.
"""
import uuid
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from types import SimpleNamespace
from datetime import datetime

from src.core.exceptions import NotFoundError, ForbiddenError, ConflictError, KasalError, BadRequestError


def make_session():
    session = AsyncMock()
    session.execute = AsyncMock()
    session.rollback = AsyncMock()
    session.commit = AsyncMock()
    return session


def make_flow(flow_id=None, name="test", group_id="g1", crew_id=None, nodes=None, edges=None, flow_config=None):
    f = MagicMock()
    f.id = flow_id or uuid.uuid4()
    f.name = name
    f.group_id = group_id
    f.crew_id = crew_id
    f.nodes = nodes or []
    f.edges = edges or []
    f.flow_config = flow_config or {}
    f.updated_at = datetime.utcnow()
    return f


def make_flow_create(**kwargs):
    from src.schemas.flow import FlowCreate
    return FlowCreate(
        name=kwargs.get("name", "My Flow"),
        crew_id=kwargs.get("crew_id", None),
        nodes=kwargs.get("nodes", []),
        edges=kwargs.get("edges", []),
        flow_config=kwargs.get("flow_config", {}),
    )


def make_flow_update(**kwargs):
    from src.schemas.flow import FlowUpdate
    return FlowUpdate(
        name=kwargs.get("name", "Updated"),
        nodes=kwargs.get("nodes", None),
        edges=kwargs.get("edges", None),
        flow_config=kwargs.get("flow_config", None),
    )


def make_group_context(group_ids=None, email="user@example.com"):
    ctx = SimpleNamespace(
        group_ids=group_ids or ["g1"],
        group_email=email
    )
    return ctx


def make_service(session=None):
    from src.services.flow_service import FlowService
    return FlowService(session=session or make_session())


# ---------------------------------------------------------------------------
# create_flow_with_group - validation paths
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_create_flow_with_group_invalid_listener():
    svc = make_service()
    group_ctx = make_group_context()

    with patch("src.services.flow_service.FlowRepository") as MockRepo:
        mock_repo = AsyncMock()
        mock_repo.find_by_name_and_group = AsyncMock(return_value=None)
        MockRepo.return_value = mock_repo

        flow_in = make_flow_create(flow_config={"listeners": ["not_a_dict"]})
        with pytest.raises(BadRequestError):
            await svc.create_flow_with_group(flow_in, group_ctx)


@pytest.mark.asyncio
async def test_create_flow_with_group_invalid_action():
    svc = make_service()
    group_ctx = make_group_context()

    with patch("src.services.flow_service.FlowRepository") as MockRepo:
        mock_repo = AsyncMock()
        mock_repo.find_by_name_and_group = AsyncMock(return_value=None)
        MockRepo.return_value = mock_repo

        flow_in = make_flow_create(flow_config={"actions": ["not_a_dict"]})
        with pytest.raises(BadRequestError):
            await svc.create_flow_with_group(flow_in, group_ctx)


@pytest.mark.asyncio
async def test_create_flow_with_group_duplicate_name():
    svc = make_service()
    group_ctx = make_group_context()

    with patch("src.services.flow_service.FlowRepository") as MockRepo:
        mock_repo = AsyncMock()
        existing = make_flow(name="My Flow")
        mock_repo.find_by_name_and_group = AsyncMock(return_value=existing)
        MockRepo.return_value = mock_repo

        flow_in = make_flow_create(name="My Flow")
        with pytest.raises(ConflictError):
            await svc.create_flow_with_group(flow_in, group_ctx)


@pytest.mark.asyncio
async def test_create_flow_with_group_crew_not_found():
    svc = make_service()
    group_ctx = make_group_context()

    import src.repositories.crew_repository as crew_repo_mod
    orig_crew_repo = crew_repo_mod.CrewRepository

    mock_crew_repo_instance = AsyncMock()
    mock_crew_repo_instance.get = AsyncMock(return_value=None)

    with patch("src.services.flow_service.FlowRepository") as MockFlowRepo:
        flow_repo = AsyncMock()
        flow_repo.find_by_name_and_group = AsyncMock(return_value=None)
        flow_repo.create = AsyncMock(return_value=make_flow())
        MockFlowRepo.return_value = flow_repo

        crew_repo_mod.CrewRepository = lambda s: mock_crew_repo_instance
        try:
            flow_in = make_flow_create(crew_id=str(uuid.uuid4()))
            result = await svc.create_flow_with_group(flow_in, group_ctx)
        finally:
            crew_repo_mod.CrewRepository = orig_crew_repo

    assert result is not None


@pytest.mark.asyncio
async def test_create_flow_with_group_crew_found():
    svc = make_service()
    group_ctx = make_group_context()
    crew_id = str(uuid.uuid4())

    import src.repositories.crew_repository as crew_repo_mod
    orig_crew_repo = crew_repo_mod.CrewRepository

    mock_crew = MagicMock()
    mock_crew_repo_instance = AsyncMock()
    mock_crew_repo_instance.get = AsyncMock(return_value=mock_crew)

    with patch("src.services.flow_service.FlowRepository") as MockFlowRepo:
        flow_repo = AsyncMock()
        flow_repo.find_by_name_and_group = AsyncMock(return_value=None)
        flow_repo.create = AsyncMock(return_value=make_flow(crew_id=crew_id))
        MockFlowRepo.return_value = flow_repo

        crew_repo_mod.CrewRepository = lambda s: mock_crew_repo_instance
        try:
            flow_in = make_flow_create(crew_id=crew_id)
            result = await svc.create_flow_with_group(flow_in, group_ctx)
        finally:
            crew_repo_mod.CrewRepository = orig_crew_repo

    assert result is not None


@pytest.mark.asyncio
async def test_create_flow_with_group_no_group_context():
    svc = make_service()

    with patch("src.services.flow_service.FlowRepository") as MockFlowRepo:
        flow_repo = AsyncMock()
        flow_repo.create = AsyncMock(return_value=make_flow())
        MockFlowRepo.return_value = flow_repo

        flow_in = make_flow_create()
        result = await svc.create_flow_with_group(flow_in, None)
        assert result is not None


@pytest.mark.asyncio
async def test_create_flow_with_group_generic_exception():
    svc = make_service()
    group_ctx = make_group_context()

    with patch("src.services.flow_service.FlowRepository") as MockFlowRepo:
        flow_repo = AsyncMock()
        flow_repo.find_by_name_and_group = AsyncMock(return_value=None)
        flow_repo.create = AsyncMock(side_effect=RuntimeError("DB crash"))
        MockFlowRepo.return_value = flow_repo

        flow_in = make_flow_create()
        with pytest.raises(KasalError):
            await svc.create_flow_with_group(flow_in, group_ctx)


# ---------------------------------------------------------------------------
# get_flow_with_group_check
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_flow_with_group_check_not_found():
    svc = make_service()

    with patch("src.services.flow_service.FlowRepository") as MockRepo:
        mock_repo = AsyncMock()
        mock_repo.get = AsyncMock(return_value=None)
        MockRepo.return_value = mock_repo

        flow_id = uuid.uuid4()
        with pytest.raises(NotFoundError):
            await svc.get_flow_with_group_check(flow_id, make_group_context())


@pytest.mark.asyncio
async def test_get_flow_with_group_check_forbidden():
    svc = make_service()

    with patch("src.services.flow_service.FlowRepository") as MockRepo:
        mock_repo = AsyncMock()
        flow = make_flow(group_id="different-group")
        mock_repo.get = AsyncMock(return_value=flow)
        MockRepo.return_value = mock_repo

        group_ctx = make_group_context(group_ids=["my-group"])
        with pytest.raises(ForbiddenError):
            await svc.get_flow_with_group_check(flow.id, group_ctx)


@pytest.mark.asyncio
async def test_get_flow_with_group_check_success():
    svc = make_service()

    with patch("src.services.flow_service.FlowRepository") as MockRepo:
        mock_repo = AsyncMock()
        flow = make_flow(group_id="g1")
        mock_repo.get = AsyncMock(return_value=flow)
        MockRepo.return_value = mock_repo

        group_ctx = make_group_context(group_ids=["g1"])
        result = await svc.get_flow_with_group_check(flow.id, group_ctx)
        assert result.id == flow.id


@pytest.mark.asyncio
async def test_get_flow_with_group_check_no_group_id_in_flow():
    """Flow has no group_id - should be accessible."""
    svc = make_service()

    with patch("src.services.flow_service.FlowRepository") as MockRepo:
        mock_repo = AsyncMock()
        flow = make_flow(group_id=None)
        mock_repo.get = AsyncMock(return_value=flow)
        MockRepo.return_value = mock_repo

        group_ctx = make_group_context(group_ids=["g1"])
        result = await svc.get_flow_with_group_check(flow.id, group_ctx)
        assert result is not None


# ---------------------------------------------------------------------------
# get_all_flows_for_group
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_all_flows_for_group_no_context():
    svc = make_service()
    result = await svc.get_all_flows_for_group(None)
    assert result == []


@pytest.mark.asyncio
async def test_get_all_flows_for_group_empty_group_ids():
    svc = make_service()
    ctx = SimpleNamespace(group_ids=[])
    result = await svc.get_all_flows_for_group(ctx)
    assert result == []


@pytest.mark.asyncio
async def test_get_all_flows_for_group_success():
    session = make_session()
    svc = make_service(session=session)

    flows = [make_flow(group_id="g1"), make_flow(group_id="g2")]
    mock_result = MagicMock()
    mock_scalars = MagicMock()
    mock_scalars.all.return_value = flows
    mock_result.scalars.return_value = mock_scalars
    session.execute = AsyncMock(return_value=mock_result)

    ctx = make_group_context(group_ids=["g1", "g2"])
    result = await svc.get_all_flows_for_group(ctx)
    assert len(result) == 2


# ---------------------------------------------------------------------------
# update_flow_with_group_check
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_update_flow_with_group_check_not_found():
    svc = make_service()

    with patch("src.services.flow_service.FlowRepository") as MockRepo:
        mock_repo = AsyncMock()
        mock_repo.get = AsyncMock(return_value=None)
        MockRepo.return_value = mock_repo

        flow_id = uuid.uuid4()
        with pytest.raises(NotFoundError):
            await svc.update_flow_with_group_check(flow_id, make_flow_update(), make_group_context())


@pytest.mark.asyncio
async def test_update_flow_with_group_check_forbidden():
    svc = make_service()

    with patch("src.services.flow_service.FlowRepository") as MockRepo:
        mock_repo = AsyncMock()
        flow = make_flow(group_id="other-group")
        mock_repo.get = AsyncMock(return_value=flow)
        MockRepo.return_value = mock_repo

        group_ctx = make_group_context(group_ids=["my-group"])
        with pytest.raises(ForbiddenError):
            await svc.update_flow_with_group_check(flow.id, make_flow_update(), group_ctx)


@pytest.mark.asyncio
async def test_update_flow_with_group_check_duplicate_name():
    svc = make_service()

    with patch("src.services.flow_service.FlowRepository") as MockRepo:
        mock_repo = AsyncMock()
        flow = make_flow(group_id="g1", name="Old Name")
        duplicate = make_flow(group_id="g1", name="New Name")
        mock_repo.get = AsyncMock(return_value=flow)
        mock_repo.find_by_name_and_group = AsyncMock(return_value=duplicate)
        MockRepo.return_value = mock_repo

        group_ctx = make_group_context(group_ids=["g1"])
        update = make_flow_update(name="New Name")
        with pytest.raises(ConflictError):
            await svc.update_flow_with_group_check(flow.id, update, group_ctx)


@pytest.mark.asyncio
async def test_update_flow_with_group_check_success():
    svc = make_service()
    flow_id = uuid.uuid4()
    updated_flow = make_flow(flow_id=flow_id, name="Updated", group_id="g1")

    with patch("src.services.flow_service.FlowRepository") as MockRepo:
        mock_repo = AsyncMock()
        existing = make_flow(flow_id=flow_id, name="Old", group_id="g1")
        mock_repo.get = AsyncMock(return_value=existing)
        mock_repo.find_by_name_and_group = AsyncMock(return_value=None)
        mock_repo.update = AsyncMock(return_value=updated_flow)
        MockRepo.return_value = mock_repo

        group_ctx = make_group_context(group_ids=["g1"])
        update = make_flow_update(name="Updated")
        result = await svc.update_flow_with_group_check(flow_id, update, group_ctx)
        assert result.name == "Updated"


# ---------------------------------------------------------------------------
# delete_all_flows_for_group
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_delete_all_flows_for_group_no_context():
    svc = make_service()
    # Should return without error
    await svc.delete_all_flows_for_group(None)


@pytest.mark.asyncio
async def test_delete_all_flows_for_group_empty_groups():
    svc = make_service()
    ctx = SimpleNamespace(group_ids=[])
    await svc.delete_all_flows_for_group(ctx)


@pytest.mark.asyncio
async def test_delete_all_flows_for_group_success():
    session = make_session()
    svc = make_service(session=session)

    flows = [make_flow(flow_id=uuid.uuid4()), make_flow(flow_id=uuid.uuid4())]
    mock_result = MagicMock()
    mock_scalars = MagicMock()
    mock_scalars.all.return_value = flows
    mock_result.scalars.return_value = mock_scalars
    session.execute = AsyncMock(return_value=mock_result)

    with patch.object(svc, "force_delete_flow_with_executions", new_callable=AsyncMock) as mock_delete:
        mock_delete.return_value = True
        ctx = make_group_context(group_ids=["g1"])
        await svc.delete_all_flows_for_group(ctx)
        assert mock_delete.call_count == 2


@pytest.mark.asyncio
async def test_delete_all_flows_for_group_error_swallowed():
    session = make_session()
    svc = make_service(session=session)

    flows = [make_flow(flow_id=uuid.uuid4())]
    mock_result = MagicMock()
    mock_scalars = MagicMock()
    mock_scalars.all.return_value = flows
    mock_result.scalars.return_value = mock_scalars
    session.execute = AsyncMock(return_value=mock_result)

    with patch.object(svc, "force_delete_flow_with_executions", new_callable=AsyncMock) as mock_delete:
        mock_delete.side_effect = RuntimeError("delete failed")
        ctx = make_group_context(group_ids=["g1"])
        # Should not raise
        await svc.delete_all_flows_for_group(ctx)


# ---------------------------------------------------------------------------
# get_flows_by_crew
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_flows_by_crew_invalid_uuid():
    svc = make_service()
    result = await svc.get_flows_by_crew("not-a-valid-uuid")
    assert result == []


@pytest.mark.asyncio
async def test_get_flows_by_crew_string_uuid():
    svc = make_service()
    crew_id = str(uuid.uuid4())

    with patch("src.services.flow_service.FlowRepository") as MockRepo:
        mock_repo = AsyncMock()
        flow = make_flow(crew_id=crew_id)
        mock_repo.find_by_crew_id = AsyncMock(return_value=[flow])
        MockRepo.return_value = mock_repo

        result = await svc.get_flows_by_crew(crew_id)
        assert len(result) == 1


# ---------------------------------------------------------------------------
# update_flow - with nodes and edges
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_update_flow_with_nodes_and_edges():
    svc = make_service()
    flow_id = uuid.uuid4()
    updated_flow = make_flow(flow_id=flow_id)

    with patch("src.services.flow_service.FlowRepository") as MockRepo:
        mock_repo = AsyncMock()
        existing = make_flow(flow_id=flow_id)
        mock_repo.get = AsyncMock(return_value=existing)
        mock_repo.update = AsyncMock(return_value=updated_flow)
        MockRepo.return_value = mock_repo

        from src.schemas.flow import FlowUpdate, Node, Edge, NodeData
        nodes = [Node(id="n1", type="crewNode", position={"x": 0, "y": 0}, data={"label": "n1"})]
        edges = [Edge(id="e1", source="n1", target="n2")]
        update = FlowUpdate(name="Updated", nodes=nodes, edges=edges)

        result = await svc.update_flow(flow_id, update)
        assert result is not None


@pytest.mark.asyncio
async def test_update_flow_with_flow_config_no_actions():
    svc = make_service()
    flow_id = uuid.uuid4()
    updated_flow = make_flow(flow_id=flow_id)

    with patch("src.services.flow_service.FlowRepository") as MockRepo:
        mock_repo = AsyncMock()
        existing = make_flow(flow_id=flow_id)
        mock_repo.get = AsyncMock(return_value=existing)
        mock_repo.update = AsyncMock(return_value=updated_flow)
        MockRepo.return_value = mock_repo

        update = make_flow_update(flow_config={"type": "default"})  # no 'actions' key
        result = await svc.update_flow(flow_id, update)
        assert result is not None


@pytest.mark.asyncio
async def test_update_flow_generic_error():
    svc = make_service()
    flow_id = uuid.uuid4()

    with patch("src.services.flow_service.FlowRepository") as MockRepo:
        mock_repo = AsyncMock()
        existing = make_flow(flow_id=flow_id)
        mock_repo.get = AsyncMock(return_value=existing)
        mock_repo.update = AsyncMock(side_effect=RuntimeError("DB error"))
        MockRepo.return_value = mock_repo

        with pytest.raises(KasalError):
            await svc.update_flow(flow_id, make_flow_update())


# ---------------------------------------------------------------------------
# force_delete_flow_with_executions
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_force_delete_flow_not_found():
    session = make_session()
    svc = make_service(session=session)

    mock_result = MagicMock()
    mock_result.first.return_value = None
    session.execute = AsyncMock(return_value=mock_result)

    flow_id = uuid.uuid4()
    with pytest.raises(NotFoundError):
        await svc.force_delete_flow_with_executions(flow_id)


@pytest.mark.asyncio
async def test_force_delete_flow_with_executions_success():
    session = make_session()
    svc = make_service(session=session)
    flow_id = uuid.uuid4()

    check_result = MagicMock()
    check_result.first.return_value = (str(flow_id),)

    find_result = MagicMock()
    find_result.fetchall.return_value = [(1,), (2,)]

    trace_result = MagicMock()
    trace_result.rowcount = 3

    exec_result = MagicMock()
    exec_result.rowcount = 2

    delete_result = MagicMock()

    session.execute = AsyncMock(side_effect=[
        check_result, find_result, trace_result, exec_result, delete_result
    ])

    result = await svc.force_delete_flow_with_executions(flow_id)
    assert result is True


@pytest.mark.asyncio
async def test_force_delete_flow_no_executions():
    session = make_session()
    svc = make_service(session=session)
    flow_id = uuid.uuid4()

    check_result = MagicMock()
    check_result.first.return_value = (str(flow_id),)

    find_result = MagicMock()
    find_result.fetchall.return_value = []  # No executions

    exec_result = MagicMock()
    exec_result.rowcount = 0

    delete_result = MagicMock()

    session.execute = AsyncMock(side_effect=[
        check_result, find_result, exec_result, delete_result
    ])

    result = await svc.force_delete_flow_with_executions(flow_id)
    assert result is True


@pytest.mark.asyncio
async def test_force_delete_flow_generic_error():
    session = make_session()
    svc = make_service(session=session)
    flow_id = uuid.uuid4()

    session.execute = AsyncMock(side_effect=RuntimeError("DB crash"))

    with pytest.raises(KasalError):
        await svc.force_delete_flow_with_executions(flow_id)


# ---------------------------------------------------------------------------
# force_delete_flow_with_executions_with_group_check
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_force_delete_with_group_check_not_found():
    session = make_session()
    svc = make_service(session=session)

    check_result = MagicMock()
    check_result.first.return_value = None
    session.execute = AsyncMock(return_value=check_result)

    flow_id = uuid.uuid4()
    with pytest.raises(NotFoundError):
        await svc.force_delete_flow_with_executions_with_group_check(flow_id, make_group_context())


@pytest.mark.asyncio
async def test_force_delete_with_group_check_forbidden():
    session = make_session()
    svc = make_service(session=session)
    flow_id = uuid.uuid4()

    check_result = MagicMock()
    check_result.first.return_value = (str(flow_id), "other-group")
    session.execute = AsyncMock(return_value=check_result)

    group_ctx = make_group_context(group_ids=["my-group"])
    with pytest.raises(ForbiddenError):
        await svc.force_delete_flow_with_executions_with_group_check(flow_id, group_ctx)


@pytest.mark.asyncio
async def test_force_delete_with_group_check_success():
    session = make_session()
    svc = make_service(session=session)
    flow_id = uuid.uuid4()

    check_result = MagicMock()
    check_result.first.return_value = (str(flow_id), "g1")

    find_result = MagicMock()
    rows = [(1, "job-1"), (2, "job-2")]
    find_result.fetchall.return_value = rows

    trace_result = MagicMock()
    trace_result.rowcount = 2

    exec_result = MagicMock()
    delete_result = MagicMock()

    session.execute = AsyncMock(side_effect=[
        check_result, find_result, trace_result, exec_result, delete_result
    ])

    group_ctx = make_group_context(group_ids=["g1"])
    result = await svc.force_delete_flow_with_executions_with_group_check(flow_id, group_ctx)
    assert result is True


@pytest.mark.asyncio
async def test_force_delete_with_group_check_generic_error():
    session = make_session()
    svc = make_service(session=session)
    flow_id = uuid.uuid4()

    session.execute = AsyncMock(side_effect=RuntimeError("crash"))

    with pytest.raises(KasalError):
        await svc.force_delete_flow_with_executions_with_group_check(flow_id, make_group_context())


# ---------------------------------------------------------------------------
# create_flow (backward compat) - validation paths
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_create_flow_invalid_listener():
    svc = make_service()
    flow_in = make_flow_create(flow_config={"listeners": ["not_a_dict"]})

    with pytest.raises(BadRequestError):
        await svc.create_flow(flow_in)


@pytest.mark.asyncio
async def test_create_flow_listener_missing_required_fields():
    svc = make_service()
    flow_in = make_flow_create(flow_config={"listeners": [{"name": "only-name"}]})

    with pytest.raises(BadRequestError):
        await svc.create_flow(flow_in)


@pytest.mark.asyncio
async def test_create_flow_invalid_action():
    svc = make_service()
    flow_in = make_flow_create(flow_config={"actions": [42]})

    with pytest.raises(BadRequestError):
        await svc.create_flow(flow_in)


@pytest.mark.asyncio
async def test_create_flow_action_missing_required_fields():
    svc = make_service()
    flow_in = make_flow_create(flow_config={"actions": [{"crewId": "only-crew"}]})

    with pytest.raises(BadRequestError):
        await svc.create_flow(flow_in)


# ---------------------------------------------------------------------------
# delete_flow
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_delete_flow_not_found():
    svc = make_service()

    with patch("src.services.flow_service.FlowRepository") as MockRepo:
        mock_repo = AsyncMock()
        mock_repo.get = AsyncMock(return_value=None)
        MockRepo.return_value = mock_repo

        flow_id = uuid.uuid4()
        with pytest.raises(NotFoundError):
            await svc.delete_flow(flow_id)


@pytest.mark.asyncio
async def test_delete_flow_has_executions():
    session = make_session()
    svc = make_service(session=session)
    flow_id = uuid.uuid4()

    with patch("src.services.flow_service.FlowRepository") as MockRepo:
        mock_repo = AsyncMock()
        flow = make_flow(flow_id=flow_id)
        mock_repo.get = AsyncMock(return_value=flow)
        MockRepo.return_value = mock_repo

        mock_count_result = MagicMock()
        mock_count_result.scalar_one.return_value = 3
        session.execute = AsyncMock(return_value=mock_count_result)

        with pytest.raises(BadRequestError):
            await svc.delete_flow(flow_id)


@pytest.mark.asyncio
async def test_delete_flow_success():
    session = make_session()
    svc = make_service(session=session)
    flow_id = uuid.uuid4()

    with patch("src.services.flow_service.FlowRepository") as MockRepo:
        mock_repo = AsyncMock()
        flow = make_flow(flow_id=flow_id)
        mock_repo.get = AsyncMock(return_value=flow)
        mock_repo.delete = AsyncMock()
        MockRepo.return_value = mock_repo

        mock_count_result = MagicMock()
        mock_count_result.scalar_one.return_value = 0
        session.execute = AsyncMock(return_value=mock_count_result)

        result = await svc.delete_flow(flow_id)
        assert result is True


# ---------------------------------------------------------------------------
# validate_flow_data - error path
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_validate_flow_data_error():
    svc = make_service()

    # Pass a bad object that will fail model_dump
    bad_flow_in = MagicMock()
    bad_flow_in.model_dump = MagicMock(side_effect=RuntimeError("cannot dump"))

    result = await svc.validate_flow_data(bad_flow_in)
    assert result["status"] == "error"
    assert "Validation failed" in result["message"]
