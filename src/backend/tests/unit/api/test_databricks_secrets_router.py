"""
Extended tests for databricks_secrets_router.py to cover missing branches.
Focuses on: create success, update failure, delete success, scopes endpoints,
legacy endpoints, set_databricks_token, and get_databricks_secrets with auth.
"""
import pytest
from unittest.mock import AsyncMock, patch
from types import SimpleNamespace

from src.api.databricks_secrets_router import (
    get_databricks_secrets,
    create_databricks_secret,
    update_databricks_secret,
    delete_databricks_secret,
    create_databricks_secret_scope,
    get_secrets,
    set_secret,
    delete_secret_endpoint,
    create_secret_scope_endpoint,
    set_databricks_token,
    get_legacy_api_keys,
    create_legacy_api_key,
    update_legacy_api_key,
    delete_legacy_api_key,
)
from src.schemas.databricks_secret import (
    DatabricksTokenRequest,
    SecretCreate,
    SecretUpdate,
)
from src.core.exceptions import BadRequestError, KasalError, NotFoundError


class Ctx:
    def __init__(self, user_role="admin"):
        self.user_role = user_role


def make_config(enabled=True, workspace_url="https://w", scope="sc"):
    return SimpleNamespace(
        is_enabled=enabled,
        workspace_url=workspace_url,
        secret_scope=scope,
    )


# ── get_databricks_secrets: enabled config with auth ─────────────────────────

@pytest.mark.asyncio
async def test_get_databricks_secrets_returns_list_when_configured():
    """Returns secrets list when Databricks is configured and auth succeeds."""
    ctx = Ctx()
    svc = AsyncMock()
    svc.databricks_service.get_databricks_config = AsyncMock(
        return_value=make_config()
    )
    svc.get_databricks_secrets = AsyncMock(
        return_value=[{"name": "key1", "value": "v1"}]
    )

    fake_auth = SimpleNamespace(token="tok123")
    with patch(
        "src.utils.databricks_auth.get_auth_context",
        AsyncMock(return_value=fake_auth),
    ):
        result = await get_databricks_secrets(group_context=ctx, service=svc)
    assert len(result) == 1
    assert result[0]["name"] == "key1"


@pytest.mark.asyncio
async def test_get_databricks_secrets_returns_empty_when_auth_fails():
    """Returns empty list when unified auth raises exception."""
    ctx = Ctx()
    svc = AsyncMock()
    svc.databricks_service.get_databricks_config = AsyncMock(
        return_value=make_config()
    )

    with patch(
        "src.utils.databricks_auth.get_auth_context",
        AsyncMock(side_effect=Exception("auth error")),
    ):
        result = await get_databricks_secrets(group_context=ctx, service=svc)
    assert result == []


@pytest.mark.asyncio
async def test_get_databricks_secrets_returns_empty_when_no_token():
    """Returns empty list when auth context has no token."""
    ctx = Ctx()
    svc = AsyncMock()
    svc.databricks_service.get_databricks_config = AsyncMock(
        return_value=make_config()
    )

    fake_auth = SimpleNamespace(token=None)
    with patch(
        "src.utils.databricks_auth.get_auth_context",
        AsyncMock(return_value=fake_auth),
    ):
        result = await get_databricks_secrets(group_context=ctx, service=svc)
    assert result == []


# ── create_databricks_secret ──────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_create_secret_success():
    """create_databricks_secret returns secret dict on success."""
    ctx = Ctx()
    svc = AsyncMock()
    svc.databricks_service.get_databricks_config = AsyncMock(
        return_value=make_config()
    )
    svc.set_databricks_secret_value = AsyncMock(return_value=True)

    out = await create_databricks_secret(
        SecretCreate(name="mykey", value="myval"), group_context=ctx, service=svc
    )
    assert out["name"] == "mykey"
    assert out["scope"] == "sc"


@pytest.mark.asyncio
async def test_create_secret_set_fails_raises_kasal_error():
    """create_databricks_secret raises KasalError when set returns False."""
    ctx = Ctx()
    svc = AsyncMock()
    svc.databricks_service.get_databricks_config = AsyncMock(
        return_value=make_config()
    )
    svc.set_databricks_secret_value = AsyncMock(return_value=False)

    with pytest.raises(KasalError):
        await create_databricks_secret(
            SecretCreate(name="k", value="v"), group_context=ctx, service=svc
        )


# ── update_databricks_secret ──────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_update_secret_failure_raises_kasal_error():
    """update_databricks_secret raises KasalError when set returns False."""
    ctx = Ctx()
    svc = AsyncMock()
    svc.databricks_service.get_databricks_config = AsyncMock(
        return_value=make_config()
    )
    svc.set_databricks_secret_value = AsyncMock(return_value=False)

    with pytest.raises(KasalError):
        await update_databricks_secret(
            "k", SecretUpdate(value="v"), group_context=ctx, service=svc
        )


@pytest.mark.asyncio
async def test_update_secret_not_configured_raises_bad_request():
    """update_databricks_secret raises BadRequestError when not configured."""
    ctx = Ctx()
    svc = AsyncMock()
    svc.databricks_service.get_databricks_config = AsyncMock(return_value=None)

    with pytest.raises(BadRequestError):
        await update_databricks_secret(
            "k", SecretUpdate(value="v"), group_context=ctx, service=svc
        )


# ── delete_databricks_secret ──────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_delete_secret_success():
    """delete_databricks_secret succeeds silently when found."""
    ctx = Ctx()
    svc = AsyncMock()
    svc.databricks_service.get_databricks_config = AsyncMock(
        return_value=make_config()
    )
    svc.delete_databricks_secret = AsyncMock(return_value=True)

    # Should not raise
    await delete_databricks_secret("mykey", group_context=ctx, service=svc)
    svc.delete_databricks_secret.assert_called_once_with("sc", "mykey")


# ── create_databricks_secret_scope ────────────────────────────────────────────

@pytest.mark.asyncio
async def test_create_scope_not_configured_raises_bad_request():
    """create_databricks_secret_scope raises BadRequestError when not configured."""
    ctx = Ctx()
    svc = AsyncMock()
    svc.databricks_service.get_databricks_config = AsyncMock(return_value=None)

    with pytest.raises(BadRequestError):
        await create_databricks_secret_scope(group_context=ctx, service=svc)


@pytest.mark.asyncio
async def test_create_scope_no_auth_raises_bad_request():
    """create_databricks_secret_scope raises BadRequestError when no auth available."""
    ctx = Ctx()
    svc = AsyncMock()
    svc.databricks_service.get_databricks_config = AsyncMock(
        return_value=make_config()
    )

    with patch(
        "src.utils.databricks_auth.get_auth_context",
        AsyncMock(return_value=None),
    ):
        with pytest.raises(BadRequestError):
            await create_databricks_secret_scope(group_context=ctx, service=svc)


@pytest.mark.asyncio
async def test_create_scope_success():
    """create_databricks_secret_scope returns success dict."""
    ctx = Ctx()
    svc = AsyncMock()
    svc.databricks_service.get_databricks_config = AsyncMock(
        return_value=make_config()
    )
    svc.create_databricks_secret_scope = AsyncMock(return_value=True)

    fake_auth = SimpleNamespace(token="tok")
    with patch(
        "src.utils.databricks_auth.get_auth_context",
        AsyncMock(return_value=fake_auth),
    ):
        out = await create_databricks_secret_scope(group_context=ctx, service=svc)
    assert out["status"] == "success"


@pytest.mark.asyncio
async def test_create_scope_failure_raises_kasal_error():
    """create_databricks_secret_scope raises KasalError when creation fails."""
    ctx = Ctx()
    svc = AsyncMock()
    svc.databricks_service.get_databricks_config = AsyncMock(
        return_value=make_config()
    )
    svc.create_databricks_secret_scope = AsyncMock(return_value=False)

    fake_auth = SimpleNamespace(token="tok")
    with patch(
        "src.utils.databricks_auth.get_auth_context",
        AsyncMock(return_value=fake_auth),
    ):
        with pytest.raises(KasalError):
            await create_databricks_secret_scope(group_context=ctx, service=svc)


# ── legacy get_secrets endpoint ───────────────────────────────────────────────

@pytest.mark.asyncio
async def test_get_secrets_returns_empty_on_exception():
    """get_secrets returns empty list on exception."""
    ctx = Ctx()
    svc = AsyncMock()
    svc.validate_databricks_config = AsyncMock(
        side_effect=Exception("config error")
    )

    out = await get_secrets(group_context=ctx, service=svc)
    assert out == []


@pytest.mark.asyncio
async def test_get_secrets_returns_list_on_success():
    """get_secrets returns secrets list on success."""
    ctx = Ctx()
    svc = AsyncMock()
    svc.validate_databricks_config = AsyncMock(return_value=("https://w", "sc"))
    svc.get_databricks_secrets = AsyncMock(return_value=[{"name": "k1"}])

    out = await get_secrets(group_context=ctx, service=svc)
    assert len(out) == 1


@pytest.mark.asyncio
async def test_get_secrets_returns_empty_when_none():
    """get_secrets returns empty list when service returns None."""
    ctx = Ctx()
    svc = AsyncMock()
    svc.validate_databricks_config = AsyncMock(return_value=("https://w", "sc"))
    svc.get_databricks_secrets = AsyncMock(return_value=None)

    out = await get_secrets(group_context=ctx, service=svc)
    assert out == []


# ── set_secret legacy ─────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_set_secret_success():
    """set_secret returns success dict."""
    ctx = Ctx()
    svc = AsyncMock()
    svc.validate_databricks_config = AsyncMock(return_value=("https://w", "sc"))
    svc.set_databricks_secret_value = AsyncMock(return_value=True)

    out = await set_secret("mykey", SecretUpdate(value="v"), group_context=ctx, service=svc)
    assert out["status"] == "success"


@pytest.mark.asyncio
async def test_set_secret_failure_raises_kasal_error():
    """set_secret raises KasalError when set returns False."""
    ctx = Ctx()
    svc = AsyncMock()
    svc.validate_databricks_config = AsyncMock(return_value=("https://w", "sc"))
    svc.set_databricks_secret_value = AsyncMock(return_value=False)

    with pytest.raises(KasalError):
        await set_secret("k", SecretUpdate(value="v"), group_context=ctx, service=svc)


# ── delete_secret_endpoint legacy ────────────────────────────────────────────

@pytest.mark.asyncio
async def test_delete_secret_endpoint_success():
    """delete_secret_endpoint succeeds silently."""
    ctx = Ctx()
    svc = AsyncMock()
    svc.validate_databricks_config = AsyncMock(return_value=("https://w", "sc"))
    svc.delete_databricks_secret = AsyncMock(return_value=True)

    await delete_secret_endpoint("mykey", group_context=ctx, service=svc)


@pytest.mark.asyncio
async def test_delete_secret_endpoint_not_found_raises():
    """delete_secret_endpoint raises NotFoundError when not found."""
    ctx = Ctx()
    svc = AsyncMock()
    svc.validate_databricks_config = AsyncMock(return_value=("https://w", "sc"))
    svc.delete_databricks_secret = AsyncMock(return_value=False)

    with pytest.raises(NotFoundError):
        await delete_secret_endpoint("missing", group_context=ctx, service=svc)


# ── create_secret_scope_endpoint legacy ──────────────────────────────────────

@pytest.mark.asyncio
async def test_create_secret_scope_endpoint_success():
    """create_secret_scope_endpoint returns success dict."""
    ctx = Ctx()
    svc = AsyncMock()
    svc.validate_databricks_config = AsyncMock(return_value=("https://w", "sc"))
    svc.create_databricks_secret_scope = AsyncMock(return_value=True)

    fake_auth = SimpleNamespace(token="tok")
    with patch(
        "src.utils.databricks_auth.get_auth_context",
        AsyncMock(return_value=fake_auth),
    ):
        out = await create_secret_scope_endpoint(group_context=ctx, service=svc)
    assert out["status"] == "success"


@pytest.mark.asyncio
async def test_create_secret_scope_endpoint_no_auth_raises():
    """create_secret_scope_endpoint raises BadRequestError when no auth."""
    ctx = Ctx()
    svc = AsyncMock()
    svc.validate_databricks_config = AsyncMock(return_value=("https://w", "sc"))

    with patch(
        "src.utils.databricks_auth.get_auth_context",
        AsyncMock(return_value=None),
    ):
        with pytest.raises(BadRequestError):
            await create_secret_scope_endpoint(group_context=ctx, service=svc)


@pytest.mark.asyncio
async def test_create_secret_scope_endpoint_failure_raises():
    """create_secret_scope_endpoint raises KasalError when creation fails."""
    ctx = Ctx()
    svc = AsyncMock()
    svc.validate_databricks_config = AsyncMock(return_value=("https://w", "sc"))
    svc.create_databricks_secret_scope = AsyncMock(return_value=False)

    fake_auth = SimpleNamespace(token="tok")
    with patch(
        "src.utils.databricks_auth.get_auth_context",
        AsyncMock(return_value=fake_auth),
    ):
        with pytest.raises(KasalError):
            await create_secret_scope_endpoint(group_context=ctx, service=svc)


# ── set_databricks_token ──────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_set_databricks_token_success():
    """set_databricks_token returns success dict."""
    ctx = Ctx()
    svc = AsyncMock()
    svc.databricks_service.get_databricks_config = AsyncMock(
        return_value=make_config()
    )
    svc.set_databricks_token = AsyncMock(return_value=True)

    out = await set_databricks_token(
        DatabricksTokenRequest(workspace_url="https://w", token="mytoken"), group_context=ctx, service=svc
    )
    assert out["status"] == "success"


@pytest.mark.asyncio
async def test_set_databricks_token_not_configured_raises():
    """set_databricks_token raises BadRequestError when not configured."""
    ctx = Ctx()
    svc = AsyncMock()
    svc.databricks_service.get_databricks_config = AsyncMock(return_value=None)

    with pytest.raises(BadRequestError):
        await set_databricks_token(
            DatabricksTokenRequest(workspace_url="https://w", token="tok"), group_context=ctx, service=svc
        )


@pytest.mark.asyncio
async def test_set_databricks_token_failure_raises_kasal_error():
    """set_databricks_token raises KasalError when service fails."""
    ctx = Ctx()
    svc = AsyncMock()
    svc.databricks_service.get_databricks_config = AsyncMock(
        return_value=make_config()
    )
    svc.set_databricks_token = AsyncMock(return_value=False)

    with pytest.raises(KasalError):
        await set_databricks_token(
            DatabricksTokenRequest(workspace_url="https://w", token="tok"), group_context=ctx, service=svc
        )


# ── legacy API key endpoints ──────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_get_legacy_api_keys_delegates():
    """get_legacy_api_keys delegates to get_databricks_secrets."""
    ctx = Ctx()
    svc = AsyncMock()
    svc.databricks_service.get_databricks_config = AsyncMock(return_value=None)

    out = await get_legacy_api_keys(group_context=ctx, service=svc)
    assert out == []


@pytest.mark.asyncio
async def test_create_legacy_api_key_delegates():
    """create_legacy_api_key delegates to create_databricks_secret."""
    ctx = Ctx()
    svc = AsyncMock()
    svc.databricks_service.get_databricks_config = AsyncMock(
        return_value=make_config()
    )
    svc.set_databricks_secret_value = AsyncMock(return_value=True)

    out = await create_legacy_api_key(
        SecretCreate(name="k", value="v"), group_context=ctx, service=svc
    )
    assert out["name"] == "k"


@pytest.mark.asyncio
async def test_update_legacy_api_key_delegates():
    """update_legacy_api_key delegates to update_databricks_secret."""
    ctx = Ctx()
    svc = AsyncMock()
    svc.databricks_service.get_databricks_config = AsyncMock(
        return_value=make_config()
    )
    svc.set_databricks_secret_value = AsyncMock(return_value=True)

    out = await update_legacy_api_key(
        "mykey", SecretUpdate(value="newval"), group_context=ctx, service=svc
    )
    assert out["name"] == "mykey"


@pytest.mark.asyncio
async def test_delete_legacy_api_key_delegates():
    """delete_legacy_api_key delegates to delete_databricks_secret."""
    ctx = Ctx()
    svc = AsyncMock()
    svc.databricks_service.get_databricks_config = AsyncMock(
        return_value=make_config()
    )
    svc.delete_databricks_secret = AsyncMock(return_value=True)

    await delete_legacy_api_key("mykey", group_context=ctx, service=svc)
    svc.delete_databricks_secret.assert_called_once_with("sc", "mykey")
