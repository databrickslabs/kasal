"""
Extended tests for user_context module to improve coverage.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from types import SimpleNamespace

from src.utils.user_context import (
    GroupContext,
    UserContext,
    extract_user_token_from_request,
    extract_group_context_from_request,
    extract_user_context_from_request,
    UserContextMiddleware,
    user_context_middleware,
    is_databricks_app_context,
)


class TestGroupContextProperties:
    def test_primary_group_id_returns_first(self):
        """primary_group_id returns first group ID."""
        ctx = GroupContext(group_ids=["g1", "g2"])
        assert ctx.primary_group_id == "g1"

    def test_primary_group_id_none_when_empty_list(self):
        """primary_group_id returns None when group_ids is empty."""
        ctx = GroupContext(group_ids=[])
        assert ctx.primary_group_id is None

    def test_primary_group_id_none_when_group_ids_none(self):
        """primary_group_id returns None when group_ids is None."""
        ctx = GroupContext(group_ids=None)
        assert ctx.primary_group_id is None

    def test_is_valid_with_ids_and_domain(self):
        """is_valid returns True when group_ids and email_domain set."""
        ctx = GroupContext(group_ids=["g1"], email_domain="test.com")
        assert ctx.is_valid() is True

    def test_is_valid_false_without_domain(self):
        """is_valid returns False when email_domain is None."""
        ctx = GroupContext(group_ids=["g1"], email_domain=None)
        assert ctx.is_valid() is False

    def test_is_valid_false_without_group_ids(self):
        """is_valid returns False when group_ids is None."""
        ctx = GroupContext(group_ids=None, email_domain="test.com")
        assert ctx.is_valid() is False

    def test_to_dict_includes_all_fields(self):
        """to_dict includes all expected keys."""
        ctx = GroupContext(
            group_ids=["g1"],
            group_email="user@test.com",
            email_domain="test.com",
            user_id="u1",
            access_token="token",
            user_role="admin",
            highest_role="admin",
        )
        d = ctx.to_dict()
        assert "group_ids" in d
        assert "group_email" in d
        assert "primary_group_id" in d
        assert "group_id" in d  # backward compat alias
        assert d["primary_group_id"] == "g1"
        assert d["group_id"] == "g1"

    def test_to_dict_with_current_user(self):
        """to_dict includes is_system_admin from current_user."""
        mock_user = SimpleNamespace(
            is_system_admin=True, is_personal_workspace_manager=False
        )
        ctx = GroupContext(group_ids=["g1"], current_user=mock_user)
        d = ctx.to_dict()
        assert d["is_system_admin"] is True
        assert d["is_personal_workspace_manager"] is False

    def test_to_dict_without_current_user(self):
        """to_dict defaults admin flags to False when current_user is None."""
        ctx = GroupContext(group_ids=["g1"], current_user=None)
        d = ctx.to_dict()
        assert d["is_system_admin"] is False
        assert d["is_personal_workspace_manager"] is False


class TestGroupContextStaticMethods:
    def test_generate_group_id(self):
        """generate_group_id converts domain dots/dashes to underscores."""
        result = GroupContext.generate_group_id("acme-corp.com")
        assert result == "acme_corp_com"

    def test_generate_individual_group_id(self):
        """generate_individual_group_id prefixes with user_."""
        result = GroupContext.generate_individual_group_id("alice@test.com")
        assert result.startswith("user_")
        assert "alice" in result
        assert "test" in result

    def test_generate_individual_group_id_special_chars(self):
        """generate_individual_group_id sanitizes special chars."""
        result = GroupContext.generate_individual_group_id("bob.smith+tag@start-up.io")
        # All special chars should become underscores
        assert "@" not in result
        assert "." not in result
        assert "+" not in result
        assert "-" not in result


class TestGroupContextFromEmail:
    @pytest.mark.asyncio
    async def test_invalid_email_returns_empty_context(self):
        """from_email returns empty context for invalid email."""
        ctx = await GroupContext.from_email("notanemail")
        assert ctx.group_ids is None

    @pytest.mark.asyncio
    async def test_empty_email_returns_empty_context(self):
        """from_email returns empty context for empty email."""
        ctx = await GroupContext.from_email("")
        assert ctx.group_ids is None

    @pytest.mark.asyncio
    async def test_user_with_no_groups_gets_individual_group(self):
        """User not in any groups gets individual workspace group."""
        mock_user = SimpleNamespace(id="u1", email="solo@example.com",
                                   is_system_admin=False, is_personal_workspace_manager=False)
        with patch.object(
            GroupContext, "_get_user_group_memberships_with_roles",
            AsyncMock(return_value=(mock_user, []))
        ):
            ctx = await GroupContext.from_email("solo@example.com")
        assert ctx.primary_group_id is not None
        assert ctx.primary_group_id.startswith("user_")

    @pytest.mark.asyncio
    async def test_user_in_groups_gets_group_ids(self):
        """User in groups gets those group IDs."""
        mock_user = SimpleNamespace(id="u1", email="member@corp.com",
                                   is_system_admin=False, is_personal_workspace_manager=False)
        mock_group = SimpleNamespace(id="corp-group", name="Corp Group")
        with patch.object(
            GroupContext, "_get_user_group_memberships_with_roles",
            AsyncMock(return_value=(mock_user, [(mock_group, "editor")]))
        ):
            ctx = await GroupContext.from_email("member@corp.com")
        assert "corp-group" in ctx.group_ids
        assert ctx.user_role == "editor"

    @pytest.mark.asyncio
    async def test_admin_role_detection(self):
        """Highest role is admin when user is admin in any group."""
        mock_user = SimpleNamespace(id="u1", email="admin@corp.com",
                                   is_system_admin=False, is_personal_workspace_manager=False)
        mock_group1 = SimpleNamespace(id="g1", name="Group 1")
        mock_group2 = SimpleNamespace(id="g2", name="Group 2")
        with patch.object(
            GroupContext, "_get_user_group_memberships_with_roles",
            AsyncMock(return_value=(mock_user, [(mock_group1, "editor"), (mock_group2, "admin")]))
        ):
            ctx = await GroupContext.from_email("admin@corp.com")
        assert ctx.highest_role == "admin"

    @pytest.mark.asyncio
    async def test_fallback_on_lookup_error(self):
        """from_email falls back to individual group on lookup failure."""
        with patch.object(
            GroupContext, "_get_user_group_memberships_with_roles",
            AsyncMock(side_effect=Exception("DB error"))
        ):
            ctx = await GroupContext.from_email("fallback@test.com")
        assert ctx.primary_group_id is not None
        assert ctx.primary_group_id.startswith("user_")

    @pytest.mark.asyncio
    async def test_security_error_propagates(self):
        """ValueError from security check propagates (not swallowed)."""
        with patch.object(
            GroupContext, "_get_user_group_memberships_with_roles",
            AsyncMock(side_effect=ValueError("Access denied"))
        ):
            with pytest.raises(ValueError, match="Access denied"):
                await GroupContext.from_email("bad@test.com")

    @pytest.mark.asyncio
    async def test_specific_group_id_header_sets_primary(self):
        """Specifying group_id header sets it as primary group."""
        mock_user = SimpleNamespace(id="u1", email="member@corp.com",
                                   is_system_admin=False, is_personal_workspace_manager=False)
        mock_group1 = SimpleNamespace(id="g1", name="Group 1")
        mock_group2 = SimpleNamespace(id="g2", name="Group 2")
        with patch.object(
            GroupContext, "_get_user_group_memberships_with_roles",
            AsyncMock(return_value=(mock_user, [(mock_group1, "editor"), (mock_group2, "admin")]))
        ):
            ctx = await GroupContext.from_email("member@corp.com", group_id="g2")
        assert ctx.group_ids[0] == "g2"

    @pytest.mark.asyncio
    async def test_explicit_group_id_scopes_strictly_no_union(self):
        """An explicit group_id scopes to ONLY that workspace — no union, no personal.

        Credential/LLM resolution keys off primary_group_id (group_ids[0]); if this
        returned the union, a different workspace's PAT could be used for the
        selected workspace (cross-workspace credential bleed)."""
        mock_user = SimpleNamespace(id="u1", email="member@corp.com",
                                    is_system_admin=False, is_personal_workspace_manager=False)
        g1 = SimpleNamespace(id="g1", name="Group 1")
        g2 = SimpleNamespace(id="g2", name="Group 2")
        with patch.object(
            GroupContext, "_get_user_group_memberships_with_roles",
            AsyncMock(return_value=(mock_user, [(g1, "editor"), (g2, "admin")]))
        ):
            ctx = await GroupContext.from_email("member@corp.com", group_id="g2")
        assert ctx.group_ids == ["g2"], "selected workspace must be the ONLY group (no union/personal)"
        assert ctx.primary_group_id == "g2"

    @pytest.mark.asyncio
    async def test_unauthorized_group_raises(self):
        """Accessing a group user doesn't belong to raises ValueError."""
        mock_user = SimpleNamespace(id="u1", email="member@corp.com",
                                   is_system_admin=False, is_personal_workspace_manager=False)
        mock_group = SimpleNamespace(id="g1", name="Group 1")
        with patch.object(
            GroupContext, "_get_user_group_memberships_with_roles",
            AsyncMock(return_value=(mock_user, [(mock_group, "editor")]))
        ):
            with pytest.raises(ValueError, match="Access denied"):
                await GroupContext.from_email("member@corp.com", group_id="g-unauthorized")


class TestUserContext:
    def test_set_and_get_user_token(self):
        """set_user_token/get_user_token round-trip."""
        UserContext.set_user_token("my-token")
        assert UserContext.get_user_token() == "my-token"
        UserContext.clear_context()

    def test_set_and_get_user_context(self):
        """set_user_context/get_user_context round-trip."""
        ctx = {"email": "user@test.com", "access_token": "tok"}
        UserContext.set_user_context(ctx)
        assert UserContext.get_user_context() == ctx
        UserContext.clear_context()

    def test_set_and_get_group_context(self):
        """set_group_context/get_group_context round-trip."""
        gc = GroupContext(group_ids=["g1"], group_email="u@t.com")
        UserContext.set_group_context(gc)
        retrieved = UserContext.get_group_context()
        assert retrieved is gc
        UserContext.clear_context()

    def test_clear_context(self):
        """clear_context resets all context vars."""
        UserContext.set_user_token("tok")
        UserContext.set_user_context({"k": "v"})
        UserContext.set_group_context(GroupContext(group_ids=["g1"]))
        UserContext.clear_context()
        assert UserContext.get_user_token() is None
        assert UserContext.get_user_context() is None
        assert UserContext.get_group_context() is None

    def test_get_user_token_returns_none_when_not_set(self):
        """get_user_token returns None by default."""
        UserContext.clear_context()
        assert UserContext.get_user_token() is None


class TestExtractUserTokenFromRequest:
    def _make_request(self, headers=None):
        mock_req = MagicMock()
        mock_req.headers = headers or {}
        return mock_req

    def test_extracts_forwarded_token(self):
        """Extracts X-Forwarded-Access-Token from headers."""
        request = self._make_request({"X-Forwarded-Access-Token": "fwd-token"})
        token = extract_user_token_from_request(request)
        assert token == "fwd-token"

    def test_extracts_bearer_token_fallback(self):
        """Falls back to Authorization Bearer token."""
        request = self._make_request({"Authorization": "Bearer bearer-token"})
        token = extract_user_token_from_request(request)
        assert token == "bearer-token"

    def test_returns_none_when_no_token(self):
        """Returns None when no relevant headers present."""
        request = self._make_request({})
        token = extract_user_token_from_request(request)
        assert token is None

    def test_handles_exception_gracefully(self):
        """Returns None when headers.get raises."""
        request = MagicMock()
        request.headers.get = MagicMock(side_effect=Exception("header error"))
        token = extract_user_token_from_request(request)
        assert token is None


class TestExtractUserContextFromRequest:
    def _make_request(self, headers=None, client_host="127.0.0.1", method="GET", url="http://test.com"):
        mock_req = MagicMock()
        mock_req.headers = headers or {}
        mock_req.client = SimpleNamespace(host=client_host)
        mock_req.method = method
        mock_req.url = url
        return mock_req

    def test_extracts_email_and_token(self):
        """Extracts email and access token from request."""
        headers = {
            "X-Forwarded-Email": "user@test.com",
            "X-Forwarded-Access-Token": "my-token",
        }
        request = self._make_request(headers=headers)
        ctx = extract_user_context_from_request(request)
        assert ctx["email"] == "user@test.com"
        assert ctx["access_token"] == "my-token"

    def test_extracts_user_agent(self):
        """Extracts User-Agent from request headers."""
        headers = {"User-Agent": "Mozilla/5.0"}
        request = self._make_request(headers=headers)
        ctx = extract_user_context_from_request(request)
        assert ctx.get("user_agent") == "Mozilla/5.0"

    def test_extracts_databricks_headers(self):
        """Extracts X-Databricks-* and X-Forwarded-* headers."""
        headers = {
            "X-Databricks-Cluster-Id": "abc123",
            "X-Forwarded-Host": "host.com",
        }
        request = self._make_request(headers=headers)
        ctx = extract_user_context_from_request(request)
        assert "databricks_headers" in ctx

    def test_handles_exception_returns_empty_dict(self):
        """Returns empty dict on exception."""
        request = MagicMock()
        request.headers.items = MagicMock(side_effect=Exception("fail"))
        request.headers.get = MagicMock(side_effect=Exception("fail"))
        ctx = extract_user_context_from_request(request)
        assert isinstance(ctx, dict)


class TestExtractGroupContextFromRequest:
    @pytest.mark.asyncio
    async def test_returns_none_when_no_email_header(self):
        """Returns None when X-Forwarded-Email not present."""
        mock_req = MagicMock()
        mock_req.headers.get = MagicMock(return_value=None)
        result = await extract_group_context_from_request(mock_req)
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_on_exception(self):
        """Returns None when GroupContext.from_email raises."""
        mock_req = MagicMock()
        mock_req.headers.get = MagicMock(return_value="user@test.com")
        with patch.object(GroupContext, "from_email", AsyncMock(side_effect=Exception("fail"))):
            result = await extract_group_context_from_request(mock_req)
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_context_for_valid_email(self):
        """Returns GroupContext when email is valid."""
        mock_req = MagicMock()
        mock_req.headers.get = MagicMock(return_value="user@test.com")
        mock_ctx = GroupContext(group_ids=["g1"], email_domain="test.com")
        with patch.object(GroupContext, "from_email", AsyncMock(return_value=mock_ctx)):
            result = await extract_group_context_from_request(mock_req)
        assert result is mock_ctx

    @pytest.mark.asyncio
    async def test_returns_none_for_invalid_context(self):
        """Returns None when context is invalid (no group_ids or domain)."""
        mock_req = MagicMock()
        mock_req.headers.get = MagicMock(return_value="bad@test.com")
        invalid_ctx = GroupContext(group_ids=None, email_domain=None)
        with patch.object(GroupContext, "from_email", AsyncMock(return_value=invalid_ctx)):
            result = await extract_group_context_from_request(mock_req)
        assert result is None

    @pytest.mark.asyncio
    async def test_forwards_group_id_header_to_from_email(self):
        """The middleware MUST forward the `group_id` header to from_email so the
        selected workspace is honored. Without it, from_email returns the UNION of
        the user's groups and primary_group_id resolves to their personal workspace,
        so credential/LLM resolution silently uses the personal workspace's PAT."""
        mock_req = MagicMock()
        mock_req.headers.get = MagicMock(
            side_effect=lambda key, default=None: {
                "X-Forwarded-Email": "user@test.com",
                "group_id": "bi-specialist",
            }.get(key, default)
        )
        mock_req.headers.items = MagicMock(return_value=[])
        captured = {}

        async def _from_email(email, access_token=None, group_id=None):
            captured["email"] = email
            captured["group_id"] = group_id
            return GroupContext(group_ids=["bi-specialist"], email_domain="test.com")

        with patch.object(GroupContext, "from_email", AsyncMock(side_effect=_from_email)):
            result = await extract_group_context_from_request(mock_req)

        assert captured["group_id"] == "bi-specialist", "group_id header must be forwarded to from_email"
        assert result.group_ids == ["bi-specialist"]


class TestUserContextMiddleware:
    @pytest.mark.asyncio
    async def test_non_http_scope_passes_through(self):
        """Non-HTTP scope passes through without modification."""
        inner_app = AsyncMock()
        middleware = UserContextMiddleware(inner_app)
        await middleware({"type": "websocket"}, None, None)
        inner_app.assert_called_once()

    @pytest.mark.asyncio
    async def test_http_scope_calls_inner_app(self):
        """HTTP scope calls inner app."""
        calls = []

        async def inner_app(scope, receive, send):
            calls.append("called")

        middleware = UserContextMiddleware(inner_app)
        scope = {
            "type": "http",
            "method": "GET",
            "path": "/test",
            "query_string": b"",
            "headers": [],
        }

        async def receive():
            return {"type": "http.request", "body": b"", "more_body": False}

        async def send(event):
            pass

        with patch("src.utils.user_context.extract_group_context_from_request", AsyncMock(return_value=None)), \
             patch("src.utils.user_context.extract_user_context_from_request", return_value={}):
            await middleware(scope, receive, send)

        assert "called" in calls

    @pytest.mark.asyncio
    async def test_context_cleared_in_finally(self):
        """UserContext is cleared after request even on error."""
        calls = []

        async def inner_app(scope, receive, send):
            calls.append("called")

        middleware = UserContextMiddleware(inner_app)
        scope = {
            "type": "http",
            "method": "GET",
            "path": "/test",
            "query_string": b"",
            "headers": [],
        }

        async def receive():
            return {"type": "http.request", "body": b"", "more_body": False}

        async def send(event):
            pass

        UserContext.set_user_token("tok")
        with patch("src.utils.user_context.extract_group_context_from_request", AsyncMock(return_value=None)), \
             patch("src.utils.user_context.extract_user_context_from_request", return_value={}):
            await middleware(scope, receive, send)
        # Context should be cleared
        assert UserContext.get_user_token() is None


class TestLegacyUserContextMiddleware:
    @pytest.mark.asyncio
    async def test_legacy_middleware_sets_and_clears_context(self):
        """Legacy user_context_middleware sets context and clears on completion."""
        mock_request = MagicMock()
        response = MagicMock()

        async def call_next(req):
            return response

        with patch("src.utils.user_context.extract_group_context_from_request", AsyncMock(return_value=None)), \
             patch("src.utils.user_context.extract_user_context_from_request", return_value={}):
            result = await user_context_middleware(mock_request, call_next)

        assert result is response
        assert UserContext.get_user_token() is None

    @pytest.mark.asyncio
    async def test_legacy_middleware_handles_exception(self):
        """Legacy user_context_middleware handles exception in call_next."""
        mock_request = MagicMock()
        response = MagicMock()

        async def call_next(req):
            return response

        with patch("src.utils.user_context.extract_group_context_from_request",
                   AsyncMock(side_effect=Exception("group error"))), \
             patch("src.utils.user_context.extract_user_context_from_request", return_value={}):
            result = await user_context_middleware(mock_request, call_next)

        assert result is response


class TestIsDatabricksAppContext:
    def test_returns_false_when_no_context(self):
        """Returns False when no user context set."""
        UserContext.clear_context()
        assert is_databricks_app_context() is False

    def test_returns_true_with_databricks_headers(self):
        """Returns True when databricks headers present."""
        UserContext.set_user_context({
            "access_token": "token",
            "databricks_headers": {"X-Databricks-Cluster": "abc"},
        })
        assert is_databricks_app_context() is True
        UserContext.clear_context()

    def test_returns_false_without_access_token(self):
        """Returns False when access_token missing from context."""
        UserContext.set_user_context({
            "databricks_headers": {"X-Databricks-Cluster": "abc"},
        })
        assert is_databricks_app_context() is False
        UserContext.clear_context()
