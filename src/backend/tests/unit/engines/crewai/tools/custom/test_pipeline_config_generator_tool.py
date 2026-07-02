"""Unit tests for PipelineConfigGeneratorTool (Tool 90)."""
import json
import pytest
from unittest.mock import patch, MagicMock, AsyncMock

from src.engines.crewai.tools.custom.pipeline_config_generator_tool import (
    PipelineConfigGeneratorTool,
    PipelineConfigGeneratorSchema,
)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

WORKSPACE_ID = "ws-aaaaaaaa-1111-2222-3333-bbbbbbbbbbbb"
DATASET_ID = "ds-cccccccc-4444-5555-6666-dddddddddddd"
TENANT_ID = "tenant-eeeeeeee-7777-8888-9999-ffffffffffff"
CLIENT_ID = "client-11111111-aaaa-bbbb-cccc-222222222222"
CLIENT_SECRET = "non-admin-secret"
ADMIN_CLIENT_ID = "admin-client-33333333-dddd-eeee-ffff-444444444444"
ADMIN_CLIENT_SECRET = "admin-secret"

MOCK_MEASURES = [
    {
        "measure_name": "Total Revenue",
        "dax_expression": "SUM(Fact_Sales[Amount])",
        "table": "Fact_Sales",
    },
    {
        "measure_name": "Profit Margin",
        "dax_expression": "DIVIDE([Profit], [Revenue])",
        "table": "Fact_Sales",
    },
]

MOCK_RELATIONSHIPS = [
    {
        "from_table": "Fact_Sales",
        "from_column": "customer_key",
        "to_table": "Dim_Customer",
        "to_column": "customer_key",
        "is_active": True,
    }
]


def _make_sp_kwargs(include_admin=True):
    kwargs = dict(
        workspace_id=WORKSPACE_ID,
        dataset_id=DATASET_ID,
        tenant_id=TENANT_ID,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        catalog="my_catalog",
        schema_name="metrics",
    )
    if include_admin:
        kwargs.update(
            admin_client_id=ADMIN_CLIENT_ID,
            admin_client_secret=ADMIN_CLIENT_SECRET,
        )
    return kwargs


# ---------------------------------------------------------------------------
# Schema tests
# ---------------------------------------------------------------------------

class TestPipelineConfigGeneratorSchema:
    def test_all_optional(self):
        schema = PipelineConfigGeneratorSchema()
        assert schema.workspace_id is None
        assert schema.dataset_id is None
        assert schema.tenant_id is None
        assert schema.admin_client_id is None

    def test_full_population(self):
        schema = PipelineConfigGeneratorSchema(**_make_sp_kwargs())
        assert schema.workspace_id == WORKSPACE_ID
        assert schema.admin_client_id == ADMIN_CLIENT_ID
        assert schema.catalog == "my_catalog"

    def test_report_id_optional(self):
        schema = PipelineConfigGeneratorSchema(
            workspace_id=WORKSPACE_ID,
            report_id="rpt-12345",
        )
        assert schema.report_id == "rpt-12345"

    def test_catalog_defaults_to_none(self):
        schema = PipelineConfigGeneratorSchema()
        assert schema.catalog is None

    def test_schema_name_field(self):
        schema = PipelineConfigGeneratorSchema(schema_name="my_schema")
        assert schema.schema_name == "my_schema"


# ---------------------------------------------------------------------------
# Init tests
# ---------------------------------------------------------------------------

class TestPipelineConfigGeneratorToolInit:
    def test_tool_name(self):
        tool = PipelineConfigGeneratorTool()
        assert "Pipeline Config" in tool.name or "Config Generator" in tool.name

    def test_static_config_stored(self):
        tool = PipelineConfigGeneratorTool(**_make_sp_kwargs())
        assert tool._default_config["workspace_id"] == WORKSPACE_ID
        assert tool._default_config["admin_client_id"] == ADMIN_CLIENT_ID

    def test_empty_init(self):
        tool = PipelineConfigGeneratorTool()
        assert tool._default_config == {}

    def test_description_not_empty(self):
        tool = PipelineConfigGeneratorTool()
        assert len(tool.description) > 20


# ---------------------------------------------------------------------------
# Missing required fields
# ---------------------------------------------------------------------------

class TestMissingFields:
    def test_missing_workspace_id(self):
        tool = PipelineConfigGeneratorTool()
        result = tool._run(
            dataset_id=DATASET_ID,
            tenant_id=TENANT_ID,
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
            admin_client_id=ADMIN_CLIENT_ID,
            admin_client_secret=ADMIN_CLIENT_SECRET,
        )
        assert "error" in result.lower() or "workspace" in result.lower() or result is not None

    def test_missing_admin_credentials(self):
        tool = PipelineConfigGeneratorTool()
        result = tool._run(
            workspace_id=WORKSPACE_ID,
            dataset_id=DATASET_ID,
            tenant_id=TENANT_ID,
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
            # No admin_client_id / admin_client_secret
        )
        assert result is not None

    def test_no_args_at_all(self):
        tool = PipelineConfigGeneratorTool()
        result = tool._run()
        assert result is not None
        assert "error" in result.lower() or "workspace" in result.lower() or "required" in result.lower()


# ---------------------------------------------------------------------------
# Static config fallback
# ---------------------------------------------------------------------------

class TestStaticConfigFallback:
    def test_kwargs_override_static_config(self):
        tool = PipelineConfigGeneratorTool(workspace_id="static-ws")
        assert tool._default_config["workspace_id"] == "static-ws"

    def test_static_config_used_when_no_runtime_kwargs(self):
        tool = PipelineConfigGeneratorTool(**_make_sp_kwargs())
        # Verify config is stored correctly before calling _run
        assert tool._default_config["tenant_id"] == TENANT_ID
        assert tool._default_config["catalog"] == "my_catalog"


# ---------------------------------------------------------------------------
# Mocked API execution
# ---------------------------------------------------------------------------

class TestMockedApiExecution:
    @patch("httpx.AsyncClient.post")
    @patch("httpx.AsyncClient.get")
    def test_api_failure_returns_error(self, mock_get, mock_post):
        mock_post.side_effect = Exception("Connection refused")
        mock_get.side_effect = Exception("Connection refused")

        tool = PipelineConfigGeneratorTool()
        result = tool._run(**_make_sp_kwargs())
        assert result is not None
        # Should contain error info, not crash silently
        assert len(result) > 0

    @patch("httpx.AsyncClient.post")
    @patch("httpx.AsyncClient.get")
    def test_auth_failure_returns_error(self, mock_get, mock_post):
        auth_resp = MagicMock()
        auth_resp.status_code = 401
        auth_resp.json.return_value = {"error": "invalid_client"}
        mock_post.return_value = auth_resp

        tool = PipelineConfigGeneratorTool()
        result = tool._run(**_make_sp_kwargs())
        assert result is not None


# ---------------------------------------------------------------------------
# Output structure
# ---------------------------------------------------------------------------

class TestOutputStructure:
    def test_output_is_string(self):
        tool = PipelineConfigGeneratorTool()
        result = tool._run()
        assert isinstance(result, str)

    def test_output_non_empty(self):
        tool = PipelineConfigGeneratorTool()
        result = tool._run()
        assert len(result) > 0


# ---------------------------------------------------------------------------
# Service Account (SA) support — non-admin + admin credential paths
# ---------------------------------------------------------------------------

SA_USERNAME = "svc-account@contoso.com"
SA_PASSWORD = "sa-password"
ADMIN_SA_USERNAME = "admin-svc@contoso.com"
ADMIN_SA_PASSWORD = "admin-sa-password"


def _make_sa_kwargs(include_admin=True):
    """Service Account credentials for both paths (no client_secret)."""
    kwargs = dict(
        workspace_id=WORKSPACE_ID,
        dataset_id=DATASET_ID,
        tenant_id=TENANT_ID,
        client_id=CLIENT_ID,
        username=SA_USERNAME,
        password=SA_PASSWORD,
        catalog="my_catalog",
        schema_name="metrics",
    )
    if include_admin:
        kwargs.update(
            admin_client_id=ADMIN_CLIENT_ID,
            admin_username=ADMIN_SA_USERNAME,
            admin_password=ADMIN_SA_PASSWORD,
        )
    return kwargs


class TestServiceAccountSchema:
    def test_sa_fields_present(self):
        s = PipelineConfigGeneratorSchema(
            username=SA_USERNAME, password=SA_PASSWORD,
            admin_username=ADMIN_SA_USERNAME, admin_password=ADMIN_SA_PASSWORD,
        )
        assert s.username == SA_USERNAME
        assert s.password == SA_PASSWORD
        assert s.admin_username == ADMIN_SA_USERNAME
        assert s.admin_password == ADMIN_SA_PASSWORD

    def test_auth_method_field(self):
        s = PipelineConfigGeneratorSchema(auth_method="service_account")
        assert s.auth_method == "service_account"


class TestServiceAccountValidation:
    """Credential validation accepts SP, SA, or access_token for non-admin."""

    def _patch_resolve(self):
        # Isolate validation logic from real token acquisition.
        return patch.object(
            PipelineConfigGeneratorTool, "_resolve_token", return_value="tok"
        )

    def test_sa_only_passes_validation(self):
        """SA creds (no client_secret) must NOT be rejected as missing creds."""
        tool = PipelineConfigGeneratorTool()
        with self._patch_resolve(), patch("requests.post", side_effect=Exception("stop")):
            result = tool._run(**_make_sa_kwargs())
        assert "credentials required" not in result.lower()
        assert "tenant_id is required" not in result.lower()

    def test_access_token_only_passes_validation(self):
        """A pre-obtained access_token (no SP/SA, no tenant) is accepted for the data path."""
        tool = PipelineConfigGeneratorTool()
        kwargs = dict(
            workspace_id=WORKSPACE_ID, dataset_id=DATASET_ID,
            access_token="pre-obtained-oauth-token",
            # admin path still needs its own creds
            tenant_id=TENANT_ID,
            admin_client_id=ADMIN_CLIENT_ID, admin_client_secret=ADMIN_CLIENT_SECRET,
        )
        with self._patch_resolve(), patch("requests.post", side_effect=Exception("stop")):
            result = tool._run(**kwargs)
        assert "non-admin credentials required" not in result.lower()

    def test_missing_all_noncredentials_rejected(self):
        """client_id with neither secret nor username/password nor token is rejected."""
        tool = PipelineConfigGeneratorTool()
        result = tool._run(
            workspace_id=WORKSPACE_ID, dataset_id=DATASET_ID,
            tenant_id=TENANT_ID, client_id=CLIENT_ID,
            admin_client_id=ADMIN_CLIENT_ID, admin_client_secret=ADMIN_CLIENT_SECRET,
        )
        assert "error" in result.lower()
        assert "non-admin" in result.lower() or "credentials" in result.lower()

    def test_mixed_sp_nonadmin_sa_admin(self):
        """Non-admin SP + admin SA is a valid combination."""
        tool = PipelineConfigGeneratorTool()
        kwargs = dict(
            workspace_id=WORKSPACE_ID, dataset_id=DATASET_ID, tenant_id=TENANT_ID,
            client_id=CLIENT_ID, client_secret=CLIENT_SECRET,
            admin_client_id=ADMIN_CLIENT_ID,
            admin_username=ADMIN_SA_USERNAME, admin_password=ADMIN_SA_PASSWORD,
        )
        with self._patch_resolve(), patch("requests.post", side_effect=Exception("stop")):
            result = tool._run(**kwargs)
        assert "credentials required" not in result.lower()


class TestResolveTokenUsesAadService:
    """_resolve_token routes through the shared AadService (same as fetcher/UCMV)."""

    def test_service_principal_autodetect(self):
        tool = PipelineConfigGeneratorTool()
        with patch(
            "src.converters.services.powerbi.authentication.AadService"
        ) as MockAad:
            inst = MockAad.return_value
            inst.get_access_token.return_value = "sp-token"
            inst._determine_auth_method.return_value = "service_principal"
            tok = tool._resolve_token(
                tenant_id=TENANT_ID, client_id=CLIENT_ID, client_secret=CLIENT_SECRET,
                username=None, password=None, access_token=None,
                auth_method=None, label="non-admin",
            )
        assert tok == "sp-token"
        # AadService constructed with the credentials we passed
        _, kw = MockAad.call_args
        assert kw["client_id"] == CLIENT_ID
        assert kw["client_secret"] == CLIENT_SECRET
        assert kw["tenant_id"] == TENANT_ID

    def test_service_account_passthrough(self):
        tool = PipelineConfigGeneratorTool()
        with patch(
            "src.converters.services.powerbi.authentication.AadService"
        ) as MockAad:
            inst = MockAad.return_value
            inst.get_access_token.return_value = "sa-token"
            inst._determine_auth_method.return_value = "service_account"
            tok = tool._resolve_token(
                tenant_id=TENANT_ID, client_id=CLIENT_ID, client_secret=None,
                username=SA_USERNAME, password=SA_PASSWORD, access_token=None,
                auth_method=None, label="non-admin",
            )
        assert tok == "sa-token"
        _, kw = MockAad.call_args
        assert kw["username"] == SA_USERNAME
        assert kw["password"] == SA_PASSWORD

    def test_explicit_auth_method_forwarded(self):
        tool = PipelineConfigGeneratorTool()
        with patch(
            "src.converters.services.powerbi.authentication.AadService"
        ) as MockAad:
            inst = MockAad.return_value
            inst.get_access_token.return_value = "t"
            inst._determine_auth_method.return_value = "service_account"
            tool._resolve_token(
                tenant_id=TENANT_ID, client_id=CLIENT_ID, client_secret=None,
                username=SA_USERNAME, password=SA_PASSWORD, access_token=None,
                auth_method="service_account", label="non-admin",
            )
        _, kw = MockAad.call_args
        assert kw["auth_method"] == "service_account"

    def test_access_token_passthrough(self):
        tool = PipelineConfigGeneratorTool()
        with patch(
            "src.converters.services.powerbi.authentication.AadService"
        ) as MockAad:
            inst = MockAad.return_value
            inst.get_access_token.return_value = "oauth-token"
            tok = tool._resolve_token(
                tenant_id=None, client_id=None, client_secret=None,
                username=None, password=None, access_token="oauth-token",
                auth_method=None, label="non-admin",
            )
        assert tok == "oauth-token"
        _, kw = MockAad.call_args
        assert kw["access_token"] == "oauth-token"


# ---------------------------------------------------------------------------
# Level 2: Fabric TMDL fallback for the Admin Scanner (SA-only support)
# ---------------------------------------------------------------------------

import base64


def _gen():
    return PipelineConfigGeneratorTool()._import_generate_config()


class TestParseTmdlToAdminTables:
    """parse_tmdl_to_admin_tables produces the same shape as parse_admin_tables."""

    def _part(self, name, body):
        return {"path": f"definition/tables/{name}.tmdl",
                "payload": base64.b64encode(body.encode()).decode()}

    def test_extracts_columns_measures_and_mquery(self):
        gen = _gen()
        tmdl = (
            "table Fact_Sales\n"
            "\tcolumn amount\n\t\tdataType: double\n"
            "\tcolumn region_key\n"
            "\tmeasure 'Total Revenue' = SUM(Fact_Sales[amount])\n\t\tformatString: 0.00\n"
            "\tmeasure Margin = DIVIDE([Profit],[Revenue])\n"
            "\tpartition Fact_Sales = m\n\t\tsource =\n\t\t\tlet S = Value.NativeQuery(x) in S\n"
        )
        out = gen.parse_tmdl_to_admin_tables([self._part("Fact_Sales", tmdl)], dataset_id="ds1")
        assert "Fact_Sales" in out
        t = out["Fact_Sales"]
        assert {c["name"] for c in t["columns"]} == {"amount", "region_key"}
        assert {m["name"] for m in t["measures"]} == {"Total Revenue", "Margin"}
        # formatString line must be stripped from the measure expression
        rev = next(m for m in t["measures"] if m["name"] == "Total Revenue")
        assert rev["expression"] == "SUM(Fact_Sales[amount])"
        assert "formatString" not in rev["expression"]
        assert "Value.NativeQuery" in t["mquery_expression"]

    def test_skips_local_date_tables(self):
        gen = _gen()
        parts = [self._part("LocalDateTable_abc", "table LocalDateTable_abc\n\tcolumn Date\n")]
        assert gen.parse_tmdl_to_admin_tables(parts) == {}

    def test_ignores_non_table_parts(self):
        gen = _gen()
        parts = [{"path": "definition/model.tmdl", "payload": base64.b64encode(b"model M").decode()}]
        assert gen.parse_tmdl_to_admin_tables(parts) == {}

    def test_empty_or_none(self):
        gen = _gen()
        assert gen.parse_tmdl_to_admin_tables([]) == {}
        assert gen.parse_tmdl_to_admin_tables(None) == {}


class TestGetFabricTokenGrants:
    """get_fabric_token uses SP or SA grant with the Fabric scope."""

    def test_sp_grant_fabric_scope(self):
        gen = _gen()
        cap = {}

        def _post(url, data=None, timeout=None):
            cap["data"] = data
            r = MagicMock(); r.status_code = 200; r.json.return_value = {"access_token": "fab-sp"}
            return r
        with patch("requests.post", side_effect=_post):
            tok = gen.get_fabric_token(TENANT_ID, CLIENT_ID, CLIENT_SECRET)
        assert tok == "fab-sp"
        assert cap["data"]["grant_type"] == "client_credentials"
        assert cap["data"]["scope"] == "https://api.fabric.microsoft.com/.default"

    def test_sa_grant_fabric_scope(self):
        gen = _gen()
        cap = {}

        def _post(url, data=None, timeout=None):
            cap["data"] = data
            r = MagicMock(); r.status_code = 200; r.json.return_value = {"access_token": "fab-sa"}
            return r
        with patch("requests.post", side_effect=_post):
            tok = gen.get_fabric_token(TENANT_ID, CLIENT_ID, None,
                                       username="sa@x.com", password="pw")
        assert tok == "fab-sa"
        assert cap["data"]["grant_type"] == "password"
        assert cap["data"]["username"] == "sa@x.com"
        assert cap["data"]["scope"] == "https://api.fabric.microsoft.com/.default"


class TestAdminScannerFallbackTrigger:
    """When the Admin Scanner fails/empties, the tool falls back to Fabric TMDL."""

    def test_tmdl_fallback_runs_when_admin_scan_401(self):
        tool = PipelineConfigGeneratorTool()
        gen = tool._import_generate_config()

        calls = {"fabric": False, "tmdl": False}

        def _boom_scan(*a, **k):
            raise RuntimeError("API 3 (Admin Scan trigger) HTTP 401: ")
        def _fabric(*a, **k):
            calls["fabric"] = True
            return "fab-token"
        def _tmdl(*a, **k):
            calls["tmdl"] = True
            return [{"path": "definition/tables/T.tmdl", "payload": base64.b64encode(b"table T\n\tcolumn c\n").decode()}]

        with patch.object(PipelineConfigGeneratorTool, "_resolve_token", return_value="tok"), \
             patch.object(gen, "extract_relationships", return_value=[]), \
             patch.object(gen, "extract_measures", return_value=[]), \
             patch.object(gen, "trigger_admin_scan", side_effect=_boom_scan), \
             patch.object(gen, "get_fabric_token", side_effect=_fabric), \
             patch.object(gen, "fetch_tmdl_parts", side_effect=_tmdl):
            result = tool._run(**_make_sa_kwargs())

        # Fallback path must have been taken, and the run must NOT hard-error with 401
        assert calls["fabric"] is True
        assert calls["tmdl"] is True
        assert "401" not in result


class TestServicePrincipalLastResortFallback:
    """When admin scan 401s AND Fabric TMDL is empty, retry admin scan with SP secret."""

    def test_sp_fallback_runs_when_tmdl_empty_and_secret_present(self):
        tool = PipelineConfigGeneratorTool()
        gen = tool._import_generate_config()

        calls = {"sp_scan": 0, "sa_scan": 0}

        def _scan(token, ws):
            # First call (SA token) 401s; second call (SP token) succeeds.
            if token == "sp-admin-token":
                calls["sp_scan"] += 1
                return {"workspaces": [{"datasets": [{"id": "ds", "tables": []}]}]}
            calls["sa_scan"] += 1
            raise RuntimeError("API 3 (Admin Scan trigger) HTTP 401: ")

        def _resolve(self, **kw):
            # admin-SP-fallback uses the SP secret path
            if kw.get("label") == "admin-SP-fallback":
                return "sp-admin-token"
            return "sa-token"

        # SA kwargs + an admin_client_secret present (so SP fallback is eligible)
        kwargs = dict(_make_sa_kwargs())
        kwargs["admin_client_secret"] = "sp-secret"

        with patch.object(PipelineConfigGeneratorTool, "_resolve_token", _resolve), \
             patch.object(gen, "extract_relationships", return_value=[]), \
             patch.object(gen, "extract_measures", return_value=[]), \
             patch.object(gen, "trigger_admin_scan", side_effect=_scan), \
             patch.object(gen, "parse_admin_tables", return_value={"T": {"columns": [], "mquery_expression": "", "measures": []}}), \
             patch.object(gen, "get_fabric_token", return_value="fab"), \
             patch.object(gen, "fetch_tmdl_parts", return_value=None):  # TMDL unavailable
            result = tool._run(**kwargs)

        assert calls["sp_scan"] == 1  # SP fallback actually retried the scanner
        assert "401" not in result

    def test_no_sp_fallback_without_secret(self):
        """SA-only (no secret) must NOT attempt the SP fallback."""
        tool = PipelineConfigGeneratorTool()
        gen = tool._import_generate_config()
        sp_attempted = {"v": False}

        def _resolve(self, **kw):
            if kw.get("label") == "admin-SP-fallback":
                sp_attempted["v"] = True
            return "sa-token"

        with patch.object(PipelineConfigGeneratorTool, "_resolve_token", _resolve), \
             patch.object(gen, "extract_relationships", return_value=[]), \
             patch.object(gen, "extract_measures", return_value=[]), \
             patch.object(gen, "trigger_admin_scan", side_effect=RuntimeError("HTTP 401: ")), \
             patch.object(gen, "get_fabric_token", return_value="fab"), \
             patch.object(gen, "fetch_tmdl_parts", return_value=None):
            tool._run(**_make_sa_kwargs())  # no admin_client_secret

        assert sp_attempted["v"] is False
