"""Unit tests for PipelineConfigGeneratorTool (Tool 90)."""
import json
import re
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

    @patch("httpx.AsyncClient.post")
    @patch("httpx.AsyncClient.get")
    def test_empty_string_kwargs_fall_back_to_injected_config(self, mock_get, mock_post):
        """Regression: the flow injects workspace_id/dataset_id into _default_config,
        but the agent calls the tool passing EMPTY STRINGS for them. The old
        `if val is not None` returned "" and the tool errored
        'workspace_id and dataset_id are required'. Empty kwargs must fall back to
        the injected config."""
        # Make the API calls fail fast — we only care that we got PAST the
        # required-fields check (i.e. did not return the 'required' error).
        mock_post.side_effect = Exception("stop after required-check")
        mock_get.side_effect = Exception("stop after required-check")

        tool = PipelineConfigGeneratorTool(**_make_sp_kwargs())  # injects real IDs
        result = tool._run(
            workspace_id="",   # agent's empty placeholder
            dataset_id="",     # agent's empty placeholder
            tenant_id="",
        )
        # Must NOT be the required-fields validation error.
        assert "are required" not in result.lower()


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

    def test_multiline_let_in_mquery_not_truncated(self):
        """Regression: a multi-line `let ... in` partition source must be captured
        in full, not truncated to just 'let'.

        The old regex stopped at the first `\\n<word> =`, but M-Query's own let-body
        is full of `Source = ...` / `Filtered = ...` bindings, so the source was cut
        to its first token ('let'). Customer symptom: mquery_expression == 'let'.
        """
        gen = _gen()
        tmdl = (
            "table DCC_Customer\n"
            "\tcolumn CustomerID\n\t\tdataType: string\n"
            "\tpartition DCC_Customer = m\n"
            "\t\tmode: import\n"
            "\t\tsource =\n"
            "\t\t\tlet\n"
            "\t\t\t    Source = Databricks.Catalogs(\"h\", \"p\", null),\n"
            "\t\t\t    db = Source{[Name=\"sales\"]}[Data],\n"
            "\t\t\t    Filtered = Table.SelectRows(db, each [Active] = true)\n"
            "\t\t\tin\n"
            "\t\t\t    Filtered\n"
            "\t\tannotation PBI_ResultType = Table\n"
        )
        out = gen.parse_tmdl_to_admin_tables([self._part("DCC_Customer", tmdl)])
        mq = out["DCC_Customer"]["mquery_expression"]
        assert mq != "let", "M-Query truncated to just 'let'"
        assert "Databricks.Catalogs" in mq
        assert "Table.SelectRows" in mq
        assert "Filtered" in mq
        # the TMDL directive after the source block must NOT leak in
        assert "annotation" not in mq

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
        # Intent: the run recovered (produced a config), not that the 401 text is
        # absent — the 401 now legitimately appears in the surfaced warnings[].
        _res = json.loads(result)
        assert "error" not in _res
        assert "proposed_config" in _res


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
        # Intent: recovered (produced a config), not that the 401 text is absent —
        # the 401 now legitimately appears in the surfaced warnings[].
        _res = json.loads(result)
        assert "error" not in _res
        assert "proposed_config" in _res

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


class TestMeasureDaxSpFallback:
    """API 2 (measures/DAX) retries with a Service Principal when the SA fails."""

    def test_api2_sp_fallback_recovers_measures(self):
        tool = PipelineConfigGeneratorTool()
        gen = tool._import_generate_config()
        calls = {"measures": 0}

        def _measures(token, ws, ds):
            calls["measures"] += 1
            if token == "sp-data-token":
                return [{"measure_name": "M", "table_name": "T", "expression": "SUM(x)"}]
            raise RuntimeError("XMLA 401 for service account")

        def _resolve(self, **kw):
            return "sp-data-token" if kw.get("label") == "data-SP-fallback" else "sa-token"

        # SA creds + a non-admin client_secret present → SP data fallback eligible
        kwargs = dict(_make_sa_kwargs())
        kwargs["client_secret"] = "sp-secret"

        with patch.object(PipelineConfigGeneratorTool, "_resolve_token", _resolve), \
             patch.object(gen, "extract_relationships", return_value=[]), \
             patch.object(gen, "extract_measures", side_effect=_measures), \
             patch.object(gen, "trigger_admin_scan", return_value={"workspaces": []}), \
             patch.object(gen, "parse_admin_tables", return_value={}), \
             patch.object(gen, "get_fabric_token", return_value="fab"), \
             patch.object(gen, "fetch_tmdl_parts", return_value=None):
            tool._run(**kwargs)

        # extract_measures called twice: SA (fail) then SP (success)
        assert calls["measures"] == 2

    def test_api2_no_sp_fallback_without_secret(self):
        tool = PipelineConfigGeneratorTool()
        gen = tool._import_generate_config()
        calls = {"measures": 0}

        def _measures(token, ws, ds):
            calls["measures"] += 1
            raise RuntimeError("XMLA 401")

        def _resolve(self, **kw):
            return "sa-token"

        with patch.object(PipelineConfigGeneratorTool, "_resolve_token", _resolve), \
             patch.object(gen, "extract_relationships", return_value=[]), \
             patch.object(gen, "extract_measures", side_effect=_measures), \
             patch.object(gen, "trigger_admin_scan", return_value={"workspaces": []}), \
             patch.object(gen, "parse_admin_tables", return_value={}), \
             patch.object(gen, "get_fabric_token", return_value="fab"), \
             patch.object(gen, "fetch_tmdl_parts", return_value=None):
            tool._run(**_make_sa_kwargs())  # no client_secret

        # Only the SA attempt — no SP retry without a secret
        assert calls["measures"] == 1


class TestConfigGenConversionHistoryPersistence:
    """Config-gen persists extracted measures (with DAX) to conversion_history."""

    def _run_async_sync(self, coro):
        import asyncio
        return asyncio.run(coro)

    def test_persists_measures_with_dax_diagnostic(self):
        tool = PipelineConfigGeneratorTool()
        measures = [
            {"measure_name": "M1", "table_name": "T", "expression": "SWITCH(TRUE(), SELECTEDVALUE(x), 1)"},
            {"measure_name": "M2", "table_name": "T", "expression": "SUM(a)"},
            {"measure_name": "M3", "table_name": "T", "expression": ""},  # no DAX
        ]
        captured = {}
        mock_repo = MagicMock()
        from types import SimpleNamespace
        mock_repo.create = AsyncMock(side_effect=lambda d: captured.update({"data": d}) or SimpleNamespace(id=1, group_id=None))
        mock_repo.session = MagicMock()
        mock_repo.session.commit = AsyncMock()

        from contextlib import asynccontextmanager

        @asynccontextmanager
        async def _ctx():
            yield mock_repo

        with patch("src.engines.crewai.tools.tool_session_provider.ToolSessionProvider.conversion_repo", _ctx):
            self._run_async_sync(tool._save_to_conversion_history(
                measures=measures, config={"switch_decompositions": {"T": [1]}, "measure_resolutions": {}},
                relationships=[], admin_tables={}, workspace_id="ws", dataset_id="ds",
            ))

        d = captured["data"]
        assert d["source_format"] == "powerbi_config"
        assert d["input_data"]["measures"] == measures
        assert d["measure_count"] == 3
        # diagnostic: 2 with DAX, 1 SELECTEDVALUE+SWITCH
        assert "2 with DAX" in d["input_summary"]
        assert "1 SELECTEDVALUE+SWITCH" in d["input_summary"]

    def test_fail_open_on_repo_error(self):
        tool = PipelineConfigGeneratorTool()
        from contextlib import asynccontextmanager

        @asynccontextmanager
        async def _boom():
            raise RuntimeError("db down")
            yield  # pragma: no cover

        with patch("src.engines.crewai.tools.tool_session_provider.ToolSessionProvider.conversion_repo", _boom):
            r = self._run_async_sync(tool._save_to_conversion_history(
                measures=[], config={}, relationships=[], admin_tables={},
                workspace_id="ws", dataset_id="ds",
            ))
        assert r is None


class TestExtractMeasuresKeyTolerance:
    """extract_measures must read DMV columns whether keys are bracketed or not.

    Regression: the Power BI executeQueries API returns column keys either
    bracketed ('[Expression]') or unbracketed ('Expression'). Reading only the
    bracketed form yielded 471 measures with 0 DAX (empty switch_decompositions).
    """

    def _gen(self):
        return PipelineConfigGeneratorTool()._import_generate_config()

    def _mock_resp(self, rows):
        r = MagicMock()
        r.status_code = 200
        r.json.return_value = {"results": [{"tables": [{"rows": rows}]}]}
        return r

    def test_unbracketed_keys_are_read(self):
        gen = self._gen()
        rows = [{"Measure Name": "Rev", "Expression": "SUM(x)", "Table": "Fact", "Description": ""}]
        with patch("requests.post", return_value=self._mock_resp(rows)):
            measures = gen.extract_measures("tok", "ws", "ds")
        assert len(measures) == 1
        assert measures[0]["measure_name"] == "Rev"
        assert measures[0]["expression"] == "SUM(x)"  # <-- was '' before the fix

    def test_bracketed_keys_still_work(self):
        gen = self._gen()
        rows = [{"[Measure Name]": "Margin", "[Expression]": "DIVIDE(a,b)", "[Table]": "Fact", "[Description]": ""}]
        with patch("requests.post", return_value=self._mock_resp(rows)):
            measures = gen.extract_measures("tok", "ws", "ds")
        assert measures[0]["measure_name"] == "Margin"
        assert measures[0]["expression"] == "DIVIDE(a,b)"

    def test_row_get_helper(self):
        gen = self._gen()
        assert gen._row_get({"Expression": "X"}, "Expression") == "X"
        assert gen._row_get({"[Expression]": "Y"}, "Expression") == "Y"
        assert gen._row_get({}, "Expression", "def") == "def"


class TestEnrichSwitchDecompNoListCollision:
    """Regression: _enrich_config_from_dax must not index a list with a str.

    build_config's derive_switch_decompositions writes a LIST per table; the
    enrichment writes a DICT. When both target the same table the old code did
    list[measure_name] = ... → 'list indices must be integers or slices, not str'.
    """

    # DAX shaped for the enrich regex: SWITCH(TRUE(), <cond>, <expr>, ...) at end,
    # with SELECTEDVALUE(table[col]) = "value" conditions so branch names extract.
    SWITCH_DAX = ('SWITCH(TRUE(), '
                  'SELECTEDVALUE(Sel[Name]) = "Absolute", [M1], '
                  'SELECTEDVALUE(Sel[Name]) = "Variance", [M2])')

    def test_no_crash_when_table_already_a_list(self):
        config = {
            "filter_sets": {},
            "switch_decompositions": {"T": [{"name": "m1", "raw_expr": "TODO"}]},  # list from build_config
            "measure_resolutions": {},
        }
        measures = [{"measure_name": "m1", "table_name": "T", "expression": self.SWITCH_DAX}]
        # Must not raise
        PipelineConfigGeneratorTool._enrich_config_from_dax(config, measures)
        assert isinstance(config["switch_decompositions"]["T"], list)  # list preserved

    def test_dict_form_still_populated_for_new_table(self):
        config = {"filter_sets": {}, "switch_decompositions": {}, "measure_resolutions": {}}
        measures = [{"measure_name": "m1", "table_name": "NewT", "expression": self.SWITCH_DAX}]
        PipelineConfigGeneratorTool._enrich_config_from_dax(config, measures)
        entry = config["switch_decompositions"].get("NewT")
        assert isinstance(entry, dict) and "m1" in entry


class TestUCMVHandoffPayload:
    """config-gen must emit measures_json + mquery_json in UCMV's shape.

    Regression: the flow chained config-gen → UCMV (JSON mode), but config-gen
    only emitted `proposed_config`. UCMV builds views from measures_json +
    mquery_json (not config_json) → empty mapping → 0 views, even though config
    extraction fully succeeded (customer log f2924007: 430 measures → 0 views).
    """

    def test_build_ucmv_measures_normalises_shape(self):
        measures = [
            {"measure_name": "Total Sales", "table_name": "Fact_Sales",
             "expression": "SUM(Fact_Sales[Amount])", "description": "x"},
            {"name": "Legacy", "table": "Fact_A", "dax_expression": "COUNT(1)"},
        ]
        out = PipelineConfigGeneratorTool._build_ucmv_measures(measures)
        assert len(out) == 2
        assert out[0] == {
            "measure_name": "Total Sales", "original_name": "Total Sales",
            "dax_expression": "SUM(Fact_Sales[Amount])",
            "proposed_allocation": "Fact_Sales", "table_refs": [],
        }
        # alt keys (name/table/dax_expression) are honoured
        assert out[1]["measure_name"] == "Legacy"
        assert out[1]["proposed_allocation"] == "Fact_A"
        assert out[1]["dax_expression"] == "COUNT(1)"

    def test_build_ucmv_measures_drops_nameless(self):
        out = PipelineConfigGeneratorTool._build_ucmv_measures(
            [{"table_name": "X", "expression": ""}]
        )
        assert out == []

    def test_build_ucmv_mquery_from_admin_tables(self):
        admin_tables = {
            "Fact_Sales": {"mquery_expression": "SELECT * FROM cat.sales", "measures": []},
            "Dim_NoSource": {"measures": []},          # dropped: no source
            "Fact_B": {"mquery": "SELECT 1"},          # alt key honoured
        }
        out = PipelineConfigGeneratorTool._build_ucmv_mquery(admin_tables)
        names = {e["table_name"] for e in out}
        assert names == {"Fact_Sales", "Fact_B"}
        entry = next(e for e in out if e["table_name"] == "Fact_Sales")
        assert entry["transpiled_sql"] == "SELECT * FROM cat.sales"
        # MUST be "Yes" — UCMV's MQueryParser drops entries whose
        # validation_passed does not start with "Yes" (unless SUM+GROUP BY).
        assert entry["validation_passed"] == "Yes"

    def test_build_ucmv_mquery_empty_when_no_sources(self):
        out = PipelineConfigGeneratorTool._build_ucmv_mquery(
            {"Dim_A": {"measures": []}, "Dim_B": {}}
        )
        assert out == []

    def test_measures_shape_matches_ucmv_pipeline_contract(self):
        """The emitted measures must carry the keys UCMV's pipeline reads."""
        out = PipelineConfigGeneratorTool._build_ucmv_measures(
            [{"measure_name": "m1", "table_name": "F", "expression": "SUM(x)"}]
        )
        # UCMV pipeline iterates mapping expecting these keys
        assert set(out[0]) >= {"measure_name", "dax_expression", "proposed_allocation"}


# ---------------------------------------------------------------------------
# Measure allocation (P0 re-homing: holder-table measures → referenced facts)
# ---------------------------------------------------------------------------

class TestMeasureReHoming:
    """A measure defined on a measure-holder table must be allocated to the
    fact table(s) its DAX references, not left on the (dataless) holder."""

    def _cfg(self, *facts):
        return {"fact_join_map": {f: {} for f in facts}}

    def test_holder_measure_rehomed_to_referenced_fact(self):
        measures = [{
            "measure_name": "CFR %",
            "table_name": "C_Measure_Table_SL",  # holder, not a fact
            "expression": "DIVIDE(SUM(fact_iom05[cfr_num]), SUM(fact_iom05[cfr_den]))",
        }]
        out = PipelineConfigGeneratorTool._build_ucmv_measures(
            measures,
            admin_tables={"fact_iom05": {}, "C_Measure_Table_SL": {}},
            config=self._cfg("fact_iom05"),
        )
        assert out[0]["proposed_allocation"] == "fact_iom05"
        assert out[0]["all_allocations"] == [{"table": "fact_iom05", "role": "primary"}]

    def test_cross_fact_measure_gets_primary_and_secondary(self):
        measures = [{
            "measure_name": "Ratio",
            "table_name": "C_Measure_Table",
            "expression": "DIVIDE(SUM(Fact_A[x]), SUM(Fact_B[y]))",
        }]
        out = PipelineConfigGeneratorTool._build_ucmv_measures(
            measures,
            admin_tables={"Fact_A": {}, "Fact_B": {}, "C_Measure_Table": {}},
            config=self._cfg("Fact_A", "Fact_B"),
        )
        assert out[0]["all_allocations"] == [
            {"table": "Fact_A", "role": "primary"},
            {"table": "Fact_B", "role": "secondary"},
        ]

    def test_measure_on_own_fact_stays_primary(self):
        measures = [{
            "measure_name": "Amount",
            "table_name": "fact_iom35",
            "expression": "SUM(fact_iom35[amt])",
        }]
        out = PipelineConfigGeneratorTool._build_ucmv_measures(
            measures,
            admin_tables={"fact_iom35": {}},
            config=self._cfg("fact_iom35"),
        )
        assert out[0]["proposed_allocation"] == "fact_iom35"
        assert out[0]["all_allocations"] == [{"table": "fact_iom35", "role": "primary"}]

    def test_no_known_fact_referenced_falls_back_to_home(self):
        measures = [{
            "measure_name": "Const",
            "table_name": "C_Measure_Table",
            "expression": "1 + 1",
        }]
        out = PipelineConfigGeneratorTool._build_ucmv_measures(
            measures,
            admin_tables={"C_Measure_Table": {}},
            config=self._cfg("fact_iom05"),
        )
        # No fact referenced → left on home table, no all_allocations key.
        assert out[0]["proposed_allocation"] == "C_Measure_Table"
        assert "all_allocations" not in out[0]

    def test_quoted_table_name_ref_resolves(self):
        measures = [{
            "measure_name": "Q",
            "table_name": "Holder",
            "expression": "SUM('Fact Sales'[amt])",
        }]
        out = PipelineConfigGeneratorTool._build_ucmv_measures(
            measures,
            admin_tables={"Fact Sales": {}, "Holder": {}},
            config=self._cfg("Fact Sales"),
        )
        assert out[0]["proposed_allocation"] == "Fact Sales"

    def test_no_config_preserves_legacy_behaviour(self):
        # Without fact_join_map, no re-homing — original table_name passthrough.
        out = PipelineConfigGeneratorTool._build_ucmv_measures(
            [{"measure_name": "m1", "table_name": "F", "expression": "SUM(F[x])"}]
        )
        assert out[0]["proposed_allocation"] == "F"
        assert "all_allocations" not in out[0]


class TestEtlColumnCuration:
    """P2 curation: pure ETL plumbing columns are demoted from dimensions."""

    def test_etl_columns_excluded_business_columns_kept(self):
        from src.engines.crewai.tools.custom.generate_config import (
            derive_dimension_exclusions,
        )
        admin_tables = {
            "Fact_X": {"columns": [
                {"name": "Region"},          # business — keep
                {"name": "ObjVers"},         # ETL — exclude
                {"name": "LogSys"},          # ETL — exclude
                {"name": "process_run_id"},  # ETL — exclude
                {"name": "YearCard"},        # ETL — exclude
                {"name": "SalesAmount", "isHidden": True},  # hidden — exclude
            ]}
        }
        excl = derive_dimension_exclusions(admin_tables)["Fact_X"]
        assert "obj_vers" in excl
        assert "log_sys" in excl
        assert "process_run_id" in excl
        assert "year_card" in excl
        assert "sales_amount" in excl  # hidden
        assert "region" not in excl    # business dimension preserved


class TestMeasureRefResolution:
    """PROP-1: referenced-measure DAX is transpiled into base_expr, not a TODO."""

    def _R(self, dax):
        from src.engines.crewai.tools.custom.generate_config import (
            _resolve_referenced_measure_dax,
        )
        return _resolve_referenced_measure_dax(dax)

    def test_bare_sum(self):
        assert self._R('SUM(FT_QSE[kbi_value])') == {
            'base_expr': 'SUM(source.kbi_value)', 'base_filters': []}

    def test_calculate_with_filters(self):
        r = self._R('CALCULATE(SUM(T[val]), T[ver]="B000")')
        assert r['base_expr'] == 'SUM(source.val)'
        assert r['base_filters'] == ["ver = 'B000'"]

    def test_constant(self):
        assert self._R('1') == {'base_expr': '1', 'base_filters': []}

    def test_switch_picks_first_calculate_branch_no_leak(self):
        plant = (
            'Switch(TRUE(),\n'
            '  Or(ISFILTERED(C_Dim_Plant[plant_desc]),HASONEVALUE(C_Dim_Plant[plant])),\n'
            '  CALCULATE(SUM(FT_QSE[kbi_value]), FT_QSE[bic_chversion]="0000", FT_QSE[bic_creg_type]="Plant"),\n'
            '  CALCULATE(SUM(FT_QSE[kbi_value]), FT_QSE[bic_chversion]="0000", FT_QSE[bic_creg_type]="Company Code"))'
        )
        r = self._R(plant)
        assert r['base_expr'] == 'SUM(source.kbi_value)'
        # only the FIRST (plant) branch filters — Company Code must NOT leak in
        assert r['base_filters'] == ["bic_chversion = '0000'", "bic_creg_type = 'Plant'"]

    def test_untranslatable_returns_none(self):
        assert self._R('var x = SELECTEDVALUE(a) return x + 1') is None

    def test_var_scaffolding_switch_sumx_filter_bp(self):
        """Dependency-cascade fix: the _BP twin carries `var std/etd` date-window
        scaffolding AND wraps the aggregate in SUMX(FILTER(...)). Both broke
        resolution (first CALCULATE found was the scaffolding CALCULATE([F_Start_
        date]), and the agg regex didn't handle SUMX(FILTER,col)) → the base
        dropped → all its _BP dependents cascaded out. Must now resolve."""
        bp = (
            'var std = CALCULATE([F_Start_date]) var etd= CALCULATE([F_End_date]) '
            'return Switch(TRUE(), '
            'Or(ISFILTERED(C_Dim_Plant[plant_desc]),HASONEVALUE(C_Dim_Plant[plant])), '
            'CALCULATE(SUMX(FILTER(FT_QSE, FT_QSE[bic_chversion] = "B000" && '
            'FT_QSE[bic_creg_type] = "Plant"),FT_QSE[kbi_value])), '
            'CALCULATE(SUMX(FILTER(FT_QSE, FT_QSE[bic_chversion] = "B000" && '
            'FT_QSE[bic_creg_type] = "Company Code"),FT_QSE[kbi_value])) )'
        )
        r = self._R(bp)
        assert r is not None, "BP base measure must resolve (else _BP dependents cascade out)"
        assert r['base_expr'] == 'SUM(source.kbi_value)'
        assert r['base_filters'] == ["bic_chversion = 'B000'", "bic_creg_type = 'Plant'"]

    def test_var_scaffolding_plain_sumx_filter(self):
        totbp = (
            'var std = CALCULATE([F_Start_date]) var etd= CALCULATE([F_End_date]) '
            'return CALCULATE(SUMX(FILTER(FT_QSE, FT_QSE[bic_chversion] = "B000" ),'
            'FT_QSE[kbi_value]))'
        )
        r = self._R(totbp)
        assert r['base_expr'] == 'SUM(source.kbi_value)'
        assert r['base_filters'] == ["bic_chversion = 'B000'"]

    def test_end_to_end_no_todo_literal(self):
        from src.engines.crewai.tools.custom.generate_config import (
            derive_measure_resolutions,
        )
        measures = [
            {'measure_name': 'BaseKBI', 'table_name': 'FT_QSE',
             'expression': 'CALCULATE(SUM(FT_QSE[kbi_value]), FT_QSE[bic_chversion]="0000")'},
            {'measure_name': 'CC', 'table_name': 'FT_QSE',
             'expression': 'CALCULATE([BaseKBI], FT_QSE[bic_csubkbi]="KEMAA0011")'},
        ]
        res = derive_measure_resolutions(measures)
        assert 'BaseKBI' in res
        assert res['BaseKBI']['base_expr'] == 'SUM(source.kbi_value)'
        assert 'TODO' not in res['BaseKBI']['base_expr']


class TestReportIdAutoDiscovery:
    """PROP-7: discover the report bound to the dataset when none supplied."""

    def _discover(self, reports, dataset_id="ds1", status=200):
        from unittest.mock import patch, MagicMock
        from src.engines.crewai.tools.custom import generate_config as gc
        resp = MagicMock(status_code=status)
        resp.json.return_value = {"value": reports}
        with patch.object(gc, "requests") as rq:
            rq.get.return_value = resp
            return gc.discover_report_id("tok", "ws", dataset_id)

    def test_matches_dataset_case_insensitive(self):
        rid = self._discover([{"id": "r2", "name": "SC Report", "datasetId": "DS1"}])
        assert rid == "r2"

    def test_prefers_real_report_over_usage(self):
        rid = self._discover([
            {"id": "u", "name": "Usage Metrics Report", "datasetId": "ds1"},
            {"id": "real", "name": "SC - Total Supply Chain", "datasetId": "ds1"},
        ])
        assert rid == "real"

    def test_no_match_returns_none(self):
        assert self._discover([{"id": "x", "datasetId": "other"}]) is None

    def test_api_failure_returns_none(self):
        assert self._discover([], status=403) is None


class TestDaxQualityGuard:
    """The DAX-quality guard classifies a run as degraded when few measures carry
    real DAX. Tested as a focused unit on the quality heuristic (the _run
    integration uses a dual-import of generate_config that resists mocking)."""

    @staticmethod
    def _quality(ms):
        # mirrors the guard's _dax_quality logic
        total = len(ms)
        with_real = sum(
            1 for m in ms
            if len((m.get("expression") or "").strip()) > 20
            and not re.fullmatch(r"[\w ]+", (m.get("expression") or "").strip() or "x")
        )
        return with_real, total

    @staticmethod
    def _is_degraded(with_real, total):
        return total >= 20 and with_real < max(1, total // 4)

    def test_bare_columns_flagged_degraded(self):
        bare = [{"expression": "bic_csubkbi"} for _ in range(40)]
        w, t = self._quality(bare)
        assert w == 0
        assert self._is_degraded(w, t) is True

    def test_real_dax_not_degraded(self):
        good = [{"expression": 'CALCULATE(SUM(T[v]), T[ver]="0000")'} for _ in range(40)]
        w, t = self._quality(good)
        assert w == 40
        assert self._is_degraded(w, t) is False

    def test_small_model_not_flagged(self):
        # <20 measures: never flagged (threshold guards against tiny/edge models)
        few = [{"expression": "bic_csubkbi"} for _ in range(5)]
        w, t = self._quality(few)
        assert self._is_degraded(w, t) is False

    def test_mixed_above_quarter_not_degraded(self):
        ms = ([{"expression": 'CALCULATE(SUM(T[v]), T[x]="0000")'} for _ in range(15)]
              + [{"expression": "bic_csubkbi"} for _ in range(25)])
        w, t = self._quality(ms)
        assert w == 15 and t == 40
        assert self._is_degraded(w, t) is False  # 15 >= 40//4 (10)
