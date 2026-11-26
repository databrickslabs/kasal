"""A rejected Power BI login is an error of the run, not a warning next to thin output."""

import json
from unittest.mock import patch

from src.services.tools.pipeline_config_generator_tool import (
    PipelineConfigGeneratorTool,
)
from src.services.tools.pipeline_config_generator_utils.source_access import (
    rejected_login_error,
)

_SCANNER_401 = "API 3 (Admin Scanner) failed: API 3 (Admin Scan trigger) HTTP 401: "
_INVALID_SECRET = (
    "Fabric TMDL fallback failed: Auth (Fabric API, service account) HTTP 401: "
    "{'error': 'invalid_client', 'error_description': 'AADSTS7000215: Invalid "
    "client secret provided.'}"
)
_SP_INVALID_SECRET = (
    "Service Principal Admin Scanner fallback failed: Error retrieving access "
    "token: Authentication failed: AADSTS7000215: Invalid client secret provided."
)
_EXPIRED_SECRET = (
    "Service Principal Admin Scanner fallback failed: AADSTS7000222: The provided "
    "client secret keys for app are expired."
)
_TMDL_EMPTY = (
    "Fabric TMDL fallback returned no data (workspace may not be Fabric-enabled)."
)

_KWARGS = {
    "workspace_id": "11111111-1111-1111-1111-111111111111",
    "dataset_id": "22222222-2222-2222-2222-222222222222",
    "tenant_id": "33333333-3333-3333-3333-333333333333",
    "client_id": "44444444-4444-4444-4444-444444444444",
    "username": "svc@example.com",
    "password": "not-a-real-password",
    "admin_client_id": "55555555-5555-5555-5555-555555555555",
    "admin_username": "admin-svc@example.com",
    "admin_password": "not-a-real-password",
    "catalog": "my_catalog",
    "schema_name": "metrics",
}


class TestRejectedLoginError:
    def test_no_tables_and_a_rejected_secret_is_an_error(self):
        error = rejected_login_error(
            {}, [_SCANNER_401, _INVALID_SECRET, _SP_INVALID_SECRET]
        )

        assert error is not None
        assert "rejected the configured client secret" in error
        assert "AADSTS7000215" in error

    def test_an_expired_secret_is_an_error_too(self):
        assert rejected_login_error({}, [_SCANNER_401, _EXPIRED_SECRET]) is not None

    def test_a_failed_tier_next_to_a_successful_one_stays_a_warning(self):
        tables = {"fact_sales": {"mquery_expression": "let Source = 1 in Source"}}

        assert rejected_login_error(tables, [_SCANNER_401, _INVALID_SECRET]) is None

    def test_missing_sources_without_a_rejected_login_stay_a_warning(self):
        assert rejected_login_error({}, [_SCANNER_401, _TMDL_EMPTY]) is None

    def test_no_warnings_is_not_an_error(self):
        assert rejected_login_error({}, []) is None

    def test_the_detail_is_cut_short(self):
        error = rejected_login_error({}, ["AADSTS7000215 " + "x" * 5000])

        assert error is not None
        assert len(error) < 800


class TestToolReturnsTheError:
    @staticmethod
    def _run(fabric_token_error: str) -> tuple[dict, bool]:
        tool = PipelineConfigGeneratorTool()
        gen = tool._import_generate_config()
        with (
            patch.object(
                PipelineConfigGeneratorTool,
                "_resolve_token",
                lambda *_args, **_kwargs: "sa-token",
            ),
            patch.object(gen, "extract_relationships", return_value=[]),
            patch.object(gen, "extract_measures", return_value=[]),
            patch.object(
                gen, "trigger_admin_scan", side_effect=RuntimeError("HTTP 401: ")
            ),
            patch.object(
                gen, "get_fabric_token", side_effect=RuntimeError(fabric_token_error)
            ),
            patch.object(PipelineConfigGeneratorTool, "_persist_run") as persist,
        ):
            result = json.loads(tool._run(**_KWARGS))
        return result, persist.called

    def test_a_rejected_secret_ends_the_run_with_an_error(self):
        result, persisted = self._run(
            "Auth (Fabric API, service account) HTTP 401: invalid_client "
            "AADSTS7000215: Invalid client secret provided."
        )

        assert set(result) == {"error"}
        assert "rejected the configured client secret" in result["error"]
        assert persisted is False

    def test_another_failure_still_returns_a_config(self):
        result, persisted = self._run("HTTP 503: service unavailable")

        assert "error" not in result
        assert "proposed_config" in result
        assert persisted is True
