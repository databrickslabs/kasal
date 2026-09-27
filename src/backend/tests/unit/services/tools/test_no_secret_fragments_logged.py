"""No token or key fragments in logs or responses, anywhere in ``src/`` (audit N4, V3-4).

``tool_factory`` logged the first 10 characters of OBO tokens and the first and
last 4 of decrypted API keys at INFO; the Jobs and Perplexity tools logged a
"masked" token; the UCMV generator logged the PowerBI token's first and last 4;
``GET /connections/test-api-key`` returned the first 4 of each LLM key; and a
debug endpoint returned 20 characters of a bearer token. For a PAT (``dapi`` +
32 hex) eight known characters are a real entropy reduction. Only ``bool(token)``
and the auth method may be logged or returned.

This used to scan three files, so a new fragment anywhere else went unnoticed.
It now scans every module under ``src/``.
"""

import ast
import re
from pathlib import Path

SRC = Path(__file__).resolve().parents[4] / "src"

# A slice of a variable whose name says it holds a secret, e.g. token[:10],
# decrypted_value[-4:], auth_header[7:11], client_secret[:6].
SECRET_SLICE = re.compile(
    r"\b(\w*(?:token|api_?key|decrypted_value|secret|auth_header|password"
    r"|credential|bearer)\w*)\s*\[\s*-?\d*\s*:\s*-?\d*\s*\]",
    re.IGNORECASE,
)

# A "first N ... last N" preview of any variable: f"{value[:4]}...{value[-4:]}".
HEAD_TAIL_PREVIEW = re.compile(r"\{(\w+)\[:\s*\d+\s*\]\}\.\.\.\{\1\[\s*-\d+\s*:\s*\]\}")

# Stripping the "Bearer " prefix (7 characters) keeps the WHOLE token for use; it
# is not a fragment.
BEARER_PREFIX_STRIP = re.compile(r"\[\s*7\s*:\s*\]$")

# Fragments outside this change's ownership, recorded rather than silently
# allowed. Each entry must still match, so fixing one fails this test until the
# entry is removed — the list can only shrink.
KNOWN_OFFENDERS = {
    # DEBUG, non-Bearer branch only: 20 characters of the SDK auth header.
    ("utils/databricks_auth.py", "auth_header[:20]"),
    # INFO in the MLflow subprocess: "prefix" of the SPN auth header. Seven
    # characters is "Bearer " on the normal path, but token text otherwise.
    ("services/mlflow/mlflow_setup.py", "auth_header[:7]"),
}


def _findings():
    for path in sorted(SRC.rglob("*.py")):
        rel = path.relative_to(SRC).as_posix()
        source = path.read_text(encoding="utf-8")
        for pattern in (SECRET_SLICE, HEAD_TAIL_PREVIEW):
            for match in pattern.finditer(source):
                text = match.group(0)
                if pattern is SECRET_SLICE and BEARER_PREFIX_STRIP.search(text):
                    continue
                line = source.count("\n", 0, match.start()) + 1
                yield rel, text, line


def test_no_secret_is_sliced_anywhere_in_src():
    offenders = [
        f"src/{rel}:{line}: {text}"
        for rel, text, line in _findings()
        if (rel, text) not in KNOWN_OFFENDERS
    ]
    assert not offenders, (
        "secret fragments sliced into a log or response; log bool(secret) or the "
        "auth method instead:\n" + "\n".join(offenders)
    )


def test_known_offenders_are_still_present():
    """A fixed site must leave the allowlist, so the list only ever shrinks."""
    found = {(rel, text) for rel, text, _ in _findings()}
    stale = sorted(KNOWN_OFFENDERS - found)
    assert not stale, f"remove fixed entries from KNOWN_OFFENDERS: {stale}"


def test_the_patterns_catch_what_they_are_for():
    for sample in (
        'logger.info(f"t={token[:10]}")',
        "key_prefix = openai_api_key[:4]",
        '"token_preview": auth_token[:20]',
        "x = client_secret[-4:]",
    ):
        assert SECRET_SLICE.search(sample), sample
    assert HEAD_TAIL_PREVIEW.search('f"{value[:4]}...{value[-4:]}"')
    assert BEARER_PREFIX_STRIP.search("auth_header[7:]")
    assert not SECRET_SLICE.search("parts = path_parts[4:]")


# ---------------------------------------------------------------------------
# Whole configs and whole secrets interpolated into log calls (audit V4-2).
#
# The slice check above missed ``logger.info(f"... config: {tool_config}")``,
# which put the PowerBI connector's client_secret, password and access_token in
# crew.log at INFO (``tool_factory``), and ``logger.error(f"Tool config:
# {tool_config}")`` in the crew task adapter. A mapping that can carry
# credentials goes into a log only through ``mask_sensitive_fields`` (or
# ``mask_sensitive_headers``), or as its keys; a secret only as ``bool(...)``.
# ---------------------------------------------------------------------------

LOG_METHODS = {"debug", "info", "warning", "warn", "error", "exception", "critical"}

# The receiver of a log call: logger, crew_logger, self.logger, logging,
# logger_manager.crew ... but not ``catalog`` or ``dialog``.
LOG_RECEIVER = re.compile(r"(?:^|[._])log|logger", re.IGNORECASE)

# A bare name that holds a whole config / credential mapping, or a whole secret.
WHOLE_SECRET_CARRIER = re.compile(
    r"^(?:\w*_)?(?:config|configs|credentials?|creds|headers|kwargs|tool_args"
    r"|secrets?|token|access_token|password|client_secret|api_?key)$",
    re.IGNORECASE,
)

# Reviewed: these names hold no credentials. Each entry must still match, so
# the list can only shrink.
REVIEWED_NOT_SECRET = {
    # Model name: the str branch of the job-configuration summary.
    ("services/agent_builder/process_executor.py", "llm_config"),
    # Volume upload settings (catalog/schema/volume path, flags).
    ("api/databricks_knowledge_router.py", "volume_config"),
    ("services/knowledge/databricks_service.py", "volume_config"),
    ("services/agent_builder/task_adapter.py", "callback_config"),
    # Pagination cursor returned by Genie, not a credential.
    ("services/databricks/genie/service.py", "page_token"),
    # UC metric window spec.
    ("services/converters/formats/uc_metrics/uc_metrics_to_sql.py", "window_config"),
    # Guardrail settings (thresholds, field names, model name).
    ("services/execution/kernel/task_builder.py", "guardrail_config"),
    ("services/execution/kernel/task_builder.py", "llm_guardrail_config"),
    ("services/guardrails/guardrail_factory.py", "config"),
    ("services/guardrails/core/minimum_number_guardrail.py", "config"),
    ("services/guardrails/demo/company_count_guardrail.py", "config"),
    ("services/guardrails/demo/company_name_not_null_guardrail.py", "config"),
    ("services/guardrails/demo/data_processing_count_guardrail.py", "config"),
    ("services/guardrails/demo/data_processing_guardrail.py", "config"),
    ("services/guardrails/demo/empty_data_processing_guardrail.py", "config"),
    # Flow wiring: HITL gates, router routes, the crew memory flag.
    ("services/flow_builder/modules/flow_builder.py", "gate_config"),
    ("services/flow_builder/modules/flow_builder.py", "hitl_config"),
    ("services/flow_builder/modules/flow_methods.py", "gate_config"),
    ("services/flow_builder/modules/flow_methods.py", "crew_memory_from_config"),
    ("services/flow_builder/modules/flow_processors.py", "route_task_configs"),
    # Already passed through the tool's own credential scrub.
    (
        "services/tools/powerbi_field_parameters_calculation_groups_tool.py",
        "safe_config",
    ),
    ("services/tools/powerbi_report_references_tool.py", "safe_config"),
}


def _receiver(call: ast.Call) -> str:
    func = call.func
    if not isinstance(func, ast.Attribute) or func.attr not in LOG_METHODS:
        return ""
    return ast.unparse(func.value)


def _interpolated(call: ast.Call):
    """Every expression whose ``str()`` ends up in the log record."""
    for arg in call.args:
        if isinstance(arg, ast.JoinedStr):  # f"...{x}..."
            for part in arg.values:
                if isinstance(part, ast.FormattedValue):
                    yield part.value
        elif isinstance(arg, ast.BinOp) and isinstance(arg.op, ast.Mod):  # "%s" % x
            right = arg.right
            yield from right.elts if isinstance(right, ast.Tuple) else [right]
        elif (
            isinstance(arg, ast.Call)
            and isinstance(arg.func, ast.Attribute)
            and arg.func.attr == "format"
        ):  # "{}".format(x)
            yield from arg.args
            yield from (kw.value for kw in arg.keywords)
        else:  # logger.info("%s", x)
            yield arg
    for kw in call.keywords:
        if kw.arg == "extra":
            if isinstance(kw.value, ast.Dict):
                yield from kw.value.values
            else:
                yield kw.value


def _bare_name(node):
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return None


def _whole_config_logs(source: str):
    for node in ast.walk(ast.parse(source)):
        if not isinstance(node, ast.Call):
            continue
        receiver = _receiver(node)
        if not receiver or not LOG_RECEIVER.search(receiver):
            continue
        for expr in _interpolated(node):
            name = _bare_name(expr)
            if (
                name
                and WHOLE_SECRET_CARRIER.match(name)
                and not name.startswith(("has_", "is_"))
            ):
                yield name, node.lineno


def _whole_config_findings():
    for path in sorted(SRC.rglob("*.py")):
        rel = path.relative_to(SRC).as_posix()
        for name, line in _whole_config_logs(path.read_text(encoding="utf-8")):
            yield rel, name, line


def test_no_whole_config_or_secret_is_logged_anywhere_in_src():
    offenders = [
        f"src/{rel}:{line}: {name}"
        for rel, name, line in _whole_config_findings()
        if (rel, name) not in REVIEWED_NOT_SECRET
    ]
    assert not offenders, (
        "a whole config / credential mapping or secret interpolated into a log; "
        "wrap it in mask_sensitive_fields(...) or log its keys:\n"
        + "\n".join(offenders)
    )


def test_reviewed_not_secret_entries_are_still_present():
    found = {(rel, name) for rel, name, _ in _whole_config_findings()}
    stale = sorted(REVIEWED_NOT_SECRET - found)
    assert not stale, f"remove fixed entries from REVIEWED_NOT_SECRET: {stale}"


def test_the_whole_config_check_catches_what_it_is_for():
    caught = [
        'logger.info(f"Creating PowerBIConnectorTool with config: {tool_config}")',
        'logger.error(f"Tool config: {tool_config}")',
        'crew_logger.info("cfg %s", self._default_config)',
        'logger.debug("cfg %s" % (name, credentials))',
        'logger.warning("h={}".format(auth_headers))',
        'logger.info("x", extra={"cfg": obj.tool_configs})',
        'logger_manager.crew.info(f"t={access_token}")',
    ]
    for sample in caught:
        assert list(_whole_config_logs(sample)), sample
    for sample in (
        'logger.info(f"cfg: {mask_sensitive_fields(tool_config)}")',
        'logger.info(f"keys: {sorted(tool_config)}")',
        'logger.info(f"has token: {bool(token)}")',
        'logger.info(f"has creds: {has_sp_creds}")',
        'catalog.info(f"{config}")',
    ):
        assert not list(_whole_config_logs(sample)), sample
