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

# A name that holds a whole config / credential mapping, or a whole secret. Any
# ``*_config``/``*_settings``/``*_payload``/``*_spec`` is presumed to be able to
# carry decrypted tool credentials (``crew_config`` did, audit V5-1).
WHOLE_SECRET_CARRIER = re.compile(
    r"^(?:\w*_)?(?:config|configs|settings|payload|spec|credentials?|creds|headers"
    r"|kwargs|secrets?|token|access_token|password|client_secret|api_?key)$"
    r"|^(?:agents_yaml|tasks_yaml|tool_args|tool_arguments|tool_input|tool_params)$",
    re.IGNORECASE,
)

# One field of a mapping, ``cfg["token"]`` / ``cfg.get("client_secret")``, is a
# secret when its key says so (the same substrings mask_sensitive_fields uses).
# ``*_tokens`` are token counts, not tokens.
SECRET_KEY = re.compile(
    r"secret|password|token(?!s)|api_?key|credential|private_?key|bearer",
    re.IGNORECASE,
)

# Calls whose result reveals nothing secret about their argument: the masking
# helpers (mask_sensitive_fields, mask_sensitive_headers, safe_log_tool_configs,
# the SSE router's _loggable_headers allowlist) and key/size/type/truth views.
SAFE_WRAPPERS = {"sorted", "keys", "len", "bool", "type", "isinstance", "hasattr"}
MASKING_CALL = re.compile(r"mask|redact|safe_log|loggable", re.IGNORECASE)

# Boolean flags named after what they gate (include_payload, has_token).
FLAG_PREFIXES = ("has_", "is_", "include_", "use_")

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
    # Request bodies carrying the user's question / messages; the credential
    # travels in a separate headers dict that is not logged.
    ("repositories/agentbricks_repository.py", "payload"),
    ("services/tools/agentbricks_tool.py", "payload"),
    ("services/tools/genie_tool.py", "payload"),
    ("services/tools/perplexity_tool.py", "payload"),
    # Server RESPONSE headers, not the request's Authorization header.
    ("repositories/databricks_volume_repository.py", "headers"),
    ("services/tools/databricks_jobs_tool.py", "headers"),
    # Reasoning effort ({"reasoning_effort": "low"}).
    ("services/agent_builder/crew_preparation.py", "['reasoning_config']"),
    # The injected security preamble text, and whitelisted agent tuning
    # params (_ADDITIONAL_AGENT_PARAMS) copied from the agent spec.
    ("services/execution/kernel/agent_builder.py", "agent_kwargs"),
    ("services/execution/kernel/agent_builder.py", "spec"),
}

# NOT reviewed safe: real whole-config logs left open because the file belongs
# to parallel work. Recorded so the guard stays green without hiding them; fix
# (log keys/counts) and delete the entry. Shrink-only, like the list above.
KNOWN_WHOLE_CONFIG_LEAKS = {
    # INFO: the schedule's agents/tasks YAML, which can carry tool_configs.
    ("services/scheduling/scheduler.py", "agents_yaml"),
    ("services/scheduling/scheduler.py", "tasks_yaml"),
}
ALLOWED = REVIEWED_NOT_SECRET | KNOWN_WHOLE_CONFIG_LEAKS


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


def _call_name(call: ast.Call) -> str:
    func = call.func
    if isinstance(func, ast.Name):
        return func.id
    return func.attr if isinstance(func, ast.Attribute) else ""


def _field_key(node):
    """The constant key of ``x["k"]`` or ``x.get("k", ...)``, else None."""
    if isinstance(node, ast.Subscript):
        key = node.slice
    elif isinstance(node, ast.Call) and _call_name(node) == "get" and node.args:
        key = node.args[0]
    elif isinstance(node, ast.Call) and _call_name(node) == "getattr":
        key = node.args[1] if len(node.args) > 1 else None
    else:
        return None
    if isinstance(key, ast.Constant) and isinstance(key.value, str):
        return key.value
    return None


def _carriers(node):
    """Names of secret carriers whose contents reach ``str(node)``.

    Reasons about what is logged, not just which bare name: ``json.dumps(cfg)``,
    ``repr(cfg)``, ``str(cfg)``, ``cfg.items()`` and ``{**cfg}`` all log ``cfg``;
    ``mask_sensitive_fields(cfg)``, ``sorted(cfg)``, ``len(cfg)`` do not; one
    field (``cfg["model"]``, ``cfg.get("name")``, ``cfg.model``) is judged by
    its key.
    """
    if isinstance(node, ast.Name):
        if WHOLE_SECRET_CARRIER.match(node.id):
            yield node.id
        return
    if isinstance(node, ast.Attribute):
        if WHOLE_SECRET_CARRIER.match(node.attr):
            yield node.attr
        return
    key = _field_key(node)
    if key is not None:
        if SECRET_KEY.search(key) or WHOLE_SECRET_CARRIER.match(key):
            yield f"[{key!r}]"
        return
    if isinstance(node, ast.Compare):  # cfg is None, "k" in cfg: a bool
        return
    if isinstance(node, ast.IfExp):  # the test only picks a branch
        yield from _carriers(node.body)
        yield from _carriers(node.orelse)
        return
    if isinstance(node, ast.Call):
        name = _call_name(node)
        if name in SAFE_WRAPPERS or MASKING_CALL.search(name):
            return
        if isinstance(node.func, ast.Attribute):  # cfg.items(), json.dumps(...)
            yield from _carriers(node.func.value)
        for arg in (*node.args, *(kw.value for kw in node.keywords)):
            yield from _carriers(arg)
        return
    for child in ast.iter_child_nodes(node):
        yield from _carriers(child)


def _whole_config_logs(source: str):
    for node in ast.walk(ast.parse(source)):
        if not isinstance(node, ast.Call):
            continue
        receiver = _receiver(node)
        if not receiver or not LOG_RECEIVER.search(receiver):
            continue
        for expr in _interpolated(node):
            for name in _carriers(expr):
                if not name.startswith(FLAG_PREFIXES):
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
        if (rel, name) not in ALLOWED
    ]
    assert not offenders, (
        "a whole config / credential mapping or secret interpolated into a log; "
        "wrap it in mask_sensitive_fields(...) or log its keys:\n"
        + "\n".join(offenders)
    )


def test_reviewed_not_secret_entries_are_still_present():
    found = {(rel, name) for rel, name, _ in _whole_config_findings()}
    stale = sorted(ALLOWED - found)
    assert not stale, f"remove fixed entries from the allowlists: {stale}"


def test_the_whole_config_check_catches_what_it_is_for():
    caught = [
        'logger.info(f"Creating PowerBIConnectorTool with config: {tool_config}")',
        'logger.error(f"Tool config: {tool_config}")',
        'crew_logger.info("cfg %s", self._default_config)',
        'logger.debug("cfg %s" % (name, credentials))',
        'logger.warning("h={}".format(auth_headers))',
        'logger.info("x", extra={"cfg": obj.tool_configs})',
        'logger_manager.crew.info(f"t={access_token}")',
        # V5-1: wrapped, dumped or indexed configs.
        'subprocess_logger.info(f"Full Config: {json.dumps(crew_config, indent=2)}")',
        'logger.error(f"value: {repr(crew_config)[:500]}")',
        'logger.info("cfg %s", str(run_settings))',
        'logger.info(f"{dict(request_payload)}")',
        'logger.info(f"{list(cfg_spec.items())}")',
        'logger.info(f"{agents_yaml}")',
        'logger.info(f"{schedule.tasks_yaml}")',
        'logger.info(f"args: {tool_args}")',
        "logger.info(f\"s={cfg.get('client_secret')}\")",
        "logger.info(f\"t={cfg['access_token']}\")",
        "logger.info(f\"{db_task['tool_configs']}\")",
        "logger.info(f\"{getattr(obj, 'api_key', None)}\")",
        'logger.info(f"{mask(x) if y else tool_config}")',
    ]
    for sample in caught:
        assert list(_whole_config_logs(sample)), sample
    for sample in (
        'logger.info(f"cfg: {mask_sensitive_fields(tool_config)}")',
        'logger.info(f"keys: {sorted(tool_config)}")',
        'logger.info(f"has token: {bool(token)}")',
        'logger.info(f"has creds: {has_sp_creds}")',
        'catalog.info(f"{config}")',
        'logger.info(f"keys: {sorted(crew_config)}")',
        "logger.info(f\"n: {len(crew_config.get('agents', []))}\")",
        "logger.info(f\"name: {crew_config.get('run_name', 'x')}\")",
        "logger.info(f\"model: {cfg['model']} / {config.model}\")",
        "logger.info(f\"tokens: {usage['total_tokens']}\")",
        "logger.info(f\"set: {'yes' if client_secret else 'no'}\")",
        'logger.info(f"present: {tool_config is not None}")',
        'logger.info(f"{safe_log_tool_configs(task.tool_configs)}")',
        "logger.info(f\"{getattr(config, 'group_id', None)}\")",
        'logger.info(f"flag: {include_payload}")',
    ):
        assert not list(_whole_config_logs(sample)), sample
