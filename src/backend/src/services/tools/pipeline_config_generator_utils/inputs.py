"""Input resolution and validation for one Pipeline Config Generator run.

A value the caller passes wins over the one injected at construction, but a
blank placeholder (``""``, ``[]``, ``{}``) never shadows the injected value: a
capable agent often passes its own empty placeholder for a field it lacks.
"""

from collections.abc import Mapping
from dataclasses import dataclass

_REDEPLOY_HINT = (
    " If these worked before a redeploy, the stored value may have been "
    "cleared because the encryption key changed — please re-enter it."
)


@dataclass(frozen=True)
class ConfigGenInputs:
    """Resolved run inputs. ``None`` means nothing usable was supplied."""

    workspace_id: str | None
    dataset_id: str | None
    report_id: str | None
    tenant_id: str | None
    client_id: str | None
    client_secret: str | None
    username: str | None
    password: str | None
    access_token: str | None
    auth_method: str | None
    admin_client_id: str | None
    admin_client_secret: str | None
    admin_username: str | None
    admin_password: str | None
    catalog: str
    schema: str
    warehouse_id: str | None
    databricks_host: str | None


def _pick(
    kwargs: Mapping[str, object], defaults: Mapping[str, object], key: str
) -> object:
    value = kwargs.get(key)
    if value is None or value == "" or value == [] or value == {}:
        return defaults.get(key)
    return value


def _text(value: object) -> str | None:
    if value is None or value == [] or value == {}:
        return None
    return value if isinstance(value, str) else str(value)


def resolve_inputs(
    kwargs: Mapping[str, object], defaults: Mapping[str, object]
) -> ConfigGenInputs:
    """Merge call kwargs over the constructor-injected defaults."""

    def text(key: str) -> str | None:
        return _text(_pick(kwargs, defaults, key))

    return ConfigGenInputs(
        workspace_id=text("workspace_id"),
        dataset_id=text("dataset_id"),
        report_id=text("report_id"),
        tenant_id=text("tenant_id"),
        client_id=text("client_id"),
        client_secret=text("client_secret"),
        username=text("username"),
        password=text("password"),
        access_token=text("access_token"),
        auth_method=text("auth_method"),
        admin_client_id=text("admin_client_id"),
        admin_client_secret=text("admin_client_secret"),
        admin_username=text("admin_username"),
        admin_password=text("admin_password"),
        catalog=text("catalog") or "main",
        schema=text("schema_name") or "default",
        warehouse_id=text("warehouse_id"),
        databricks_host=text("databricks_host"),
    )


def _creds_ok(
    cid: str | None, secret: str | None, user: str | None, pw: str | None
) -> bool:
    """A credential set is a Service Principal, a Service Account, or neither."""
    has_sp = bool(cid and secret)
    has_sa = bool(cid and user and pw)
    return has_sp or has_sa


def input_error(inputs: ConfigGenInputs) -> str | None:
    """The first validation message for ``inputs``, or ``None`` when runnable.

    After this returns ``None`` the workspace and dataset ids are non-empty.
    """
    if not inputs.workspace_id or not inputs.dataset_id:
        return "workspace_id and dataset_id are required."
    if not inputs.access_token and not inputs.tenant_id:
        return "tenant_id is required (unless a pre-obtained access_token is supplied)."
    if not inputs.access_token and not _creds_ok(
        inputs.client_id, inputs.client_secret, inputs.username, inputs.password
    ):
        return (
            "Non-admin credentials required: a Service Principal "
            "(client_id + client_secret), a Service Account "
            "(client_id + username + password), or a pre-obtained access_token."
            + _REDEPLOY_HINT
        )
    if not _creds_ok(
        inputs.admin_client_id,
        inputs.admin_client_secret,
        inputs.admin_username,
        inputs.admin_password,
    ):
        return (
            "Admin credentials required: either a Service Principal "
            "(admin_client_id + admin_client_secret) or a Service Account "
            "(admin_client_id + admin_username + admin_password)." + _REDEPLOY_HINT
        )
    return None
