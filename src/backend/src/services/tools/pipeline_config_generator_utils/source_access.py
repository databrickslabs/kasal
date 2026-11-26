"""Tell a rejected Power BI login from a model whose table sources are simply absent.

The table sources (Power Query per table) come from the Admin Scanner, the Fabric
definition or a service-principal retry. Each failure is only a warning, because
one of the three usually succeeds. When all three fail on a rejected credential
the run used to finish with measures and no table sources, and the metric view
generator then built views only for the tables that hold measures: a result that
looks complete and is not.
"""

from collections.abc import Sequence

# Entra ID codes for a client secret that is wrong (7000215) or expired (7000222),
# and the OAuth error name both are returned under.
_REJECTED_CREDENTIAL_MARKERS = ("AADSTS7000215", "AADSTS7000222", "invalid_client")
_DETAIL_CHARS = 300


def rejected_login_error(admin_tables: object, warnings: Sequence[str]) -> str | None:
    """The error to return when no table source was read because a login was rejected.

    ``None`` when at least one table source was read (a failed tier next to a
    successful one stays a warning), or when the sources are missing for another
    reason than a rejected credential.
    """
    if admin_tables:
        return None
    rejected = [
        warning
        for warning in warnings
        if any(marker in warning for marker in _REJECTED_CREDENTIAL_MARKERS)
    ]
    if not rejected:
        return None
    return (
        "Power BI rejected the configured client secret, so no table source "
        "(Power Query) could be read. The secret is wrong, has expired or was "
        "replaced: enter a valid one in this task's configuration and run again. "
        "Nothing was generated, because without table sources the metric views "
        "would cover only the tables that hold measures. "
        f"Detail: {rejected[0][:_DETAIL_CHARS]}"
    )
