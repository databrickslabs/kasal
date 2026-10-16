"""What a run's stored inputs may carry.

``executionhistory.inputs`` is the record of what a run was asked to do. Any
member of the run's workspace can read it back through the run APIs, and it
outlives the run in the database.

A run's launch config also carries the caller's credentials, because the
subprocess has to act as that caller. Those travel WITH the run; they are not
part of its record. The subprocess gets them in its spawn arguments, and a
resume takes a fresh one from the request that resumes it, so nothing ever
needs to read one back from the row.

:func:`strip_run_credentials` is the one place that names those keys. It is
applied where the row is written and again where it is read, so rows stored
before it existed are clean on the way out too.

Only these exact top-level keys go. Masking every key that merely contains
"token" would also hit ``max_tokens`` and ``thinking_budget_tokens``, and a
resume that read them back would hand a placeholder string to the model.

Kept free of heavy imports: the flow subprocess imports this.
"""

from typing import Dict, FrozenSet, TypeVar

#: Launch-config keys that carry the caller's identity to the subprocess.
#: ``group_context`` holds ``access_token`` (and is not JSON in any case).
RUN_CREDENTIAL_KEYS: FrozenSet[str] = frozenset(
    {"user_token", "access_token", "group_context"}
)

_V = TypeVar("_V")


def strip_run_credentials(inputs: Dict[str, _V]) -> Dict[str, _V]:
    """``inputs`` without the credential keys, as a shallow copy.

    A value that is not a dict (a NULL or malformed column) comes back as it
    was, so this is safe on whatever a row holds.
    """
    if not isinstance(inputs, dict):
        return inputs
    return {k: v for k, v in inputs.items() if k not in RUN_CREDENTIAL_KEYS}
