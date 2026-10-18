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

import copy
from typing import Dict, FrozenSet, TypeVar

#: Launch-config keys that carry the caller's identity to the subprocess.
#: ``group_context`` holds ``access_token`` (and is not JSON in any case).
RUN_CREDENTIAL_KEYS: FrozenSet[str] = frozenset(
    {"user_token", "access_token", "group_context"}
)

#: Fields of an ``agents_yaml`` / ``tasks_yaml`` entry that can hold stored
#: credentials: tool overrides, the volume callback's config (its PAT), and
#: the task config, which nests a second copy of that callback config.
CREDENTIAL_FIELDS = ("tool_configs", "callback_config", "config")

_V = TypeVar("_V")


def strip_run_credentials(inputs: Dict[str, _V]) -> Dict[str, _V]:
    """``inputs`` without the credential keys, as a shallow copy.

    A value that is not a dict (a NULL or malformed column) comes back as it
    was, so this is safe on whatever a row holds.
    """
    if not isinstance(inputs, dict):
        return inputs
    return {k: v for k, v in inputs.items() if k not in RUN_CREDENTIAL_KEYS}


def mask_run_inputs(inputs: Dict[str, object]) -> Dict[str, object]:
    """``inputs`` as a run record may keep it, for every launch path.

    No run credentials, and every credential string masked in the agents' and
    tasks' :data:`CREDENTIAL_FIELDS` and in the user ``inputs``. Strings only,
    under the keys ``is_sensitive_key`` names: a count such as ``max_tokens``
    survives, as a number or a string, for a resume or a schedule that reads it
    back. A deep copy; the run itself keeps the unmasked config. Empty input
    comes back as it was.
    """
    if not inputs:
        return inputs
    # Imported here: cryptography, which the masking module pulls in, is not
    # needed for strip_run_credentials alone.
    from src.utils.sensitive_data_utils import mask_sensitive_strings

    masked = copy.deepcopy(strip_run_credentials(inputs))
    for section in ("agents_yaml", "tasks_yaml"):
        entries = masked.get(section)
        if not isinstance(entries, dict):
            continue
        for entry in entries.values():
            if not isinstance(entry, dict):
                continue
            for field in CREDENTIAL_FIELDS:
                if field in entry:
                    entry[field] = mask_sensitive_strings(entry[field])
    if isinstance(masked.get("inputs"), dict):
        masked["inputs"] = mask_sensitive_strings(masked["inputs"])
    return masked
