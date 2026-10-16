"""An external caller's arguments, as the config of the flow run they start.

The flow service rebuilds a run's config from a whitelist and takes the
caller's values from ``inputs`` only; passed at the top level they were dropped
and the flow ran on empty state. ``session_id`` and ``user_message`` are read
from the top level instead.

What reaches ``inputs`` reaches flow state, so it is checked here, before the
run exists. A typed state (every conversational flow has one) refuses a key it
has no channel for at kickoff, which an external caller only learns after it
already holds a run id; a declared ``input_schema`` lets that be a 422 instead.
"""

from typing import Dict, List, Mapping, Optional, Tuple

from src.core.exceptions import UnprocessableEntityError

#: Keys that steer the RUN rather than feed its state, dropped from a caller's
#: arguments along with the flow's own RUN_METADATA_INPUTS. ``id`` names the
#: saved state a flow restores, so a caller's value would pick which thread
#: resumes.
_NOT_STATE = frozenset({"id"})

#: JSON Schema type names -> the Python types a JSON value of that type has.
_JSON_TYPES: Dict[str, Tuple[type, ...]] = {
    "string": (str,),
    "integer": (int,),
    "number": (int, float),
    "boolean": (bool,),
    "array": (list,),
    "object": (dict,),
    "null": (type(None),),
}


def flow_run_config(
    name: str, input_schema: object, arguments: Optional[Mapping[str, object]]
) -> Dict[str, object]:
    """The run config for ``arguments``; raises 422 if the schema refuses them.

    ``request`` is the free-text ask that the default tool schema and every A2A
    message carry: it is the turn's user line, and a state input only when the
    publication declares it.
    """
    from src.services.flow_builder.backend_flow import RUN_METADATA_INPUTS

    args = {
        key: value
        for key, value in (arguments or {}).items()
        if key not in _NOT_STATE and key not in RUN_METADATA_INPUTS
    }
    session_id = args.pop("session_id", None)
    user_message = args.pop("user_message", None)
    schema = input_schema if isinstance(input_schema, dict) else {}
    properties = schema.get("properties")
    declared = properties if isinstance(properties, dict) else {}
    if "request" in declared:
        request = args.get("request")
    else:
        request = args.pop("request", None)
    if declared:
        _check(name, schema, declared, args)
    return {
        "inputs": args,
        "session_id": session_id,
        "user_message": user_message or request,
    }


def _check(
    name: str,
    schema: Mapping[str, object],
    declared: Mapping[str, object],
    args: Mapping[str, object],
) -> None:
    """Refuse arguments the publication's input_schema does not allow.

    An undeclared key is refused unless the schema says ``additionalProperties``
    is allowed: JSON Schema permits them by default, but the flow state the
    schema describes does not.
    """
    problems: List[str] = []
    if not schema.get("additionalProperties"):
        unknown = sorted(set(args) - set(declared))
        if unknown:
            problems.append(f"unknown argument(s) {unknown}")
    required = schema.get("required")
    if isinstance(required, list):
        missing = sorted(str(key) for key in required if key not in args)
        if missing:
            problems.append(f"missing required argument(s) {missing}")
    for key, value in args.items():
        spec = declared.get(key)
        expected = spec.get("type") if isinstance(spec, dict) else None
        if expected is not None and not _has_type(value, expected):
            problems.append(f"{key!r} should be {expected}")
    if problems:
        raise UnprocessableEntityError(
            f"Invalid arguments for {name}: {'; '.join(problems)}. "
            f"It accepts: {sorted(declared)}."
        )


def _has_type(value: object, expected: object) -> bool:
    names = expected if isinstance(expected, list) else [expected]
    for type_name in names:
        python_types = _JSON_TYPES.get(str(type_name))
        if python_types is None:
            return True  # a type this check does not know: not refused here
        # bool is an int in Python, never a JSON number.
        if isinstance(value, bool) and bool not in python_types:
            continue
        if isinstance(value, python_types):
            return True
    return False
