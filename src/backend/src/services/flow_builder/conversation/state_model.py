"""Compiling a declared flow state schema into a real state class.

``Flow`` has supported a typed state since it was written — ``Flow(Generic[T])``
with ``initial_state``, and ``_build_initial_state`` branching on class /
instance / dict. The builder just never used it: ``state_config.get("model")``
was read once and passed nowhere, so every flow ran on an untyped dict.

Untyped means unchecked. A dict takes any key, so a misspelled input lands in
state, the condition that reads the correct name sees nothing, and the flow
branches as though the value were never supplied. That is the failure this makes
detectable — ``Flow._merge_inputs`` raises on a field a TYPED state has no room
for, and until now that raise had no way to fire.

Why the generated class is dict-compatible
==========================================

Every condition ever written for a flow uses dict access::

    state.get("has_results", "") == True        # what the UI generates
    state["region"] == "DACH"

On a plain pydantic model both RAISE — ``'FlowState' object has no attribute
'get'``, ``not subscriptable`` — so turning on typed state would have broken
every existing flow, and (since conditions now fail loudly rather than
returning False) broken them noisily.

So the generated model answers to all three forms: ``get``, ``[]`` and
attribute. Typing a flow's state becomes additive — existing conditions keep
working, and ``state.region`` starts working too.
"""

import logging
from typing import Any, ClassVar, Dict, List, Optional, Type

from pydantic import BaseModel, ConfigDict, Field, create_model

from src.services.flow_builder.conversation.channels import (
    REPLACE,
    apply_reducer,
    normalize_reducer,
)

logger = logging.getLogger(__name__)

#: Names on the dict surface below. A declared field may still use one — the
#: author's data wins — but it shadows the method, so ``state.keys()`` in a
#: condition would then fail. Warned about explicitly rather than left to
#: pydantic's generic shadowing warning, which nobody reading a flow log would
#: connect to their own schema.
_DICT_SURFACE = frozenset(
    {"get", "keys", "items", "values", "update", "model_dump", "model_fields"}
)

#: The shape a reducer only makes sense on, when the schema declares no type.
_IMPLIED_TYPE: Dict[str, str] = {
    "append": "array",
    "add": "integer",
    "merge": "object",
}

#: JSON Schema type -> (python type, default). The default matters: a flow's
#: state is constructed with NO arguments at kickoff, so every field must have
#: one or the model cannot be instantiated at all.
_TYPES: Dict[str, tuple] = {
    "string": (str, ""),
    "number": (float, 0.0),
    "integer": (int, 0),
    "boolean": (bool, False),
    "array": (List[Any], None),  # default_factory, see below
    "object": (Dict[str, Any], None),
}


class DictLikeState(BaseModel):
    """Base for a generated flow state: reads like a dict, is really a model.

    The dict surface is not a nicety. Router conditions are authored as
    ``state.get("x")`` and ``state["x"]``, and a state that answers only to
    attributes would break every one of them the moment a flow declared a
    schema.
    """

    # Extra keys are WRITABLE but never DECLARED, and the difference is the
    # whole design. The flow writes to its own state at runtime — the builder
    # stores ``state["previous_output"]`` between methods, and a state operation
    # writes whatever variable its node names — none of which any author would
    # think to declare. Rejecting those would make a typed state unusable.
    #
    # What is still rejected is the case this exists for: an INPUT naming a
    # field the state does not have. ``_merge_inputs`` and ``update`` below
    # check ``hasattr`` explicitly, and an extra that has not been set yet fails
    # that check — so a misspelled input still raises at kickoff while the
    # flow's own bookkeeping passes through.
    model_config = ConfigDict(extra="allow")

    #: Channel name -> reducer name. Set by :func:`build_state_model` from the
    #: declared schema; empty here so a hand-written state still behaves.
    __reducers__: ClassVar[Dict[str, str]] = {}

    def reducer_for(self, key: str) -> str:
        return type(self).__reducers__.get(key, REPLACE)

    def merge(self, updates: Dict[str, Any]) -> None:
        """Apply a batch of writes THROUGH each channel's reducer.

        The difference between this and :meth:`update` is the whole point of
        reducers: ``update`` seeds a value, ``merge`` combines one with what is
        already there. A turn's new message merged into ``messages`` appends;
        the same write through ``update`` would replace the conversation with
        its newest line.

        Unknown keys raise, for the reason ``_merge_inputs`` does: a write the
        state has no channel for is silently lost, and a flow that branches on
        the missing value looks like it simply chose the other branch.
        """
        unknown = [key for key in updates if not hasattr(self, key)]
        if unknown:
            raise ValueError(
                f"Flow state has no channel(s) {sorted(unknown)}. "
                f"This state declares: {sorted(type(self).model_fields)}."
            )
        for key, value in updates.items():
            reducer = self.reducer_for(key)
            merged = apply_reducer(reducer, getattr(self, key, None), value)
            setattr(self, key, merged)

    def get(self, key: str, default: Any = None) -> Any:
        return getattr(self, key, default)

    def __getitem__(self, key: str) -> Any:
        try:
            return getattr(self, key)
        except AttributeError as exc:
            raise KeyError(key) from exc

    def __setitem__(self, key: str, value: Any) -> None:
        setattr(self, key, value)

    def __contains__(self, key: object) -> bool:
        return isinstance(key, str) and hasattr(self, key)

    def keys(self):  # noqa: D102 — dict surface
        return self.model_dump().keys()

    def items(self):  # noqa: D102 — dict surface
        return self.model_dump().items()

    def values(self):  # noqa: D102 — dict surface
        return self.model_dump().values()

    def update(self, other: Dict[str, Any]) -> None:
        """Apply initial values, the way the builder's ``__init__`` does.

        ``create_init_method`` calls ``self.state.update(initial_values)`` — a
        dict method. Providing it here is what lets that code stay as it is
        instead of branching on the state's kind.

        Unknown keys RAISE, for the same reason ``_merge_inputs`` does: an
        initial value the state has no field for is a typo in the flow's own
        configuration, and silently dropping it produces a flow that starts in a
        state nobody authored.
        """
        unknown = [key for key in other if not hasattr(self, key)]
        if unknown:
            raise ValueError(
                f"Flow state has no field(s) {sorted(unknown)} for initial "
                f"values. This state declares: {sorted(type(self).model_fields)}."
            )
        for key, value in other.items():
            setattr(self, key, value)


def build_state_model(
    schema: Any,
    name: str = "FlowState",
    base: Type[DictLikeState] = DictLikeState,
) -> Optional[Type[DictLikeState]]:
    """A JSON Schema -> a flow state class, or None when there is nothing to build.

    The schema shape is the one the rest of the product already uses for
    declared inputs (``publications.input_schema``): ``{"type": "object",
    "properties": {...}}``, with one addition — a property may name a
    ``reducer`` saying how writes to that channel merge. One schema concept,
    not two.

    ``base`` lets a conversational flow start from ``ConversationState`` (which
    brings ``messages`` and the turn fields) instead of a bare state. A base
    with channels of its own is the only case where this returns a model for an
    EMPTY schema: the base's channels are already worth having.

    Returns None rather than raising for anything unusable — a malformed schema
    must leave the flow running on a dict, exactly as before, not fail its
    kickoff.
    """
    properties = schema.get("properties") if isinstance(schema, dict) else None
    if not isinstance(properties, dict):
        properties = {}
    if not properties and base is DictLikeState:
        return None

    fields: Dict[str, tuple] = {}
    reducers: Dict[str, str] = {}
    for field_name, spec in properties.items():
        field_name = str(field_name)
        if not field_name.isidentifier() or field_name.startswith("_"):
            # A field the expression evaluator could never name.
            logger.warning(
                "[flow-state] skipping field %r: not a usable identifier",
                field_name,
            )
            continue
        if field_name in _DICT_SURFACE:
            logger.warning(
                "[flow-state] field %r shadows the state's dict surface; "
                "state.%s(...) will not be callable in a condition",
                field_name,
                field_name,
            )
        spec_dict = spec if isinstance(spec, dict) else {}
        declared_reducer = spec_dict.get("reducer")
        reducer = normalize_reducer(declared_reducer)
        if declared_reducer:
            # Recorded whenever the schema NAMES one, including `replace`.
            # Recording only non-defaults would silently ignore a flow that
            # declares `replace` to override an inherited `append` — the
            # override would parse, and do nothing.
            reducers[field_name] = reducer

        # Declaring a reducer IS declaring the shape: `append` only means
        # anything on a list, `add` on a number, `merge` on an object. Inferring
        # it saves a schema whose two halves can disagree — and, more
        # practically, an `add` channel left untyped defaults to None, so the
        # first `state.count + 1` in a node dies on NoneType.
        declared = spec_dict.get("type") or _IMPLIED_TYPE.get(reducer)

        python_type, default = _TYPES.get(str(declared), (Any, None))
        if declared == "array":
            fields[field_name] = (List[Any], Field(default_factory=list))
        elif declared == "object":
            fields[field_name] = (Dict[str, Any], Field(default_factory=dict))
        else:
            fields[field_name] = (python_type, default)

    if not fields and base is DictLikeState:
        return None

    # The runtime carries an `id` on every state — it is the checkpoint handle
    # (`_restore_state`, and `{"id": ...}` at kickoff). A declared schema that
    # forgets it would make the flow unresumable.
    fields.setdefault("id", (str, ""))

    # A field the base already declares must not be redeclared with a weaker
    # type: `messages` arriving as an untyped property would otherwise turn
    # ConversationState's list channel into `Any` and lose its default.
    for inherited in base.model_fields:
        if inherited in fields and inherited != "id":
            fields.pop(inherited)

    try:
        model = create_model(name, __base__=base, **fields)
    except Exception as exc:  # noqa: BLE001 — a bad schema must not break kickoff
        logger.warning("[flow-state] could not build state model: %s", exc)
        return None

    # Inherited channels keep their reducers; declared ones win on a clash, so
    # a flow can make its own decision about a channel the base also names.
    model.__reducers__ = {**base.__reducers__, **reducers}
    return model
