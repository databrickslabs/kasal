"""JSON Schema → Pydantic model.

The previous converter (inline in ``task_adapter``) handled one flat level:
``type: "object"`` collapsed to ``Dict[str, Any]`` and an array of objects to
``List[Any]``. That is enough to make ``Task(output_json=Model)`` construct and
nothing more — a schema for "a list of findings, each with a claim, a source and
a confidence" validated any list at all, so a gate built on it would pass on
``{"findings": [1, 2, 3]}``.

This module builds the whole tree: nested objects become real submodels,
``$ref``/``$defs`` resolve, enums become ``Literal``, and the range/length
keywords become Pydantic constraints. A schema-shaped gate is only as strong as
the model behind it.

Two deliberate behaviours:

* **``required`` decides optionality, and it decides it the right way round.**
  The old code read ``None if field_name in required_fields else ...``, which
  made required fields optional and optional fields mandatory — exactly
  inverted, so a model could omit the field the schema insisted on and be
  validated anyway.
* **Cycles degrade rather than recurse.** A ``$ref`` that reaches itself falls
  back to ``Dict[str, Any]`` for the inner occurrence; self-referential research
  schemas are rare and an unbounded build is worse than a loose leaf.
"""

import logging
import re
from typing import Any, Dict, List, Literal, Optional, Tuple, Type, Union

from pydantic import BaseModel, Field, create_model

logger = logging.getLogger(__name__)

#: JSON Schema scalar types → Python types.
_SCALARS: Dict[str, Any] = {
    "string": str,
    "integer": int,
    "number": float,
    "boolean": bool,
    "null": type(None),
}

#: Constraint keyword → the Pydantic ``Field`` kwarg carrying it, per family.
#: Split by family because ``minLength``/``minItems`` both map onto Pydantic v2's
#: single ``min_length`` but must only be applied to the type that accepts them.
_STRING_CONSTRAINTS = {"minLength": "min_length", "maxLength": "max_length"}
_ARRAY_CONSTRAINTS = {"minItems": "min_length", "maxItems": "max_length"}
_NUMBER_CONSTRAINTS = {
    "minimum": "ge",
    "maximum": "le",
    "exclusiveMinimum": "gt",
    "exclusiveMaximum": "lt",
}


def _sanitize(name: str) -> str:
    """A valid Python class name — ``create_model`` puts this in ``__name__``,
    which ends up in the prompt via ``model_json_schema()``."""
    cleaned = re.sub(r"\W|^(?=\d)", "_", str(name or "Model"))
    return cleaned[:1].upper() + cleaned[1:] if cleaned else "Model"


class _Builder:
    """One conversion. Holds the ``$defs`` table and the in-progress ref set."""

    def __init__(self, root: Dict[str, Any]):
        self.defs: Dict[str, Any] = {}
        for key in ("$defs", "definitions"):
            table = root.get(key)
            if isinstance(table, dict):
                self.defs.update(table)
        self._building: set = set()
        self._cache: Dict[str, Any] = {}

    # ---------------------------------------------------------------- refs

    def resolve(self, schema: Any) -> Tuple[Dict[str, Any], Optional[str]]:
        """Follow ``$ref`` to the schema it names. Returns (schema, ref-name)."""
        if not isinstance(schema, dict):
            return {}, None
        ref = schema.get("$ref")
        if not isinstance(ref, str):
            return schema, None
        name = ref.rsplit("/", 1)[-1]
        target = self.defs.get(name)
        if not isinstance(target, dict):
            logger.warning("unresolvable $ref %r; treating as untyped", ref)
            return {}, None
        return target, name

    # --------------------------------------------------------------- types

    def type_for(self, schema: Any, hint: str) -> Any:
        """The Python type for one JSON Schema node."""
        schema, ref_name = self.resolve(schema)
        if not schema:
            return Any

        # A closed value set is stronger than its base type — prefer it.
        enum = schema.get("enum")
        if isinstance(enum, list) and enum and all(_is_literal(v) for v in enum):
            return Literal[tuple(enum)]  # type: ignore[valid-type]

        # anyOf/oneOf: the common "nullable" spelling plus genuine unions.
        for key in ("anyOf", "oneOf"):
            options = schema.get(key)
            if isinstance(options, list) and options:
                return self._union_for(options, hint)

        json_type = schema.get("type")
        if isinstance(json_type, list):
            # ["string", "null"] — the other common nullable spelling.
            return self._union_for([{"type": t} for t in json_type], hint)

        if json_type == "object" or (json_type is None and "properties" in schema):
            return self.model_for(schema, ref_name or hint)
        if json_type == "array":
            return List[self.type_for(schema.get("items", {}), f"{hint}Item")]  # type: ignore[misc]
        return _SCALARS.get(json_type, Any)

    def _union_for(self, options: List[Any], hint: str) -> Any:
        members = [
            self.type_for(option, f"{hint}{index}")
            for index, option in enumerate(options)
        ]
        members = [m for m in members if m is not Any] or [Any]
        unique: List[Any] = []
        for member in members:
            if member not in unique:
                unique.append(member)
        return unique[0] if len(unique) == 1 else Union[tuple(unique)]  # type: ignore[return-value]

    # -------------------------------------------------------------- models

    def model_for(self, schema: Dict[str, Any], name: str) -> Any:
        """Build (or reuse) a submodel for an object node."""
        properties = schema.get("properties")
        if not isinstance(properties, dict) or not properties:
            # An object with no declared shape really is a free-form dict.
            return Dict[str, Any]

        cache_key = name
        if cache_key in self._cache:
            return self._cache[cache_key]
        if cache_key in self._building:
            # Cycle — see the module docstring.
            logger.info("recursive schema at %r; using an untyped mapping", name)
            return Dict[str, Any]

        self._building.add(cache_key)
        try:
            required = set(schema.get("required") or [])
            fields: Dict[str, Any] = {}
            for field_name, field_schema in properties.items():
                fields[field_name] = self.field_for(
                    field_schema, field_name in required, f"{name}_{field_name}"
                )
            model = create_model(
                _sanitize(name),
                **fields,
                __doc__=schema.get("description") or f"Model for {name}",
            )
        finally:
            self._building.discard(cache_key)

        self._cache[cache_key] = model
        return model

    def field_for(self, schema: Any, is_required: bool, hint: str) -> Tuple[Any, Any]:
        """``(annotation, default)`` for one property."""
        resolved, _ = self.resolve(schema)
        annotation = self.type_for(schema, hint)
        constraints = _constraints_for(resolved, annotation)
        description = resolved.get("description") if resolved else None

        if is_required:
            default: Any = (
                ...
                if not constraints and not description
                else Field(..., description=description, **constraints)
            )
            return (annotation, default)

        # Optional: nullable annotation and an explicit None, so a model that
        # legitimately omits the field validates instead of failing the gate.
        return (
            Optional[annotation],
            Field(default=None, description=description, **constraints),
        )


def _is_literal(value: Any) -> bool:
    return isinstance(value, (str, int, bool)) and not isinstance(value, float)


def _constraints_for(schema: Dict[str, Any], annotation: Any) -> Dict[str, Any]:
    """Constraint kwargs valid for this annotation.

    Applied by family: handing ``min_length`` to an int field makes Pydantic
    raise at class-creation time, which would turn a slightly-off schema into a
    hard crash of the whole crew build.
    """
    if not schema:
        return {}
    origin = getattr(annotation, "__origin__", None)
    table: Dict[str, str] = {}
    if annotation is str:
        table = _STRING_CONSTRAINTS
    elif origin is list:
        table = _ARRAY_CONSTRAINTS
    elif annotation in (int, float):
        table = _NUMBER_CONSTRAINTS

    constraints = {
        target: schema[source] for source, target in table.items() if source in schema
    }
    if annotation is str and isinstance(schema.get("pattern"), str):
        constraints["pattern"] = schema["pattern"]
    return constraints


def build_model_from_schema(
    name: str, schema: Dict[str, Any]
) -> Optional[Type[BaseModel]]:
    """Convert a JSON Schema object into a Pydantic model class.

    Returns ``None`` when the schema is unusable, so callers keep their existing
    "could not resolve, carry on without structured output" branch rather than
    failing the run.
    """
    if not isinstance(schema, dict):
        logger.error("schema %r is %s, not an object", name, type(schema).__name__)
        return None

    builder = _Builder(schema)
    try:
        model = builder.model_for(schema, name)
        if not (isinstance(model, type) and issubclass(model, BaseModel)):
            # A property-less ROOT schema still yields a (field-less) model —
            # nested property-less objects become free-form dicts, but the root
            # has to be a class or there is nothing to hand ``Task``.
            model = create_model(
                _sanitize(name),
                __doc__=schema.get("description") or f"Model for {name}",
            )
    except Exception:
        logger.exception("could not build a Pydantic model for schema %r", name)
        return None
    return model
