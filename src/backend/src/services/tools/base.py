"""BaseTool — the engine's widest compatibility surface (31 kasal files subclass it).

Authored module; its surface is validated against the kasal_engine datamodel
by generator/validate.py. Behavior follows crewAI 1.15.5 (code2db evidence,
repo_id 1) with deliberate simplifications:

- no checkpoint deserialization registry lookups by dotted path (kasal does
  not use checkpoints; ``tool_type`` still reports the dotted path),
- ``SerializableCallable`` is a plain callable alias (no dotted-path
  resolution of serialized callbacks),
- ``description`` is never mutated: the LLM-facing composite lives in
  ``formatted_description`` (1.15.5 behavior; 1.14.5 rewrote description).
"""

import asyncio
import json
import re
import threading
import unicodedata
from abc import ABC, abstractmethod
from collections.abc import Callable
from inspect import Parameter, signature
from typing import Any

from pydantic import BaseModel
from pydantic import BaseModel as PydanticBaseModel
from pydantic import (
    ConfigDict,
    Field,
    GetCoreSchemaHandler,
    PrivateAttr,
    computed_field,
    create_model,
    field_serializer,
    field_validator,
)
from pydantic_core import CoreSchema, core_schema

SerializableCallable = Callable[..., Any]

_TOOL_TYPE_REGISTRY: dict[str, type] = {}

# Sentinel set after BaseTool is defined so __get_pydantic_core_schema__ can
# distinguish the base class from subclasses.
_BASE_TOOL_CLS: type | None = None

_MAX_TOOL_NAME_LENGTH = 64

_COMPOSITE_PREFIX_RE = re.compile(
    r"\ATool Name: .*?\nTool Arguments: .*?\nTool Description: ", re.DOTALL
)


def sanitize_tool_name(name: str, max_length: int = _MAX_TOOL_NAME_LENGTH) -> str:
    """Normalize a tool name for LLM provider compatibility."""
    normalized = unicodedata.normalize("NFKC", name)
    normalized = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", normalized)
    normalized = re.sub(r"[^a-zA-Z0-9_-]", "_", normalized).lower()
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    return normalized[:max_length]


def strip_composite_description_prefix(description: str) -> str:
    """Undo a previously composed description block (idempotency guard)."""
    match = _COMPOSITE_PREFIX_RE.match(description)
    if match:
        return description[match.end() :]
    return description


def format_description_for_llm(
    name: str, args_schema: type[BaseModel] | None, description: str
) -> str:
    """Compose the LLM-facing tool description; never mutates the tool."""
    description = strip_composite_description_prefix(description)
    if args_schema is not None:
        args_json = json.dumps(args_schema.model_json_schema(), indent=2)
    else:
        args_json = "{}"
    return (
        f"Tool Name: {sanitize_tool_name(name)}\n"
        f"Tool Arguments: {args_json}\n"
        f"Tool Description: {description}"
    )


def build_schema_hint(args_schema: type[BaseModel]) -> str:
    try:
        schema = args_schema.model_json_schema()
    except Exception:
        return ""
    props = schema.get("properties", {})
    if not props:
        return ""
    required = set(schema.get("required", []))
    parts = [
        f"{name}: {spec.get('type', 'any')}"
        + ("" if name in required else " (optional)")
        for name, spec in props.items()
    ]
    return f" Expected arguments: {', '.join(parts)}."


def _default_cache_function(_args: Any = None, _result: Any = None) -> bool:
    return True


def _schema_from_callable(
    func: Callable[..., Any], model_name: str
) -> type[PydanticBaseModel]:
    fields: dict[str, Any] = {}
    for param_name, param in signature(func).parameters.items():
        if param_name in ("self", "return"):
            continue
        if param.kind in (Parameter.VAR_POSITIONAL, Parameter.VAR_KEYWORD):
            continue
        annotation = (
            param.annotation if param.annotation is not Parameter.empty else Any
        )
        default = ... if param.default is Parameter.empty else param.default
        fields[param_name] = (annotation, default)
    return create_model(model_name, **fields)


def _schema_from_json_schema(
    spec: dict[str, Any], model_name: str
) -> type[PydanticBaseModel]:
    """Build a pydantic model from a plain JSON-schema dict (primitive types)."""
    type_map = {
        "string": str,
        "integer": int,
        "number": float,
        "boolean": bool,
        "array": list,
        "object": dict,
    }
    required = set(spec.get("required", []))
    fields: dict[str, Any] = {}
    for field_name, field_spec in (spec.get("properties") or {}).items():
        annotation = type_map.get(field_spec.get("type", ""), Any)
        default = ... if field_name in required else field_spec.get("default")
        fields[field_name] = (annotation, default)
    return create_model(spec.get("title") or model_name, **fields)


class EnvVar(BaseModel):
    name: str
    description: str
    required: bool = True
    default: str | None = None


class ToolUsageLimitExceededError(Exception):
    """Raised when a tool has reached its maximum usage limit."""


def _format_tool_output_for_agent(tool: Any, raw_result: Any) -> str:
    original_tool = getattr(tool, "_original_tool", None)
    if original_tool is not None:
        return original_tool.format_output_for_agent(raw_result)

    result_schema = getattr(tool, "result_schema", None)
    if not (isinstance(result_schema, type) and issubclass(result_schema, BaseModel)):
        return raw_result if isinstance(raw_result, str) else str(raw_result)
    try:
        validation_input = raw_result
        if isinstance(raw_result, BaseModel) and not isinstance(
            raw_result, result_schema
        ):
            validation_input = raw_result.model_dump()
        return result_schema.model_validate(validation_input).model_dump_json()
    except Exception:
        return str(raw_result)


class CrewStructuredTool(BaseModel):
    """Structured-tool adapter around a plain callable."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    name: str = Field(default="")
    description: str = Field(default="")
    args_schema: type[PydanticBaseModel] | None = Field(default=None)
    result_schema: type[PydanticBaseModel] | None = Field(default=None)
    func: Any = Field(default=None, exclude=True)
    result_as_answer: bool = Field(default=False)
    max_usage_count: int | None = Field(default=None)
    current_usage_count: int = Field(default=0)
    cache_function: Any = Field(default=None, exclude=True)
    _original_tool: Any = PrivateAttr(default=None)

    def invoke(self, input: dict[str, Any] | str, **kwargs: Any) -> Any:
        parsed = json.loads(input) if isinstance(input, str) else dict(input)
        if self.args_schema is not None and self.args_schema.model_fields:
            parsed = self.args_schema.model_validate(parsed).model_dump()
        result = self.func(**parsed)
        if asyncio.iscoroutine(result):
            result = asyncio.run(result)
        return result

    async def ainvoke(self, input: dict[str, Any] | str, **kwargs: Any) -> Any:
        parsed = json.loads(input) if isinstance(input, str) else dict(input)
        if self.args_schema is not None and self.args_schema.model_fields:
            parsed = self.args_schema.model_validate(parsed).model_dump()
        result = self.func(**parsed)
        if asyncio.iscoroutine(result):
            result = await result
        return result

    @property
    def args(self) -> dict[str, Any]:
        return (
            self.args_schema.model_json_schema().get("properties", {})
            if self.args_schema
            else {}
        )


class BaseTool(BaseModel, ABC):
    class _ArgsSchemaPlaceholder(PydanticBaseModel):
        pass

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        _TOOL_TYPE_REGISTRY[f"{cls.__module__}.{cls.__qualname__}"] = cls

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        default_schema = handler(source_type)
        if cls is not _BASE_TOOL_CLS:
            return default_schema

        def _validate_tool(value: Any, nxt: Any) -> Any:
            if isinstance(value, _BASE_TOOL_CLS):
                return value
            if isinstance(value, dict) and "tool_type" in value:
                tool_cls = _TOOL_TYPE_REGISTRY.get(value["tool_type"])
                if tool_cls is not None:
                    data = {k: v for k, v in value.items() if k != "tool_type"}
                    return tool_cls.model_validate(data)
            return nxt(value)

        return core_schema.no_info_wrap_validator_function(
            _validate_tool,
            default_schema,
            serialization=core_schema.plain_serializer_function_ser_schema(
                lambda v: v.model_dump(mode="json"), info_arg=False, when_used="json"
            ),
        )

    name: str = Field(
        description="The unique name of the tool that clearly communicates its purpose."
    )
    description: str = Field(
        description="Used to tell the model how/when/why to use the tool."
    )
    env_vars: list[EnvVar] = Field(
        default_factory=list,
        description="List of environment variables used by the tool.",
    )
    args_schema: type[PydanticBaseModel] = Field(
        default=_ArgsSchemaPlaceholder,
        validate_default=True,
        description="The schema for the arguments that the tool accepts.",
    )
    result_schema: type[PydanticBaseModel] | None = Field(
        default=None,
        validate_default=True,
        description="The schema for the output that the tool returns.",
    )
    description_updated: bool = Field(
        default=False, description="Flag to check if the description has been updated."
    )
    cache_function: SerializableCallable = Field(
        default=_default_cache_function,
        description="Function that will be used to determine if the tool should be cached, should return a boolean. If None, the tool will be cached.",
    )
    result_as_answer: bool = Field(
        default=False,
        description="Flag to check if the tool should be the final agent answer.",
    )
    max_usage_count: int | None = Field(
        default=None,
        description="Maximum number of times this tool can be used. None means unlimited usage.",
    )
    current_usage_count: int = Field(
        default=0,
        description="Current number of times this tool has been used.",
    )
    _usage_lock: threading.Lock = PrivateAttr(default_factory=threading.Lock)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def tool_type(self) -> str:
        cls = type(self)
        return f"{cls.__module__}.{cls.__qualname__}"

    @property
    def formatted_description(self) -> str:
        """LLM-facing composite of name, argument schema, and description."""
        return format_description_for_llm(self.name, self.args_schema, self.description)

    @field_validator("args_schema", mode="before")
    @classmethod
    def _default_args_schema(
        cls, v: type[PydanticBaseModel] | dict[str, Any] | None
    ) -> type[PydanticBaseModel]:
        if isinstance(v, dict):
            return _schema_from_json_schema(v, f"{cls.__name__}Schema")
        if isinstance(v, type) and v is not cls._ArgsSchemaPlaceholder:
            return v
        schema = _schema_from_callable(cls._run, f"{cls.__name__}Schema")
        if not schema.model_fields:
            schema = _schema_from_callable(cls._arun, f"{cls.__name__}Schema")
        return schema

    @field_validator("result_schema", mode="before")
    @classmethod
    def _default_result_schema(
        cls, v: type[PydanticBaseModel] | dict[str, Any] | None
    ) -> type[PydanticBaseModel] | None:
        if isinstance(v, dict):
            return _schema_from_json_schema(v, f"{cls.__name__}Result")
        if v is None:
            annotation = signature(cls._run).return_annotation
            if isinstance(annotation, type) and issubclass(
                annotation, PydanticBaseModel
            ):
                return annotation
            return None
        return v

    @field_serializer("args_schema", when_used="json")
    def _serialize_args_schema(
        self, schema: type[PydanticBaseModel] | None
    ) -> dict[str, Any] | None:
        return schema.model_json_schema() if schema is not None else None

    @field_serializer("result_schema", when_used="json")
    def _serialize_result_schema(
        self, schema: type[PydanticBaseModel] | None
    ) -> dict[str, Any] | None:
        return schema.model_json_schema() if schema is not None else None

    @field_validator("max_usage_count", mode="before")
    @classmethod
    def validate_max_usage_count(cls, v: int | None) -> int | None:
        if v is not None and v <= 0:
            raise ValueError("max_usage_count must be a positive integer")
        return v

    def model_post_init(self, __context: Any) -> None:
        self._generate_description()
        super().model_post_init(__context)

    def _validate_kwargs(self, kwargs: dict[str, Any]) -> dict[str, Any]:
        """Validate (and coerce) keyword arguments against args_schema."""
        if self.args_schema is not None and self.args_schema.model_fields:
            try:
                return self.args_schema.model_validate(kwargs).model_dump()
            except Exception as e:
                hint = build_schema_hint(self.args_schema)
                raise ValueError(
                    f"Tool '{self.name}' arguments validation failed: {e}{hint}"
                ) from e
        return kwargs

    def _claim_usage(self) -> str | None:
        """Atomically check max usage and increment the counter.

        Returns None when usage was claimed, or the limit-reached message.
        """
        with self._usage_lock:
            if (
                self.max_usage_count is not None
                and self.current_usage_count >= self.max_usage_count
            ):
                return (
                    f"Tool '{self.name}' has reached its usage limit of "
                    f"{self.max_usage_count} times and cannot be used anymore."
                )
            self.current_usage_count += 1
            return None

    def run(self, *args: Any, **kwargs: Any) -> Any:
        if not args:
            kwargs = self._validate_kwargs(kwargs)

        limit_error = self._claim_usage()
        if limit_error:
            return limit_error

        result = self._run(*args, **kwargs)
        if asyncio.iscoroutine(result):
            result = asyncio.run(result)
        return result

    async def arun(self, *args: Any, **kwargs: Any) -> Any:
        if not args:
            kwargs = self._validate_kwargs(kwargs)

        limit_error = self._claim_usage()
        if limit_error:
            return limit_error

        return await self._arun(*args, **kwargs)

    async def _arun(self, *args: Any, **kwargs: Any) -> Any:
        """Async implementation. Default runs _run in a worker thread."""
        return await asyncio.to_thread(self._run, *args, **kwargs)

    def reset_usage_count(self) -> None:
        self.current_usage_count = 0

    @abstractmethod
    def _run(self, *args: Any, **kwargs: Any) -> Any:
        """Sync implementation of the tool; subclasses must implement."""

    def format_output_for_agent(self, raw_result: Any) -> str:
        """Format a raw tool result into the string sent to an agent."""
        return _format_tool_output_for_agent(self, raw_result)

    def to_structured_tool(self) -> CrewStructuredTool:
        self._set_args_schema()
        structured_tool = CrewStructuredTool(
            name=self.name,
            description=self.description,
            args_schema=self.args_schema,
            result_schema=self.result_schema,
            func=self._run,
            result_as_answer=self.result_as_answer,
            max_usage_count=self.max_usage_count,
            current_usage_count=self.current_usage_count,
            cache_function=self.cache_function,
        )
        structured_tool._original_tool = self
        return structured_tool

    @classmethod
    def from_langchain(cls, tool: Any) -> "BaseTool":
        """Wrap a langchain-style tool (callable ``func`` attribute)."""
        if not hasattr(tool, "func") or not callable(tool.func):
            raise ValueError("The provided tool must have a callable 'func' attribute.")
        args_schema = getattr(tool, "args_schema", None)
        if args_schema is None:
            args_schema = _schema_from_callable(
                tool.func, f"{sanitize_tool_name(getattr(tool, 'name', 'tool'))}_input"
            )

        func = tool.func

        class _LangchainTool(cls):  # type: ignore[misc, valid-type]
            def _run(self, *args: Any, **kwargs: Any) -> Any:
                return func(*args, **kwargs)

        return _LangchainTool(
            name=getattr(tool, "name", "Unnamed Tool"),
            description=getattr(tool, "description", ""),
            args_schema=args_schema,
        )

    def _set_args_schema(self) -> None:
        if self.args_schema is None:
            self.args_schema = _schema_from_callable(
                self._run, f"{self.__class__.__name__}Schema"
            )

    def _generate_description(self) -> None:
        """Deprecated hook kept for subclass compatibility; does nothing.

        The authored ``description`` is preserved as written; the LLM-facing
        composite is exposed via ``formatted_description``.
        """


_BASE_TOOL_CLS = BaseTool
