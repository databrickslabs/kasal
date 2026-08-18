"""Kasal's tools, in the shape CrewAI calls.

The 38 first-party tools are NEVER ported per harness. They subclass
``services/tools/base.BaseTool`` — a surface deliberately modelled on crewAI
1.15.5 — so making them callable from CrewAI is a wrapper, not a rewrite. A
second implementation of a tool is a second place for a Databricks query, a
credential lookup or a redaction rule to be subtly different.

## This wrapper is also where the run's tool policy lives

Under the Kasal harness, three behaviours hang off the runtime's tool hooks
(``runtime/executor.register_tool_hooks``): the human-in-the-loop approval gate,
tool-call replay, and the outcome ledger that lets a task output be flagged when
its sources were all unavailable. CrewAI has no such hook, so they attach HERE —
in the one place every CrewAI tool call passes through.

That keeps the guarantee where it belongs. An approval gate that only works on
one harness is not an approval gate; it is a setting that silently stops applying
when an operator changes a dropdown.
"""

from __future__ import annotations

from contextvars import ContextVar
from typing import Any, List, Optional, Tuple, Type

from pydantic import BaseModel

from src.core.logger import LoggerManager
from src.services.execution.harnesses.crewai.availability import crewai_symbols

logger = LoggerManager.get_instance().crew

#: The agent and task CrewAI is currently running a tool for.
#:
#: ``crewai.tools.BaseTool._run`` receives neither, but ``wrap_tool`` needs both
#: — without them a tool's trace rows carry no agent role and no task id, so the
#: timeline cannot group them under the task that caused them. CrewAI's
#: ``before_tool_call`` hook DOES carry both and fires synchronously immediately
#: before the call, so it can hand them over through a ContextVar.
_tool_context: ContextVar[Tuple[Any, Any]] = ContextVar(
    "crewai_tool_context", default=(None, None)
)

_hook_installed = False


def current_tool_context() -> Tuple[Any, Any]:
    """The (agent, task) for the tool call being made, or (None, None)."""
    return _tool_context.get()


def _install_tool_context_hook() -> None:
    """Capture agent/task before each CrewAI tool call. Idempotent.

    Registered once per process rather than per run: it only writes a
    ContextVar, so it is inert for anything that does not read it, and CrewAI's
    hook registry is global anyway.
    """
    global _hook_installed
    if _hook_installed:
        return
    try:
        from crewai.hooks import register_before_tool_call_hook

        def _capture(context: Any) -> None:
            _tool_context.set(
                (getattr(context, "agent", None), getattr(context, "task", None))
            )

        register_before_tool_call_hook(_capture)
        _hook_installed = True
    except Exception as e:  # noqa: BLE001 — attribution is not worth a failed run
        logger.warning(
            "Could not install the CrewAI tool-context hook (%s); tool trace "
            "rows will not carry their agent and task",
            e,
        )


#: Cache the generated class per process. ``type()`` per tool would defeat
#: pydantic's schema cache and rebuild a model for every tool of every agent.
_adapter_class: Optional[type] = None


def _adapter_base() -> type:
    """The wrapper class, built once, over ``crewai.tools.BaseTool``."""
    global _adapter_class
    if _adapter_class is not None:
        return _adapter_class

    crew_base = crewai_symbols()["BaseTool"]
    _install_tool_context_hook()

    class KasalToolAdapter(crew_base):  # type: ignore[misc, valid-type]
        """One Kasal tool, callable by a CrewAI agent.

        ``name``, ``description`` and ``args_schema`` are COPIED rather than
        proxied: CrewAI reads them to build the function schema it sends to the
        model, and a property that resolves lazily would be read before the
        wrapped tool is attached.
        """

        model_config = {"arbitrary_types_allowed": True}

        def _run(self, *args: Any, **kwargs: Any) -> Any:
            """Run the Kasal tool through the runtime's own tool pipeline.

            NOT ``inner.run(**kwargs)``. ``runtime/executor.wrap_tool`` is where
            a tool call acquires everything that makes it a Kasal tool call:

            * the approval gate (``kernel/tool_approval``) — a HITL gate that
              only applied on one harness would be a setting that silently stops
              applying when an operator changes a dropdown
            * replay (``kernel/tool_replay``) via ``ToolCallAnswered``
            * ``ToolUsageStarted`` / ``Finished`` / ``Error`` on the Kasal bus,
              which is where every tool row in the trace comes from
            * the outcome ledger that lets a task output be flagged when all of
              its sources were unavailable

            Reusing it, rather than re-emitting a subset here, is what makes
            those behaviours true on both harnesses by construction.
            """
            inner = getattr(self, "_kasal_tool", None)
            if inner is None:  # pragma: no cover — construction guarantees it
                raise RuntimeError(f"Tool adapter {self.name!r} has no wrapped tool")

            # From the module, not the package: ``runtime/__init__.py`` is
            # generated and does not export it.
            from src.services.execution.runtime.executor import wrap_tool

            agent, task = current_tool_context()
            return wrap_tool(inner, agent=agent, task=task)(*args, **kwargs)

        async def _arun(self, *args: Any, **kwargs: Any) -> Any:
            """The async entry point. Same pipeline, off the event loop.

            ``wrap_tool`` is synchronous; running it in a thread keeps a slow
            tool from blocking the loop, which is why CrewAI asks for an async
            variant at all.
            """
            import asyncio

            return await asyncio.to_thread(self._run, *args, **kwargs)

        @property
        def kasal_tool(self) -> Any:
            return getattr(self, "_kasal_tool", None)

    _adapter_class = KasalToolAdapter
    return KasalToolAdapter


def _args_schema_of(tool: Any) -> Optional[Type[BaseModel]]:
    """The wrapped tool's argument model, when it has a usable one.

    Returned as-is rather than rebuilt. CrewAI turns it into the JSON schema the
    model sees; regenerating it here would give the two harnesses different tool
    signatures for the same tool, which is exactly the class of difference that
    makes a cross-harness comparison meaningless.
    """
    schema = getattr(tool, "args_schema", None)
    if isinstance(schema, type) and issubclass(schema, BaseModel):
        return schema
    return None


def adapt_tool(tool: Any) -> Any:
    """One Kasal tool as a CrewAI tool. An already-CrewAI tool passes through.

    Pass-through matters: MCP tools, and anything a future integration hands
    over already in CrewAI's shape, must not be double-wrapped — a wrapper
    around a wrapper reports the inner one's name and loses the outer one's
    schema.
    """
    crew_base = crewai_symbols()["BaseTool"]
    if isinstance(tool, crew_base):
        return tool

    # A tool the adapter cannot actually CALL is worse than an absent one: it
    # reaches the model as a usable function and fails at the moment the agent
    # commits to using it. Refuse here, where `adapt_tools` turns it into one
    # warning line naming the tool.
    if not callable(getattr(tool, "run", None)):
        raise TypeError(
            f"{type(tool).__name__} is not a Kasal BaseTool: it has no callable "
            f"run(), so a CrewAI agent could not invoke it"
        )

    adapter_cls = _adapter_base()
    fields: dict[str, Any] = {
        "name": str(getattr(tool, "name", None) or type(tool).__name__),
        "description": str(getattr(tool, "description", "") or ""),
    }
    schema = _args_schema_of(tool)
    if schema is not None:
        fields["args_schema"] = schema

    adapter = adapter_cls(**fields)
    # Past pydantic: the wrapped tool holds sessions, credentials and a group
    # context. A declared field would put all of it in `model_dump()`, which is
    # how a tool config ends up serialized into a trace row.
    object.__setattr__(adapter, "_kasal_tool", tool)
    return adapter


def adapt_tools(tools: Optional[List[Any]]) -> List[Any]:
    """Every tool an agent was given, in CrewAI's shape.

    A tool that cannot be adapted is DROPPED with a warning rather than raising.
    One malformed tool must not fail a run that has twelve others — but it must
    not vanish silently either, because "the agent ignored my tool" is
    unanswerable without this line.
    """
    adapted: List[Any] = []
    for tool in tools or []:
        try:
            adapted.append(adapt_tool(tool))
        except Exception as e:  # noqa: BLE001 — one bad tool is not a failed run
            logger.warning(
                "Could not adapt tool %r for the CrewAI harness (%s); it will not "
                "be available to the agent",
                getattr(tool, "name", tool),
                e,
            )
    return adapted
