"""Which agent runtime this app runs on — Kasal's own, or CrewAI.

Chosen at EXPORT time, not at run time: `RUNTIME` below is stamped by the
exporter, and only the matching dependencies are shipped. There is no switch to
flip in a deployed app, because a bundle carries one runtime.

``agent.py`` imports ``Agent``, ``Task``, ``Crew``, ``Process`` and
``LLMGuardrail`` from here and constructs them exactly the same way either way.
Everything that differs between the two runtimes is in this file.

**Kasal** (the default) — the vendored runtime under ``kasal_runtime/``. The app
depends on no third-party agent framework at all.

**CrewAI** — CrewAI's Agent/Task/Crew, sitting on Kasal's transport, tools and
events, which is the same arrangement the platform runs. That matters: if the
exported app called models through CrewAI's own LLM stack instead, it would not
be the app you tested — the endpoint fixes Kasal's handlers carry (stripping
``cache_breakpoint``, empty ``name``, ``strict``) would be gone, and a Databricks
400 would surface in your customer's workspace rather than in yours.
"""

from __future__ import annotations

from typing import Any

# Stamped by the exporter: "kasal" or "crewai".
RUNTIME = "{{BUNDLE_RUNTIME}}"

if RUNTIME == "crewai":
    from crewai import Agent as _CrewAgent
    from crewai import Crew as _CrewCrew
    from crewai import Process  # noqa: F401 — re-exported
    from crewai import Task as _CrewTask

    from agent_server.kasal_runtime.services.execution.harnesses.crewai.build import (
        translate,
    )
    from agent_server.kasal_runtime.services.execution.harnesses.crewai.llm import (
        build_kasal_backed_llm,
    )
    from agent_server.kasal_runtime.services.execution.harnesses.crewai.tools import (
        adapt_tools,
    )

    def _wrap_llm(llm: Any) -> Any:
        """Kasal's transport object, presented to CrewAI as an LLM.

        Left alone when it already is one (a str model name, or something CrewAI
        built itself), so a config that names a model still works.
        """
        if llm is None or isinstance(llm, str):
            return llm
        from crewai.llms.base_llm import BaseLLM as _CrewBaseLLM

        if isinstance(llm, _CrewBaseLLM):
            return llm
        return build_kasal_backed_llm(llm)

    class Agent(_CrewAgent):  # type: ignore[misc, valid-type]
        """CrewAI's Agent, built from the kwargs this app already assembles.

        A subclass rather than a factory function because ``agent.py`` annotates
        with these names; a function there would be a type error at import.
        """

        def __init__(self, **kwargs: Any) -> None:
            kwargs["llm"] = _wrap_llm(kwargs.get("llm"))
            if kwargs.get("tools"):
                kwargs["tools"] = adapt_tools(kwargs["tools"])
            accepted, _dropped = translate(kwargs, _CrewAgent, "agent")
            super().__init__(**accepted)

    class Task(_CrewTask):  # type: ignore[misc, valid-type]
        def __init__(self, **kwargs: Any) -> None:
            if kwargs.get("tools"):
                kwargs["tools"] = adapt_tools(kwargs["tools"])
            guardrail = kwargs.get("guardrail")
            if guardrail is not None and not isinstance(guardrail, str):
                # A Kasal LLMGuardrail cannot be handed to CrewAI, which wants a
                # string or a callable. Rebuild it as CrewAI's own over the same
                # description — the guardrail is the description.
                description = getattr(guardrail, "description", None)
                kwargs["guardrail"] = description if description else None
            accepted, _dropped = translate(kwargs, _CrewTask, "task")
            super().__init__(**accepted)

    class Crew(_CrewCrew):  # type: ignore[misc, valid-type]
        def __init__(self, **kwargs: Any) -> None:
            for field in ("manager_llm", "function_calling_llm", "chat_llm"):
                if kwargs.get(field) is not None:
                    kwargs[field] = _wrap_llm(kwargs[field])
            # CrewAI's own memory would start chromadb inside the app. Kasal's
            # memory subsystem is not part of an exported bundle either, so
            # memory is off here whatever the crew was configured with — the
            # same call the platform's binding makes, for the same reason.
            kwargs["memory"] = False
            accepted, _dropped = translate(kwargs, _CrewCrew, "crew")
            super().__init__(**accepted)

    class LLMGuardrail:  # type: ignore[no-redef]
        """CrewAI takes a guardrail as a string; this keeps ``agent.py`` uniform."""

        def __init__(self, description: str, llm: Any = None) -> None:
            self.description = description
            self.llm = llm

else:
    from agent_server.kasal_runtime.services.execution.runtime import (  # noqa: F401
        Agent,
        Crew,
        LLMGuardrail,
        Process,
        Task,
    )

__all__ = ["RUNTIME", "Agent", "Crew", "LLMGuardrail", "Process", "Task"]
