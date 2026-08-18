"""Kasal's memory seams, on a CrewAI crew.

Kasal wires memory through two lists on its own ``Crew``:

* ``context_providers`` — run when each task's CONTEXT is assembled, i.e. AFTER
  the prior tasks have produced output. That timing is the point: recall queries
  blend the static task description with what this run has actually produced, so
  a saved crew does not match its own history on template text alone.
* ``output_sinks`` — run after every completed task, fire-and-forget.

CrewAI has neither. It does, however, have the two methods those seams
correspond to, and they are called through ``self``:

* ``Crew._get_context(task, task_outputs)`` — the string handed to the task
* ``Crew._process_task_result(task, output)`` — after each task completes

So the bridge is a SUBCLASS overriding both, rather than a re-implementation of
recall. The memory subsystem, its queries, its events and its group scoping are
untouched; only the two attachment points differ.

## CrewAI's own memory is off, and its object must not be lost with it

``build.py`` forces ``Crew(memory=False)``: CrewAI 1.15 ships unified cognitive
memory over chromadb/lancedb, and letting it initialise would fork tenant memory
across two stores.

But the call sites that wire recall read the memory object back off the crew
(``getattr(crew, "memory", None)``). With ``memory=False`` that returns False,
``make_memory_context_provider(False, ...)`` returns None, and the crew is
silently memory-less — it neither reads nor writes, with nothing to show for it.
That is the exact failure ``flow_methods`` warns about in prose.

So the Kasal memory object is carried on the crew as ``_kasal_memory`` and read
back through the binding's ``crew_memory()``, which every call site now uses.
"""

from __future__ import annotations

from typing import Any, List, Optional

from src.core.logger import LoggerManager
from src.services.execution.harnesses.crewai.availability import crewai_symbols

logger = LoggerManager.get_instance().crew

_crew_class: Optional[type] = None


def kasal_memory_crew_class() -> type:
    """A ``crewai.Crew`` that honours Kasal's two memory seams. Built once."""
    global _crew_class
    if _crew_class is not None:
        return _crew_class

    crew_base = crewai_symbols()["Crew"]

    class KasalMemoryCrew(crew_base):  # type: ignore[misc, valid-type]
        """CrewAI's Crew, with ``context_providers`` and ``output_sinks``.

        The lists live in private attributes rather than pydantic fields: they
        hold closures over a memory backend and a group context, and a declared
        field would put all of that into ``model_dump()`` — which is how a
        credential ends up serialized into a checkpoint or a trace row.
        """

        @property
        def context_providers(self) -> List[Any]:
            if not hasattr(self, "_context_providers"):
                object.__setattr__(self, "_context_providers", [])
            return self._context_providers  # type: ignore[attr-defined]

        @property
        def output_sinks(self) -> List[Any]:
            if not hasattr(self, "_output_sinks"):
                object.__setattr__(self, "_output_sinks", [])
            return self._output_sinks  # type: ignore[attr-defined]

        def _get_context(self, task: Any, task_outputs: List[Any]) -> str:
            """CrewAI's context, plus whatever the providers recall.

            ``_get_context`` is a ``staticmethod`` on the base class but is
            always called as ``self._get_context(...)``, so overriding it as an
            instance method is a supported extension point rather than a trick.
            """
            base = crew_base._get_context(task, task_outputs)
            providers = self.context_providers
            if not providers:
                return base

            agent = getattr(task, "agent", None)
            chunks = [base] if base else []
            for provider in providers:
                try:
                    extra = provider(task=task, agent=agent, context=base or None)
                except Exception as e:  # noqa: BLE001 — recall never breaks a run
                    logger.warning("Memory context provider failed: %s", e)
                    continue
                if extra:
                    chunks.append(str(extra))
            return "\n\n".join(chunks)

        def _process_task_result(self, task: Any, output: Any) -> None:
            """CrewAI's own bookkeeping, then Kasal's persistence.

            A RESTORED task is skipped: its sinks already ran in the attempt
            that produced the checkpoint, and running them again would write
            the same memory twice and record a second checkpoint unit for work
            that did not happen.
            """
            crew_base._process_task_result(self, task, output)
            if getattr(task, "_kasal_restored", False):
                return
            for sink in self.output_sinks:
                try:
                    sink(task=task, output=output)
                except Exception as e:  # noqa: BLE001 — a write never breaks a run
                    logger.warning("Memory output sink failed: %s", e)

        # ---------------------------------------------------------------
        # Crash-resume
        # ---------------------------------------------------------------

        def kickoff(self, *args: Any, from_checkpoint: Any = None, **kwargs: Any):
            self._stamp_run_deadline()
            self._seed_from_checkpoint(from_checkpoint)
            return crew_base.kickoff(self, *args, **kwargs)

        async def kickoff_async(
            self, *args: Any, from_checkpoint: Any = None, **kwargs: Any
        ):
            self._stamp_run_deadline()
            self._seed_from_checkpoint(from_checkpoint)
            return await crew_base.kickoff_async(self, *args, **kwargs)

        def _stamp_run_deadline(self) -> None:
            """One deadline for the whole run, on every agent.

            The same thing Kasal's ``Crew.kickoff`` does, and for the same
            reason. ``resolve_execution_budget`` builds the per-call clock fresh
            on EVERY ``call()``, so by itself it bounds one call and nothing
            else; ``run_deadline`` is the one fixed point it takes the minimum
            against.

            That distinction is sharper under CrewAI than under Kasal. CrewAI's
            executor owns the tool loop, so one ``call()`` is one round — a
            30-second cap became 30 seconds *per round*, and an agent ran for
            two and a half minutes against it without ever timing out.

            Computed HERE rather than at build time so the clock starts when
            work does, not when the crew was assembled.
            """
            import time

            seconds = getattr(self, "_kasal_run_max_seconds", None)
            if not seconds:
                return
            deadline = time.monotonic() + float(seconds)
            from src.services.execution.harnesses.crewai.deadline import (
                RUN_DEADLINE_ATTR,
            )

            for agent in [*(self.agents or []), self.manager_agent]:
                if agent is None:
                    continue
                object.__setattr__(agent, "run_deadline", deadline)
                # Kept separately so a per-TURN deadline can be stamped over
                # `run_deadline` and then restored to the run's ceiling.
                object.__setattr__(agent, RUN_DEADLINE_ATTR, deadline)
            logger.info(
                "[crewai] run wall clock: %ss for %d agent(s)",
                seconds,
                len(self.agents or []),
            )

        def _seed_from_checkpoint(self, from_checkpoint: Any) -> None:
            """Make the matching prefix return its stored output instead of running.

            The tasks stay in the list. CrewAI accumulates ``task_outputs`` as
            it goes and feeds them to ``_get_context``, so removing a restored
            task would change the context every later task sees — a resume that
            silently alters the inputs of the work it did not redo.
            """
            if not from_checkpoint:
                return

            from src.services.execution.harnesses.crewai.checkpoint import (
                restorable_outputs,
            )

            sequential = (
                str(getattr(self.process, "value", self.process)) == "sequential"
            )
            restored = restorable_outputs(list(self.tasks), from_checkpoint, sequential)
            if not restored:
                logger.info(
                    "[crewai] checkpoint restored nothing; running from scratch"
                )
                return

            for index, output in restored.items():
                _seed_task(self.tasks[index], output)
            logger.info(
                "[crewai] restored %d task(s) from checkpoint; re-running from %d",
                len(restored),
                max(restored) + 1,
            )

    _crew_class = KasalMemoryCrew
    return KasalMemoryCrew


def carry_memory(crew: Any, memory: Any) -> None:
    """Keep the Kasal memory object reachable after ``memory=False``."""
    object.__setattr__(crew, "_kasal_memory", memory)


def crew_memory(crew: Any) -> Any:
    """The Kasal memory object for this crew, whatever CrewAI's field says."""
    return getattr(crew, "_kasal_memory", None)


def wire_memory(crew: Any, provider: Any = None, sink: Any = None) -> None:
    """Attach recall and persistence to a CrewAI crew."""
    if provider is not None:
        crew.context_providers.append(provider)
    if sink is not None:
        crew.output_sinks.append(sink)


def _seed_task(task: Any, output: Any) -> None:
    """Replace one task's execution with its recorded result.

    ``TaskCheckpointRestoredEvent`` is emitted — and NOT a completion event.
    A resume is a NEW execution with its own trace, so a silently skipped
    prefix would leave that trace starting midway through the crew with no sign
    the earlier tasks existed. This says the task was restored without claiming
    it ran, which is exactly what the Kasal runtime does.
    """
    from src.core.events import TaskCheckpointRestoredEvent, event_bus

    object.__setattr__(task, "_kasal_restored", True)
    task.output = output

    def _restored_sync(*args: Any, **kwargs: Any) -> Any:
        event_bus.emit(task, TaskCheckpointRestoredEvent(output=output, task=task))
        return output

    async def _restored_async(*args: Any, **kwargs: Any) -> Any:
        return _restored_sync()

    object.__setattr__(task, "execute_sync", _restored_sync)
    object.__setattr__(task, "aexecute_sync", _restored_async)
