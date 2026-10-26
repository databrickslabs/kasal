"""The CrewAI harness binding. See ``binding.py``.

Nothing here imports ``crewai`` at module scope — that costs ~2.6s and loads
chromadb, and an installation running the Kasal harness must never pay for it.
"""

from src.services.execution.harnesses.crewai.binding import CrewAIBinding

__all__ = ["CrewAIBinding"]
