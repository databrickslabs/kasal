"""Crew generation, split by strategy.

Each module holds ONE way of turning a prompt into a crew. They are mixed into
``CrewGenerationService`` rather than composed, so the split is pure movement:
every method still reads ``self`` exactly as it did in the single 2,332-line
file, and the public surface is unchanged.

The point is not tidiness. Three sessions edited that one file concurrently in a
single afternoon, and every collision was between changes to DIFFERENT
strategies that merely happened to share a filename.
"""

from src.services.crew_generation.recipes import RecipeHooksMixin
from src.services.crew_generation.complete import CompleteGenerationMixin
from src.services.crew_generation.progressive import ProgressiveGenerationMixin
from src.services.crew_generation.conversation import ConversationGenerationMixin
from src.services.crew_generation.chat_fast_path import ChatFastPathMixin

__all__ = [
    "RecipeHooksMixin",
    "CompleteGenerationMixin",
    "ProgressiveGenerationMixin",
    "ConversationGenerationMixin",
    "ChatFastPathMixin",
]
