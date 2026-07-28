"""Crew generation, split by strategy.

Each module holds ONE way of turning a prompt into a crew. They are mixed into
``CrewGenerationService`` rather than composed, so the split is pure movement:
every method still reads ``self`` exactly as it did in the single 2,332-line
file, and the public surface is unchanged.

The point is not tidiness. Three sessions edited that one file concurrently in a
single afternoon, and every collision was between changes to DIFFERENT
strategies that merely happened to share a filename.
"""

from src.services.generation.crew.chat_fast_path import ChatFastPathMixin
from src.services.generation.crew.complete import CompleteGenerationMixin
from src.services.generation.crew.conversation import ConversationGenerationMixin
from src.services.generation.crew.progressive import ProgressiveGenerationMixin
from src.services.generation.crew.recipes import RecipeHooksMixin

__all__ = [
    "RecipeHooksMixin",
    "CompleteGenerationMixin",
    "ProgressiveGenerationMixin",
    "ConversationGenerationMixin",
    "ChatFastPathMixin",
]
