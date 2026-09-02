"""Decks — editing the HTML slide decks the chat renders, one slide at a time."""

import logging

from fastapi import APIRouter

from src.core.dependencies import GroupContextDep, SessionDep
from src.core.exceptions import BadRequestError
from src.schemas.deck import SlideRefineRequest, SlideRefineResponse
from src.services.decks.slide_refine import SlideRefineService

router = APIRouter(
    prefix="/decks",
    tags=["decks"],
    responses={404: {"description": "Not found"}},
)

logger = logging.getLogger(__name__)


@router.post("/slides/refine", response_model=SlideRefineResponse)
async def refine_slide(
    body: SlideRefineRequest, session: SessionDep, group_context: GroupContextDep
):
    """Revise one slide of a deck, or write a new one between two.

    One focused generation call: the model sees the slide (or its neighbours)
    plus a reference slide for the design and returns ONE ``<section>``; the
    chat splices it into the deck it already has. Recorded as a run so the
    call shows in the run activity like any other.
    """
    try:
        return await SlideRefineService.refine(
            mode=body.mode,
            instruction=body.instruction,
            group_context=group_context,
            slide=body.slide,
            reference=body.reference,
            before=body.before,
            after=body.after,
            position=body.position,
            model=body.model,
            session=session,
        )
    except ValueError as exc:
        raise BadRequestError(str(exc)) from exc
