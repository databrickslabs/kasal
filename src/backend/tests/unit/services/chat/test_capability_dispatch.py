"""Turning a route decision into a run — and refusing to, correctly.

Two things are load-bearing here and neither is obvious from the happy path:

* a miss NEVER falls through to generation. The user selected "Use existing";
  quietly building a crew instead runs work they did not ask for and bills a full
  crew run for it;
* every resolve goes through ``resolve_capability_for_group``, the single
  authorisation choke point. Reaching past it — to ``find_by_external_name``, or
  to the catalogue — would create a second visibility semantic where an
  unpublished crew silently becomes chat-invocable.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from src.core.exceptions import ForbiddenError, NotFoundError
from src.schemas.crew_publication import PublishedCapability
from src.services.chat.capability_dispatch import (
    build_dispatch_result,
    no_match,
    route_and_dispatch,
)
from src.services.chat.capability_router import RouteDecision

CREW_ID = "11111111-1111-1111-1111-111111111111"
FLOW_ID = "22222222-2222-2222-2222-222222222222"

SCHEMA = {
    "type": "object",
    "properties": {"region": {"type": "string"}, "quarter": {"type": "string"}},
    "required": ["region", "quarter"],
}


def _capability(name="quarterly_risk_review", entity_type="crew", entity_id=CREW_ID):
    return PublishedCapability(
        entity_type=entity_type,
        entity_id=entity_id,
        name=name,
        description="Runs the quarterly risk review for one region.",
        input_schema=SCHEMA,
    )


def _publication(entity_type="crew", entity_id=CREW_ID):
    return SimpleNamespace(
        entity_type=entity_type,
        entity_id=entity_id,
        external_name="quarterly_risk_review",
        input_schema=SCHEMA,
    )


def _crew():
    return SimpleNamespace(
        id=CREW_ID,
        name="Quarterly Risk Review",
        nodes=[{"type": "agentNode"}],
        edges=[],
        process="sequential",
        memory=True,
        verbose=False,
        max_rpm=10,
    )


def _flow():
    return SimpleNamespace(
        id=FLOW_ID, name="Risk Flow", nodes=[], edges=[], flow_config={"a": 1}
    )


def _group():
    return SimpleNamespace(primary_group_id="grp-1", group_ids=["grp-1"])


def _decision(**kw):
    return RouteDecision(
        **{
            "capability": "quarterly_risk_review",
            "confidence": 0.95,
            "inputs": {"region": "DACH"},
            **kw,
        }
    )


class TestResultShape:
    @pytest.mark.asyncio
    async def test_crew_returns_the_existing_execute_crew_shape(self):
        # Deliberately the SAME dict the slash-command path builds. A new result
        # type would mean reimplementing variable detection, config building,
        # execution creation and streaming beside the ones that exist.
        catalog = SimpleNamespace(get=AsyncMock(return_value=_crew()))
        result = await build_dispatch_result(
            _decision(), _publication(), _capability(), catalog, None, _group()
        )
        assert result["type"] == "execute_crew"
        assert set(result["plan"]) == {
            "id",
            "name",
            "nodes",
            "edges",
            "process",
            "memory",
            "verbose",
            "max_rpm",
        }

    @pytest.mark.asyncio
    async def test_carries_what_was_extracted_and_what_is_required(self):
        catalog = SimpleNamespace(get=AsyncMock(return_value=_crew()))
        result = await build_dispatch_result(
            _decision(), _publication(), _capability(), catalog, None, _group()
        )
        assert result["extracted_inputs"] == {"region": "DACH"}
        assert result["input_schema"]["required"] == ["region", "quarter"]
        assert result["capability"] == "quarterly_risk_review"

    @pytest.mark.asyncio
    async def test_entity_id_is_coerced_to_a_uuid(self):
        # It is a string on the publication because it addresses two id types,
        # but Crew.id is a UUID column and asyncpg rejects a str.
        catalog = SimpleNamespace(get=AsyncMock(return_value=_crew()))
        await build_dispatch_result(
            _decision(), _publication(), _capability(), catalog, None, _group()
        )
        assert str(catalog.get.await_args.args[0]) == CREW_ID
        assert not isinstance(catalog.get.await_args.args[0], str)

    @pytest.mark.asyncio
    async def test_flow_returns_the_existing_execute_flow_shape(self):
        flows = SimpleNamespace(
            get_flow_with_group_check=AsyncMock(return_value=_flow())
        )
        result = await build_dispatch_result(
            _decision(),
            _publication(entity_type="flow", entity_id=FLOW_ID),
            _capability(entity_type="flow", entity_id=FLOW_ID),
            None,
            flows,
            _group(),
        )
        assert result["type"] == "execute_flow"
        assert set(result["flow"]) == {"id", "name", "nodes", "edges", "flow_config"}


class TestDeletedEntities:
    @pytest.mark.asyncio
    async def test_a_deleted_crew_declines_instead_of_crashing(self):
        catalog = SimpleNamespace(get=AsyncMock(return_value=None))
        result = await build_dispatch_result(
            _decision(), _publication(), _capability(), catalog, None, _group()
        )
        assert result["type"] == "catalog_no_match"

    @pytest.mark.asyncio
    async def test_a_deleted_flow_raises_rather_than_returning_none(self):
        # The asymmetry that a `flow is None` check sails straight past.
        flows = SimpleNamespace(
            get_flow_with_group_check=AsyncMock(side_effect=NotFoundError("gone"))
        )
        result = await build_dispatch_result(
            _decision(),
            _publication(entity_type="flow", entity_id=FLOW_ID),
            None,
            None,
            flows,
            _group(),
        )
        assert result["type"] == "catalog_no_match"

    @pytest.mark.asyncio
    async def test_rows_disagreeing_about_ownership_declines(self):
        flows = SimpleNamespace(
            get_flow_with_group_check=AsyncMock(side_effect=ForbiddenError("denied"))
        )
        result = await build_dispatch_result(
            _decision(),
            _publication(entity_type="flow", entity_id=FLOW_ID),
            None,
            None,
            flows,
            _group(),
        )
        assert result["type"] == "catalog_no_match"


class TestNoMatchReads:
    def test_the_three_reasons_do_not_read_the_same(self):
        # An empty workspace needs a signpost to the publish dialog; a genuine
        # miss needs to say so; and our own bug must not be dressed up as the
        # user's.
        empty = no_match("nothing_published")["message"]
        miss = no_match("no_match")["message"]
        assert "Publish a crew or flow" in empty
        assert empty != miss

    def test_always_offers_the_build(self):
        for reason in ("nothing_published", "no_match", "unresolved"):
            assert no_match(reason)["build_instead"] is True


class TestRouteAndDispatch:
    @staticmethod
    def _publications(capabilities, resolved):
        service = SimpleNamespace(
            list_capabilities_for_group=AsyncMock(return_value=capabilities),
            resolve_capability_for_group=AsyncMock(return_value=resolved),
        )
        return (
            patch(
                "src.services.chat.capability_dispatch.PublicationService",
                return_value=service,
            ),
            service,
        )

    @pytest.mark.asyncio
    async def test_an_empty_catalog_costs_no_llm_call(self):
        ask = AsyncMock()
        ctx, _ = self._publications([], None)
        with ctx:
            result = await route_and_dispatch(
                session=None,
                group_context=_group(),
                message="anything",
                ask_models=ask,
                log_llm=AsyncMock(),
                catalog_service=None,
                flow_service=None,
            )
        assert result["reason"] == "nothing_published"
        ask.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_reads_the_catalog_on_the_chat_protocol_only(self):
        ctx, service = self._publications([], None)
        with ctx:
            await route_and_dispatch(
                session=None,
                group_context=_group(),
                message="x",
                ask_models=AsyncMock(),
                log_llm=AsyncMock(),
                catalog_service=None,
                flow_service=None,
            )
        service.list_capabilities_for_group.assert_awaited_once_with(["grp-1"], "chat")

    @pytest.mark.asyncio
    async def test_a_low_confidence_pick_declines_rather_than_generating(self):
        ctx, service = self._publications([_capability()], _publication())
        with (
            ctx,
            patch(
                "src.services.chat.capability_dispatch.TemplateService."
                "get_effective_template_content",
                AsyncMock(return_value="prompt"),
            ),
        ):
            result = await route_and_dispatch(
                session=None,
                group_context=_group(),
                message="run something",
                ask_models=AsyncMock(
                    return_value=(
                        {"capability": "quarterly_risk_review", "confidence": 0.2},
                        "m",
                        1,
                    )
                ),
                log_llm=AsyncMock(),
                catalog_service=None,
                flow_service=None,
            )
        assert result["type"] == "catalog_no_match"
        # The point: it did NOT resolve and did NOT run anything.
        service.resolve_capability_for_group.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_no_model_answered_declines_rather_than_guessing(self):
        # An outage is indistinguishable from a genuine miss, and running the
        # wrong capability is worse than running nothing.
        ctx, _ = self._publications([_capability()], _publication())
        with (
            ctx,
            patch(
                "src.services.chat.capability_dispatch.TemplateService."
                "get_effective_template_content",
                AsyncMock(return_value="prompt"),
            ),
        ):
            result = await route_and_dispatch(
                session=None,
                group_context=_group(),
                message="run the risk review",
                ask_models=AsyncMock(return_value=(None, None, 3)),
                log_llm=AsyncMock(),
                catalog_service=None,
                flow_service=None,
            )
        assert result["type"] == "catalog_no_match"

    @pytest.mark.asyncio
    async def test_a_name_that_will_not_resolve_declines(self):
        ctx, _ = self._publications([_capability()], None)
        with (
            ctx,
            patch(
                "src.services.chat.capability_dispatch.TemplateService."
                "get_effective_template_content",
                AsyncMock(return_value="prompt"),
            ),
        ):
            result = await route_and_dispatch(
                session=None,
                group_context=_group(),
                message="run the risk review",
                ask_models=AsyncMock(
                    return_value=(
                        {"capability": "quarterly_risk_review", "confidence": 0.95},
                        "m",
                        1,
                    )
                ),
                log_llm=AsyncMock(),
                catalog_service=None,
                flow_service=None,
            )
        assert result["reason"] == "unresolved"

    @pytest.mark.asyncio
    async def test_a_confident_pick_runs_the_published_crew(self):
        ctx, service = self._publications([_capability()], _publication())
        log = AsyncMock()
        with (
            ctx,
            patch(
                "src.services.chat.capability_dispatch.TemplateService."
                "get_effective_template_content",
                AsyncMock(return_value="prompt"),
            ),
        ):
            result = await route_and_dispatch(
                session=None,
                group_context=_group(),
                message="Kick off the risk review for DACH",
                ask_models=AsyncMock(
                    return_value=(
                        {
                            "capability": "quarterly_risk_review",
                            "confidence": 0.95,
                            "inputs": {
                                "region": {"value": "DACH", "source_span": "DACH"}
                            },
                        },
                        "served-model",
                        1,
                    )
                ),
                log_llm=log,
                catalog_service=SimpleNamespace(get=AsyncMock(return_value=_crew())),
                flow_service=None,
            )
        assert result["type"] == "execute_crew"
        assert result["extracted_inputs"] == {"region": "DACH"}
        # Resolved through the choke point, on the chat protocol.
        service.resolve_capability_for_group.assert_awaited_once_with(
            ["grp-1"], "chat", "quarterly_risk_review"
        )
        # And logged against the model that actually answered — this call really
        # happened, unlike the fast paths that log nothing.
        assert log.await_args.kwargs["endpoint"] == "capability-route"
        assert log.await_args.kwargs["model"] == "served-model"

    @pytest.mark.asyncio
    async def test_no_group_means_no_capabilities(self):
        # Empty group_ids returns [] rather than everything — the guarantee the
        # repository exists to hold. Assert the router honours it end to end.
        ctx, service = self._publications([], None)
        with ctx:
            result = await route_and_dispatch(
                session=None,
                group_context=SimpleNamespace(primary_group_id=None, group_ids=[]),
                message="run anything",
                ask_models=AsyncMock(),
                log_llm=AsyncMock(),
                catalog_service=None,
                flow_service=None,
            )
        assert result["type"] == "catalog_no_match"
        service.list_capabilities_for_group.assert_awaited_once_with([], "chat")


class TestConversationChangesTheDecision:
    """A turn is a turn, not an isolated instruction.

    Judged alone, "what is this Aviation sector" is a plausible news request: it
    matches a news capability, runs a whole crew, and answers a question the deck
    already on screen answers. The fix is not a follow-up/new-request classifier
    — that would force "now do the same for Germany" and "turn this into a deck"
    into the same box — it is letting the router SEE the conversation.
    """

    @staticmethod
    def _turns():
        return [
            SimpleNamespace(
                index=1,
                role="user",
                preview="gather news",
                content="q",
                capability=None,
            ),
            SimpleNamespace(
                index=2,
                role="assistant",
                preview="# News",
                content="the news summary",
                capability=None,
            ),
        ]

    @staticmethod
    def _run(model_output, turns, resolved=None, catalog=None):
        service = SimpleNamespace(
            list_capabilities_for_group=AsyncMock(return_value=[_capability()]),
            resolve_capability_for_group=AsyncMock(
                return_value=resolved if resolved is not None else _publication()
            ),
        )
        return (
            patch(
                "src.services.chat.capability_dispatch.PublicationService",
                return_value=service,
            ),
            patch(
                "src.services.chat.capability_dispatch.recent_turns",
                AsyncMock(return_value=turns),
            ),
            patch(
                "src.services.chat.capability_dispatch.TemplateService."
                "get_effective_template_content",
                AsyncMock(return_value="prompt"),
            ),
            AsyncMock(return_value=(model_output, "m", 1)),
            catalog or SimpleNamespace(get=AsyncMock(return_value=_crew())),
        )

    @pytest.mark.asyncio
    async def test_the_conversation_reaches_the_router(self):
        pubs, turns_patch, tpl, ask, catalog = self._run(
            {"capability": None, "confidence": 0.0}, self._turns()
        )
        with pubs, turns_patch, tpl:
            await route_and_dispatch(
                session=None,
                group_context=_group(),
                message="what is this Aviation sector",
                ask_models=ask,
                log_llm=AsyncMock(),
                catalog_service=catalog,
                flow_service=None,
                session_id="s1",
            )
        user_message = ask.await_args.args[0][1]["content"]
        assert "CONVERSATION SO FAR" in user_message
        assert "[answer 2]" in user_message

    @pytest.mark.asyncio
    async def test_declining_mid_conversation_answers_the_turn(self):
        pubs, turns_patch, tpl, ask, catalog = self._run(
            {"capability": None, "confidence": 0.0}, self._turns()
        )
        with pubs, turns_patch, tpl:
            result = await route_and_dispatch(
                session=None,
                group_context=_group(),
                message="what is this Aviation sector",
                ask_models=ask,
                log_llm=AsyncMock(),
                catalog_service=catalog,
                flow_service=None,
                session_id="s1",
            )
        assert result["type"] == "catalog_no_match"
        # Answered, not dead-ended — and the build offer stays beside it.
        assert result["answer_here"] is True
        assert result["build_instead"] is True

    @pytest.mark.asyncio
    async def test_declining_with_no_conversation_has_nothing_to_answer_from(self):
        pubs, turns_patch, tpl, ask, catalog = self._run(
            {"capability": None, "confidence": 0.0}, []
        )
        with pubs, turns_patch, tpl:
            result = await route_and_dispatch(
                session=None,
                group_context=_group(),
                message="write me a poem",
                ask_models=ask,
                log_llm=AsyncMock(),
                catalog_service=catalog,
                flow_service=None,
                session_id="s1",
            )
        assert result["answer_here"] is False

    @pytest.mark.asyncio
    async def test_a_referenced_answer_is_handed_to_the_run(self):
        # "Turn this into a deck" is useless if the crew starts from nothing: it
        # re-derives the material, and on a polluted memory pool it re-derives
        # the wrong subject.
        pubs, turns_patch, tpl, ask, catalog = self._run(
            {
                "capability": "quarterly_risk_review",
                "confidence": 0.95,
                "refers_to": 2,
            },
            self._turns(),
        )
        with pubs, turns_patch, tpl:
            result = await route_and_dispatch(
                session=None,
                group_context=_group(),
                message="turn this into a deck",
                ask_models=ask,
                log_llm=AsyncMock(),
                catalog_service=catalog,
                flow_service=None,
                session_id="s1",
            )
        assert result["referenced_answer"] == "the news summary"

    @pytest.mark.asyncio
    async def test_an_index_it_was_never_shown_binds_nothing(self):
        # Same stance as an unquoted value: a number it could not have read is a
        # number it made up.
        pubs, turns_patch, tpl, ask, catalog = self._run(
            {
                "capability": "quarterly_risk_review",
                "confidence": 0.95,
                "refers_to": 99,
            },
            self._turns(),
        )
        with pubs, turns_patch, tpl:
            result = await route_and_dispatch(
                session=None,
                group_context=_group(),
                message="turn this into a deck",
                ask_models=ask,
                log_llm=AsyncMock(),
                catalog_service=catalog,
                flow_service=None,
                session_id="s1",
            )
        assert result["referenced_answer"] is None

    @pytest.mark.asyncio
    async def test_a_fresh_request_carries_no_referenced_answer(self):
        pubs, turns_patch, tpl, ask, catalog = self._run(
            {"capability": "quarterly_risk_review", "confidence": 0.95},
            self._turns(),
        )
        with pubs, turns_patch, tpl:
            result = await route_and_dispatch(
                session=None,
                group_context=_group(),
                message="run the risk review for DACH",
                ask_models=ask,
                log_llm=AsyncMock(),
                catalog_service=catalog,
                flow_service=None,
                session_id="s1",
            )
        assert result["referenced_answer"] is None
