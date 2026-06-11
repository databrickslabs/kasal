"""Crew thumbs feedback: service rules + repository aggregation + schemas."""
import pytest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

from src.services.crew_feedback_service import CrewFeedbackService
from src.utils.user_context import GroupContext


def _ctx(group_ids=None):
    return GroupContext(group_ids=group_ids or ["g1"], group_email="u@x.com")


def _svc():
    svc = CrewFeedbackService(session=MagicMock())
    svc.repository = AsyncMock()
    svc.repository.create = AsyncMock(side_effect=lambda d: SimpleNamespace(**d))
    return svc


class TestAddFeedback:
    @pytest.mark.asyncio
    async def test_up_vote_without_comment(self):
        svc = _svc()
        record = await svc.add_feedback("c1", "up", group_context=_ctx())
        assert record.rating == "up" and record.comment is None
        assert record.group_id == "g1"

    @pytest.mark.asyncio
    async def test_down_vote_requires_comment(self):
        svc = _svc()
        with pytest.raises(ValueError, match="comment"):
            await svc.add_feedback("c1", "down", comment="   ")

    @pytest.mark.asyncio
    async def test_down_vote_with_comment(self):
        svc = _svc()
        record = await svc.add_feedback("c1", "down", comment="wrong tool picked", group_context=_ctx())
        assert record.rating == "down" and record.comment == "wrong tool picked"

    @pytest.mark.asyncio
    async def test_invalid_rating_rejected(self):
        svc = _svc()
        with pytest.raises(ValueError, match="rating"):
            await svc.add_feedback("c1", "meh")


class TestQueries:
    @pytest.mark.asyncio
    async def test_list_scoped_to_group(self):
        svc = _svc()
        rows = [SimpleNamespace(id="f1")]
        svc.repository.list_by_crew_and_group = AsyncMock(return_value=rows)
        assert await svc.list_for_crew("c1", _ctx(["g1", "g2"])) == rows
        svc.repository.list_by_crew_and_group.assert_awaited_once_with("c1", ["g1", "g2"])

    @pytest.mark.asyncio
    async def test_list_no_group_returns_empty(self):
        svc = _svc()
        assert await svc.list_for_crew("c1", None) == []

    @pytest.mark.asyncio
    async def test_summary_scoped_to_group(self):
        svc = _svc()
        svc.repository.summary_by_group = AsyncMock(return_value=[{"crew_id": "c1", "up": 2, "down": 1}])
        assert (await svc.summary(_ctx()))[0]["up"] == 2


class TestSchemas:
    def test_create_request_down_requires_comment(self):
        import pydantic
        from src.schemas.crew_feedback import CrewFeedbackCreateRequest
        with pytest.raises(pydantic.ValidationError):
            CrewFeedbackCreateRequest(rating="down")
        ok = CrewFeedbackCreateRequest(rating="down", comment="broken output")
        assert ok.comment == "broken output"

    def test_create_request_up_comment_optional(self):
        from src.schemas.crew_feedback import CrewFeedbackCreateRequest
        assert CrewFeedbackCreateRequest(rating="up").comment is None

    def test_summary_entry_defaults(self):
        from src.schemas.crew_feedback import CrewFeedbackSummaryEntry
        e = CrewFeedbackSummaryEntry(crew_id="c1")
        assert e.up == 0 and e.down == 0


class TestRepositorySummary:
    @pytest.mark.asyncio
    async def test_summary_aggregates_ratings(self):
        from src.repositories.crew_feedback_repository import CrewFeedbackRepository
        session = AsyncMock()
        result = MagicMock()
        result.all.return_value = [("c1", "up", 3), ("c1", "down", 1), ("c2", "up", 2)]
        session.execute = AsyncMock(return_value=result)
        repo = CrewFeedbackRepository(session)

        rows = await repo.summary_by_group(["g1"])
        by_id = {r["crew_id"]: r for r in rows}
        assert by_id["c1"] == {"crew_id": "c1", "up": 3, "down": 1}
        assert by_id["c2"] == {"crew_id": "c2", "up": 2, "down": 0}

    @pytest.mark.asyncio
    async def test_summary_empty_groups_short_circuits(self):
        from src.repositories.crew_feedback_repository import CrewFeedbackRepository
        session = AsyncMock()
        repo = CrewFeedbackRepository(session)
        assert await repo.summary_by_group([]) == []
        session.execute.assert_not_awaited()
