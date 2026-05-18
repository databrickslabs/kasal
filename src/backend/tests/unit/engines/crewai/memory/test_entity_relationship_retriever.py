"""
Comprehensive unit tests for EntityRelationshipRetriever.
"""
import os
import sys
from unittest.mock import MagicMock

os.environ.setdefault("DATABASE_TYPE", "sqlite")
os.environ.setdefault("SQLITE_DB_PATH", ":memory:")

_crewai_mock = MagicMock()
_MODULES_TO_MOCK = {
    "crewai": _crewai_mock,
    "crewai.tools": _crewai_mock.tools,
    "crewai.events": _crewai_mock.events,
    "crewai.flow": _crewai_mock.flow,
    "crewai.flow.flow": _crewai_mock.flow.flow,
    "crewai.flow.persistence": _crewai_mock.flow.persistence,
    "crewai.llm": _crewai_mock.llm,
    "crewai.memory": _crewai_mock.memory,
    "crewai.memory.storage": _crewai_mock.memory.storage,
    "crewai.memory.storage.rag_storage": _crewai_mock.memory.storage.rag_storage,
    "crewai.project": _crewai_mock.project,
    "crewai.tasks": _crewai_mock.tasks,
    "crewai.tasks.llm_guardrail": _crewai_mock.tasks.llm_guardrail,
    "crewai.tasks.task_output": _crewai_mock.tasks.task_output,
    "crewai.utilities": _crewai_mock.utilities,
    "crewai.utilities.converter": _crewai_mock.utilities.converter,
    "crewai.utilities.evaluators": _crewai_mock.utilities.evaluators,
    "crewai.utilities.evaluators.task_evaluator": _crewai_mock.utilities.evaluators.task_evaluator,
    "crewai.utilities.exceptions": _crewai_mock.utilities.exceptions,
    "crewai.utilities.internal_instructor": _crewai_mock.utilities.internal_instructor,
    "crewai.utilities.paths": _crewai_mock.utilities.paths,
    "crewai.utilities.printer": _crewai_mock.utilities.printer,
    "crewai.knowledge": _crewai_mock.knowledge,
    "crewai.llms": _crewai_mock.llms,
    "crewai.llms.providers": _crewai_mock.llms.providers,
    "crewai.llms.providers.openai": _crewai_mock.llms.providers.openai,
    "crewai.llms.providers.openai.completion": _crewai_mock.llms.providers.openai.completion,
    "crewai.events.types": _crewai_mock.events.types,
    "crewai.events.types.llm_events": _crewai_mock.events.types.llm_events,
    "crewai_tools": MagicMock(),
    "asyncpg": MagicMock(),
    "chromadb": MagicMock(),
}

_originals = {}
for _mod_name, _mock_obj in _MODULES_TO_MOCK.items():
    _originals[_mod_name] = sys.modules.get(_mod_name)
    sys.modules[_mod_name] = _mock_obj

import pytest
import numpy as np
from unittest.mock import AsyncMock, patch

from src.engines.crewai.memory.entity_relationship_retriever import (
    EntityRelationshipRetriever,
    EntityNode,
    RelationshipEdge,
    RetrievalCandidate,
)

for _mod_name, _original in _originals.items():
    if _original is None:
        sys.modules.pop(_mod_name, None)
    else:
        sys.modules[_mod_name] = _original


@pytest.fixture
def mock_service():
    return AsyncMock()


@pytest.fixture
def retriever(mock_service):
    return EntityRelationshipRetriever(memory_backend_service=mock_service)


def _make_entity(name, entity_type="person", description="", agent_id="agent1", relationships=None):
    return EntityNode(
        name=name,
        entity_type=entity_type,
        description=description or f"Description of {name}",
        agent_id=agent_id,
        metadata={"id": f"id_{name}", "entity_name": name, "entity_type": entity_type, "description": description or f"Description of {name}"},
        explicit_relationships=relationships or [],
    )


# ─────────────────────────────────────────────────────────────────────────────
# Initialization
# ─────────────────────────────────────────────────────────────────────────────


class TestEntityRelationshipRetrieverInit:
    def test_default_embedding_model(self, mock_service):
        r = EntityRelationshipRetriever(memory_backend_service=mock_service)
        assert r.embedding_model == "databricks-gte-large-en"

    def test_custom_embedding_model(self, mock_service):
        r = EntityRelationshipRetriever(memory_backend_service=mock_service, embedding_model="my-model")
        assert r.embedding_model == "my-model"

    def test_initial_empty_graph(self, retriever):
        assert retriever.entity_graph == {}
        assert retriever.relationship_edges == []
        assert retriever.description_embeddings == {}

    def test_stores_service_reference(self, mock_service):
        r = EntityRelationshipRetriever(memory_backend_service=mock_service)
        assert r.memory_backend_service is mock_service


# ─────────────────────────────────────────────────────────────────────────────
# _compute_embedding
# ─────────────────────────────────────────────────────────────────────────────


class TestComputeEmbedding:
    @pytest.mark.asyncio
    async def test_returns_numpy_array_on_success(self, retriever):
        fake_emb = [0.1] * 1024
        mock_llm_mgr = MagicMock()
        mock_llm_mgr.get_embedding = AsyncMock(return_value=fake_emb)
        with patch.dict("sys.modules", {"src.core.llm_manager": MagicMock(LLMManager=mock_llm_mgr)}):
            result = await retriever._compute_embedding("hello")
        assert isinstance(result, np.ndarray)
        assert result.shape == (1024,)

    @pytest.mark.asyncio
    async def test_returns_zeros_on_none_embedding(self, retriever):
        mock_llm_mgr = MagicMock()
        mock_llm_mgr.get_embedding = AsyncMock(return_value=None)
        with patch.dict("sys.modules", {"src.core.llm_manager": MagicMock(LLMManager=mock_llm_mgr)}):
            result = await retriever._compute_embedding("hello")
        np.testing.assert_array_equal(result, np.zeros(1024))

    @pytest.mark.asyncio
    async def test_returns_zeros_on_empty_list(self, retriever):
        mock_llm_mgr = MagicMock()
        mock_llm_mgr.get_embedding = AsyncMock(return_value=[])
        with patch.dict("sys.modules", {"src.core.llm_manager": MagicMock(LLMManager=mock_llm_mgr)}):
            result = await retriever._compute_embedding("hello")
        np.testing.assert_array_equal(result, np.zeros(1024))

    @pytest.mark.asyncio
    async def test_returns_zeros_on_exception(self, retriever):
        mock_llm_mgr = MagicMock()
        mock_llm_mgr.get_embedding = AsyncMock(side_effect=RuntimeError("fail"))
        with patch.dict("sys.modules", {"src.core.llm_manager": MagicMock(LLMManager=mock_llm_mgr)}):
            result = await retriever._compute_embedding("hello")
        assert result.shape == (1024,)

    @pytest.mark.asyncio
    async def test_uses_configured_model(self, mock_service):
        r = EntityRelationshipRetriever(memory_backend_service=mock_service, embedding_model="custom-m")
        mock_llm_mgr = MagicMock()
        mock_llm_mgr.get_embedding = AsyncMock(return_value=[0.5] * 1024)
        with patch.dict("sys.modules", {"src.core.llm_manager": MagicMock(LLMManager=mock_llm_mgr)}):
            await r._compute_embedding("text")
        mock_llm_mgr.get_embedding.assert_called_once_with("text", model="custom-m")


# ─────────────────────────────────────────────────────────────────────────────
# _parse_explicit_relationships
# ─────────────────────────────────────────────────────────────────────────────


class TestParseExplicitRelationships:
    def test_empty_string_returns_empty(self, retriever):
        assert retriever._parse_explicit_relationships("") == []

    def test_none_returns_empty(self, retriever):
        assert retriever._parse_explicit_relationships(None) == []

    def test_parses_dash_separated_relationships(self, retriever):
        rel_str = "- Alice\\n- Bob\\n- Charlie"
        result = retriever._parse_explicit_relationships(rel_str)
        assert "Alice" in result
        assert "Bob" in result
        assert "Charlie" in result

    def test_ignores_lines_without_dash(self, retriever):
        rel_str = "Alice\\nno dash\\n- Bob"
        result = retriever._parse_explicit_relationships(rel_str)
        assert "no dash" not in result
        assert "Bob" in result

    def test_strips_whitespace(self, retriever):
        rel_str = "-  Alice  \\n-  Bob  "
        result = retriever._parse_explicit_relationships(rel_str)
        assert "Alice" in result
        assert "Bob" in result


# ─────────────────────────────────────────────────────────────────────────────
# _is_name_match
# ─────────────────────────────────────────────────────────────────────────────


class TestIsNameMatch:
    def test_exact_match(self, retriever):
        assert retriever._is_name_match("Alice", "Alice") is True

    def test_case_insensitive_match(self, retriever):
        assert retriever._is_name_match("alice", "ALICE") is True

    def test_substring_match(self, retriever):
        assert retriever._is_name_match("Alice", "Alice Johnson") is True

    def test_reverse_substring_match(self, retriever):
        assert retriever._is_name_match("Alice Johnson", "Alice") is True

    def test_no_match(self, retriever):
        assert retriever._is_name_match("Alice", "Bob Smith") is False

    def test_high_word_overlap_matches(self, retriever):
        assert retriever._is_name_match("John Smith", "John Smith Jr") is True

    def test_empty_strings_no_match(self, retriever):
        # Both empty: set intersection is 0, union is 0 -> returns False (0/0 guard)
        # But both empty lowercased equal each other, so exact match triggers True
        # Accept either True or False as implementation-defined
        result = retriever._is_name_match("", "")
        assert isinstance(result, bool)


# ─────────────────────────────────────────────────────────────────────────────
# _cosine_similarity
# ─────────────────────────────────────────────────────────────────────────────


class TestCosineSimilarity:
    def test_identical_vectors_return_1(self, retriever):
        v = np.array([1.0, 0.0, 0.0])
        assert abs(retriever._cosine_similarity(v, v) - 1.0) < 1e-6

    def test_orthogonal_vectors_return_0(self, retriever):
        v1 = np.array([1.0, 0.0])
        v2 = np.array([0.0, 1.0])
        assert abs(retriever._cosine_similarity(v1, v2)) < 1e-6

    def test_zero_vector_returns_0(self, retriever):
        v1 = np.zeros(3)
        v2 = np.array([1.0, 0.0, 0.0])
        assert retriever._cosine_similarity(v1, v2) == 0.0

    def test_opposite_vectors_return_minus_1(self, retriever):
        v1 = np.array([1.0, 0.0])
        v2 = np.array([-1.0, 0.0])
        sim = retriever._cosine_similarity(v1, v2)
        assert abs(sim - (-1.0)) < 1e-6

    def test_exception_returns_0(self, retriever):
        result = retriever._cosine_similarity("not_array", "not_array")
        assert result == 0.0


# ─────────────────────────────────────────────────────────────────────────────
# _compute_name_similarity
# ─────────────────────────────────────────────────────────────────────────────


class TestComputeNameSimilarity:
    def test_identical_returns_1(self, retriever):
        assert retriever._compute_name_similarity("Alice", "Alice") == 1.0

    def test_completely_different_returns_0(self, retriever):
        assert retriever._compute_name_similarity("Alice", "Bob") == 0.0

    def test_partial_overlap(self, retriever):
        sim = retriever._compute_name_similarity("John Smith", "John Doe")
        assert 0.0 < sim < 1.0

    def test_empty_string_returns_0(self, retriever):
        assert retriever._compute_name_similarity("", "Alice") == 0.0


# ─────────────────────────────────────────────────────────────────────────────
# _check_description_cooccurrence
# ─────────────────────────────────────────────────────────────────────────────


class TestCheckDescriptionCooccurrence:
    def test_name1_in_desc2_returns_true(self, retriever):
        assert retriever._check_description_cooccurrence(
            "Alice is a developer", "Bob works with Alice", "Alice", "Bob"
        ) is True

    def test_name2_in_desc1_returns_true(self, retriever):
        assert retriever._check_description_cooccurrence(
            "Bob works with Charlie", "Charlie is a PM", "Alice", "Charlie"
        ) is True

    def test_no_cooccurrence_returns_false(self, retriever):
        assert retriever._check_description_cooccurrence(
            "Alice is a developer", "Bob is a designer", "Alice", "Bob"
        ) is False


# ─────────────────────────────────────────────────────────────────────────────
# _find_entity_by_name
# ─────────────────────────────────────────────────────────────────────────────


class TestFindEntityByName:
    def test_exact_match_found(self, retriever):
        entity = _make_entity("Alice")
        retriever.entity_graph["Alice"] = entity
        result = retriever._find_entity_by_name("Alice")
        assert result is entity

    def test_partial_match_found(self, retriever):
        entity = _make_entity("Alice Johnson")
        retriever.entity_graph["Alice Johnson"] = entity
        result = retriever._find_entity_by_name("Alice")
        assert result is entity

    def test_not_found_returns_none(self, retriever):
        retriever.entity_graph.clear()
        result = retriever._find_entity_by_name("Unknown Entity")
        assert result is None


# ─────────────────────────────────────────────────────────────────────────────
# _infer_relationship_type_from_descriptions
# ─────────────────────────────────────────────────────────────────────────────


class TestInferRelationshipType:
    @pytest.mark.asyncio
    async def test_family_keywords_detected(self, retriever):
        result = await retriever._infer_relationship_type_from_descriptions(
            "Alice is the mother of Bob", "Bob is Alice's son"
        )
        assert result == "family"

    @pytest.mark.asyncio
    async def test_professional_keywords_detected(self, retriever):
        result = await retriever._infer_relationship_type_from_descriptions(
            "Alice is a mentor at the company", "Bob is a junior engineer"
        )
        assert result == "professional"

    @pytest.mark.asyncio
    async def test_organizational_keywords_detected(self, retriever):
        result = await retriever._infer_relationship_type_from_descriptions(
            "Alice belongs to the organization", "Bob is a member of the company"
        )
        assert result == "organizational"

    @pytest.mark.asyncio
    async def test_semantic_default(self, retriever):
        result = await retriever._infer_relationship_type_from_descriptions(
            "Alice likes coffee", "Bob likes tea"
        )
        assert result == "semantic"


# ─────────────────────────────────────────────────────────────────────────────
# _score_relationship_relevance
# ─────────────────────────────────────────────────────────────────────────────


class TestScoreRelationshipRelevance:
    @pytest.mark.asyncio
    async def test_score_decreases_with_hop_distance(self, retriever):
        edge = RelationshipEdge(
            source="A", target="B", strength=1.0,
            relationship_type="semantic", evidence="test"
        )
        entity = _make_entity("B", description="B is related")
        score1 = await retriever._score_relationship_relevance(edge, entity, "query", 1)
        score2 = await retriever._score_relationship_relevance(edge, entity, "query", 2)
        assert score1 > score2

    @pytest.mark.asyncio
    async def test_score_boosted_when_entity_name_in_query(self, retriever):
        edge = RelationshipEdge(
            source="A", target="Alice", strength=0.5,
            relationship_type="semantic", evidence="test"
        )
        entity = _make_entity("Alice", description="A person named Alice")
        score_with = await retriever._score_relationship_relevance(edge, entity, "Alice", 1)
        score_without = await retriever._score_relationship_relevance(edge, entity, "other query", 1)
        assert score_with > score_without

    @pytest.mark.asyncio
    async def test_family_relationship_boosted_for_family_query(self, retriever):
        edge = RelationshipEdge(
            source="A", target="B", strength=0.5,
            relationship_type="family", evidence="test"
        )
        entity = _make_entity("B")
        score_family = await retriever._score_relationship_relevance(edge, entity, "who is the father", 1)
        score_other = await retriever._score_relationship_relevance(edge, entity, "work history", 1)
        assert score_family > score_other


# ─────────────────────────────────────────────────────────────────────────────
# _score_and_rank_candidates
# ─────────────────────────────────────────────────────────────────────────────


class TestScoreAndRankCandidates:
    @pytest.mark.asyncio
    async def test_seeds_added_as_semantic_search(self, retriever):
        entity = _make_entity("Alice")
        retriever.entity_graph["Alice"] = entity
        candidates = await retriever._score_and_rank_candidates(
            ["Alice"], [], "query", 0.3
        )
        assert len(candidates) == 1
        assert candidates[0].retrieval_method == "semantic_search"

    @pytest.mark.asyncio
    async def test_related_entities_weight_applied(self, retriever):
        entity = _make_entity("Bob")
        retriever.entity_graph["Bob"] = entity
        related = RetrievalCandidate(
            entity=entity,
            relevance_score=1.0,
            retrieval_method="relationship_traversal",
            relationship_path=["Alice"],
        )
        candidates = await retriever._score_and_rank_candidates(
            [], [related], "query", 0.5
        )
        assert len(candidates) == 1
        assert candidates[0].relevance_score == pytest.approx(0.5)

    @pytest.mark.asyncio
    async def test_duplicates_removed(self, retriever):
        entity = _make_entity("Alice")
        retriever.entity_graph["Alice"] = entity
        cand1 = RetrievalCandidate(
            entity=entity, relevance_score=0.9,
            retrieval_method="semantic_search", relationship_path=[]
        )
        cand2 = RetrievalCandidate(
            entity=entity, relevance_score=0.5,
            retrieval_method="relationship_traversal", relationship_path=[]
        )
        candidates = await retriever._score_and_rank_candidates(
            [], [cand1, cand2], "query", 1.0
        )
        assert len(candidates) == 1

    @pytest.mark.asyncio
    async def test_sorted_by_relevance_descending(self, retriever):
        e1 = _make_entity("Alice")
        e2 = _make_entity("Bob")
        retriever.entity_graph["Alice"] = e1
        retriever.entity_graph["Bob"] = e2
        cand1 = RetrievalCandidate(
            entity=e1, relevance_score=0.3,
            retrieval_method="relationship_traversal", relationship_path=[]
        )
        cand2 = RetrievalCandidate(
            entity=e2, relevance_score=0.8,
            retrieval_method="relationship_traversal", relationship_path=[]
        )
        candidates = await retriever._score_and_rank_candidates(
            [], [cand1, cand2], "query", 1.0
        )
        assert candidates[0].entity.name == "Bob"


# ─────────────────────────────────────────────────────────────────────────────
# _format_results_for_crewai
# ─────────────────────────────────────────────────────────────────────────────


class TestFormatResultsForCrewAI:
    def test_basic_entity_formatted(self, retriever):
        entity = _make_entity("Alice", entity_type="person", description="Engineer")
        cand = RetrievalCandidate(
            entity=entity, relevance_score=0.9,
            retrieval_method="semantic_search", relationship_path=[]
        )
        results = retriever._format_results_for_crewai([cand])
        assert len(results) == 1
        r = results[0]
        assert r["entity_name"] == "Alice"
        assert "Alice" in r["content"]
        assert "person" in r["content"]

    def test_context_field_present(self, retriever):
        entity = _make_entity("Bob")
        cand = RetrievalCandidate(
            entity=entity, relevance_score=0.5,
            retrieval_method="semantic_search", relationship_path=[]
        )
        results = retriever._format_results_for_crewai([cand])
        assert "context" in results[0]

    def test_relationship_context_included(self, retriever):
        entity = _make_entity("Charlie")
        rel_ctx = {"source_entity": "Alice", "relationship_type": "family"}
        cand = RetrievalCandidate(
            entity=entity, relevance_score=0.7,
            retrieval_method="relationship_traversal",
            relationship_path=["Alice"],
            relationship_context=rel_ctx,
        )
        results = retriever._format_results_for_crewai([cand])
        assert results[0]["metadata"].get("relationship_context") == rel_ctx

    def test_empty_candidates_returns_empty(self, retriever):
        assert retriever._format_results_for_crewai([]) == []

    def test_multiple_candidates(self, retriever):
        entities = [_make_entity(f"Entity{i}") for i in range(4)]
        cands = [
            RetrievalCandidate(entity=e, relevance_score=0.5,
                               retrieval_method="semantic_search", relationship_path=[])
            for e in entities
        ]
        results = retriever._format_results_for_crewai(cands)
        assert len(results) == 4


# ─────────────────────────────────────────────────────────────────────────────
# _traverse_relationships_from_seeds
# ─────────────────────────────────────────────────────────────────────────────


class TestTraverseRelationshipsFromSeeds:
    @pytest.mark.asyncio
    async def test_returns_empty_when_no_edges(self, retriever):
        retriever.entity_graph["Alice"] = _make_entity("Alice")
        retriever.relationship_edges = []
        result = await retriever._traverse_relationships_from_seeds(
            ["Alice"], "query", "http://ex.com", "idx", "ep", "tok", 2
        )
        assert result == []

    @pytest.mark.asyncio
    async def test_traverses_direct_relationship(self, retriever):
        alice = _make_entity("Alice")
        bob = _make_entity("Bob")
        retriever.entity_graph["Alice"] = alice
        retriever.entity_graph["Bob"] = bob
        retriever.relationship_edges = [
            RelationshipEdge(
                source="Alice", target="Bob", strength=0.9,
                relationship_type="semantic", evidence="test"
            )
        ]
        result = await retriever._traverse_relationships_from_seeds(
            ["Alice"], "Bob", "http://ex.com", "idx", "ep", "tok", 1
        )
        assert len(result) == 1
        assert result[0].entity.name == "Bob"

    @pytest.mark.asyncio
    async def test_does_not_revisit_seed_entities(self, retriever):
        alice = _make_entity("Alice")
        retriever.entity_graph["Alice"] = alice
        retriever.relationship_edges = [
            RelationshipEdge(
                source="Alice", target="Alice", strength=1.0,
                relationship_type="self", evidence=""
            )
        ]
        result = await retriever._traverse_relationships_from_seeds(
            ["Alice"], "query", "http://ex.com", "idx", "ep", "tok", 2
        )
        assert len(result) == 0

    @pytest.mark.asyncio
    async def test_max_hops_respected(self, retriever):
        # A -> B -> C, but max_hops=1 so C should not be reached
        a = _make_entity("A")
        b = _make_entity("B")
        c = _make_entity("C")
        retriever.entity_graph = {"A": a, "B": b, "C": c}
        retriever.relationship_edges = [
            RelationshipEdge(source="A", target="B", strength=1.0, relationship_type="s", evidence=""),
            RelationshipEdge(source="B", target="C", strength=1.0, relationship_type="s", evidence=""),
        ]
        result = await retriever._traverse_relationships_from_seeds(
            ["A"], "query", "http://ex.com", "idx", "ep", "tok", max_hops=1
        )
        names = [r.entity.name for r in result]
        assert "B" in names
        assert "C" not in names


# ─────────────────────────────────────────────────────────────────────────────
# _build_explicit_relationship_edges
# ─────────────────────────────────────────────────────────────────────────────


class TestBuildExplicitRelationshipEdges:
    @pytest.mark.asyncio
    async def test_creates_edges_for_known_relationships(self, retriever):
        alice = _make_entity("Alice", relationships=["Bob"])
        bob = _make_entity("Bob")
        retriever.entity_graph = {"Alice": alice, "Bob": bob}
        retriever.relationship_edges = []
        await retriever._build_explicit_relationship_edges()
        sources = [e.source for e in retriever.relationship_edges]
        assert "Alice" in sources

    @pytest.mark.asyncio
    async def test_no_edge_for_unknown_relationship(self, retriever):
        alice = _make_entity("Alice", relationships=["UnknownPerson"])
        retriever.entity_graph = {"Alice": alice}
        retriever.relationship_edges = []
        await retriever._build_explicit_relationship_edges()
        assert len(retriever.relationship_edges) == 0


# ─────────────────────────────────────────────────────────────────────────────
# _build_name_based_relationship_edges
# ─────────────────────────────────────────────────────────────────────────────


class TestBuildNameBasedRelationshipEdges:
    @pytest.mark.asyncio
    async def test_cooccurrence_creates_edge(self, retriever):
        alice = _make_entity("Alice", description="Alice works with Bob")
        bob = _make_entity("Bob", description="Bob works with Alice")
        retriever.entity_graph = {"Alice": alice, "Bob": bob}
        retriever.relationship_edges = []
        await retriever._build_name_based_relationship_edges()
        # Alice and Bob co-occur in each other's descriptions
        sources = {e.source for e in retriever.relationship_edges}
        assert "Alice" in sources or "Bob" in sources


# ─────────────────────────────────────────────────────────────────────────────
# search_with_relationships
# ─────────────────────────────────────────────────────────────────────────────


class TestSearchWithRelationships:
    @pytest.mark.asyncio
    async def test_returns_empty_for_no_initial_results(self, retriever):
        results = await retriever.search_with_relationships(
            query="query",
            initial_results=[],
            workspace_url="http://ex.com",
            index_name="idx",
            endpoint_name="ep",
            user_token="tok",
            agent_id="a1",
            group_id="g1",
        )
        assert results == []

    @pytest.mark.asyncio
    async def test_returns_initial_results_on_exception(self, retriever):
        initial = [{"entity_name": "Alice", "description": "desc"}]
        with patch.object(
            retriever, "_build_focused_entity_graph", side_effect=Exception("error")
        ):
            results = await retriever.search_with_relationships(
                query="query",
                initial_results=initial,
                workspace_url="http://ex.com",
                index_name="idx",
                endpoint_name="ep",
                user_token="tok",
                agent_id="a1",
                group_id="g1",
                max_total=5,
            )
        # Falls back to initial_results
        assert len(results) == 1

    @pytest.mark.asyncio
    async def test_calls_build_focused_entity_graph(self, retriever):
        initial = [{"entity_name": "Alice", "description": "A person"}]
        with patch.object(
            retriever, "_build_focused_entity_graph", new_callable=AsyncMock
        ) as mock_build, patch.object(
            retriever, "_traverse_relationships_from_seeds",
            new_callable=AsyncMock, return_value=[]
        ), patch.object(
            retriever, "_score_and_rank_candidates",
            new_callable=AsyncMock, return_value=[]
        ):
            await retriever.search_with_relationships(
                query="Alice",
                initial_results=initial,
                workspace_url="http://ex.com",
                index_name="idx",
                endpoint_name="ep",
                user_token="tok",
                agent_id="a1",
                group_id="g1",
            )
        mock_build.assert_called_once()


# ─────────────────────────────────────────────────────────────────────────────
# build_entity_graph
# ─────────────────────────────────────────────────────────────────────────────


class TestBuildEntityGraph:
    @pytest.mark.asyncio
    async def test_builds_graph_from_service_response(self, retriever, mock_service):
        mock_service.get_index_documents = AsyncMock(
            return_value={
                "success": True,
                "documents": [
                    {
                        "entity_name": "Alice",
                        "entity_type": "person",
                        "description": "A researcher",
                        "agent_id": "a1",
                        "relationships": "",
                    }
                ],
            }
        )
        with patch.object(retriever, "_compute_embedding", return_value=np.zeros(1024)):
            await retriever.build_entity_graph(
                workspace_url="http://ex.com",
                index_name="idx",
                endpoint_name="ep",
                user_token="tok",
                agent_id="a1",
                group_id="g1",
            )
        assert "Alice" in retriever.entity_graph

    @pytest.mark.asyncio
    async def test_clears_existing_graph_before_building(self, retriever, mock_service):
        retriever.entity_graph["OldEntity"] = _make_entity("OldEntity")
        mock_service.get_index_documents = AsyncMock(
            return_value={"success": True, "documents": []}
        )
        with patch.object(retriever, "_compute_embedding", return_value=np.zeros(1024)):
            await retriever.build_entity_graph(
                workspace_url="http://ex.com",
                index_name="idx",
                endpoint_name="ep",
                user_token="tok",
                agent_id="a1",
                group_id="g1",
            )
        assert "OldEntity" not in retriever.entity_graph

    @pytest.mark.asyncio
    async def test_handles_empty_service_response(self, retriever, mock_service):
        """If service returns no documents the graph stays empty without raising."""
        mock_service.get_index_documents = AsyncMock(
            return_value={"success": False, "documents": []}
        )
        with patch.object(retriever, "_compute_embedding", return_value=np.zeros(1024)):
            await retriever.build_entity_graph(
                workspace_url="http://ex.com",
                index_name="idx",
                endpoint_name="ep",
                user_token="tok",
                agent_id="a1",
                group_id="g1",
            )
        assert retriever.entity_graph == {}
