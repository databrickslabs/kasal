"""GraphMemory behind the run layer's existing seams: the same recall and
persist entry points every execution path calls, the teamspace switch, and the
trace events a run's memory rows are built from."""

from src.core.events.bus import event_bus
from src.core.events.types import MemoryQueryCompletedEvent, MemorySaveCompletedEvent
from src.schemas.memory_backend import (
    MemoryBackendConfig,
    MemoryBackendType,
    MemoryTuningConfig,
)
from src.services.memory.engine import Memory
from src.services.memory.engine.graph_memory import GraphMemory
from src.services.memory.maintenance.passes import run_memory_maintenance
from src.services.memory.run.crew_memory import CrewMemoryService
from src.services.memory.run.persist import flush_memory_writes, remember_async
from src.services.memory.run.recall import build_memory_preamble
from src.services.memory.storage.graph_factory import graph_enabled, graph_store_for
from src.services.memory.storage.local import LocalStorageBackend
from src.services.memory.storage.local_graph import LocalGraphStore

from .graph_fakes import ScriptedLLM, embed, fact

TURN = "User: remember that the Atlas deadline is 30 July. Assistant: noted."
ATLAS = fact(
    "Atlas", "deadline_is", "30 July", "The Atlas deadline is 30 July.", kind="project"
)


def _memory(tmp_path, *replies, **kwargs):
    kwargs.setdefault("min_similarity", 0.3)
    return GraphMemory(
        LocalGraphStore(tmp_path / "memory.db", embedder=embed, scope="/grp"),
        llm=ScriptedLLM(*replies),
        root_scope="/grp",
        **kwargs,
    )


class TestRememberAndRecall:
    def test_a_turn_with_nothing_durable_stores_nothing(self, tmp_path):
        mem = _memory(tmp_path, [])
        assert mem.remember(TURN) is None
        assert mem.storage.count() == 0

    def test_recall_returns_facts_as_semantic_records_with_provenance(self, tmp_path):
        mem = _memory(tmp_path, [ATLAS])
        mem.remember(TURN, source="chat", metadata={"execution_id": "run-1"})
        (record,) = mem.recall("When is the Atlas deadline?")
        assert record.content == "The Atlas deadline is 30 July."
        assert (record.kind, record.source, record.scope) == (
            "semantic",
            "chat",
            "/grp",
        )
        assert record.metadata["execution_id"] == "run-1"
        assert record.metadata["relation"] == "deadline_is"
        assert record.metadata["similarity"] >= 0.3

    def test_recall_makes_no_model_call(self, tmp_path):
        mem = _memory(tmp_path, [ATLAS])
        mem.remember(TURN)
        mem.recall("When is the Atlas deadline?")
        assert mem.llm.calls == 1  # the write's extraction, nothing for the read

    def test_without_an_llm_nothing_is_written(self, tmp_path):
        mem = _memory(tmp_path)
        mem.llm = None
        assert mem.remember(TURN) is None

    def test_read_only_memory_does_not_write(self, tmp_path):
        mem = _memory(tmp_path, [ATLAS], read_only=True)
        assert mem.remember(TURN) is None and mem.llm.calls == 0

    def test_events_carry_the_stored_fact_and_the_recalled_ones(self, tmp_path):
        mem = _memory(tmp_path, [ATLAS])
        seen = []
        with event_bus.scoped_handlers():

            @event_bus.on(MemorySaveCompletedEvent)
            @event_bus.on(MemoryQueryCompletedEvent)
            def _collect(source, event):
                if source is mem:
                    seen.append(event)

            saved = mem.remember(TURN)
            mem.recall("When is the Atlas deadline?")
        save, query = seen
        assert save.record_id == saved.id and save.value == saved.content
        assert [r.id for r in query.results] == [saved.id]


class TestWriteDiscipline:
    """What the first real runs showed: one exchange arriving through two write
    paths, and a preference re-extracted in new words on every later turn."""

    def test_the_same_exchange_arriving_twice_costs_one_extraction(self, tmp_path):
        mem = _memory(tmp_path, [ATLAS], [ATLAS])
        mem.remember(TURN, source="chat")
        again = GraphMemory(mem.storage, llm=mem.llm, root_scope="/grp")
        assert again.remember(TURN + " ", source="chat") is None
        assert mem.llm.calls == 1 and mem.storage.count() == 1

    def test_another_teamspace_saying_the_same_thing_is_not_a_duplicate(self, tmp_path):
        mem = _memory(tmp_path, [ATLAS], [ATLAS])
        mem.remember(TURN)
        other = GraphMemory(
            LocalGraphStore(tmp_path / "other.db", embedder=embed, scope="/other"),
            llm=mem.llm,
            root_scope="/other",
        )
        assert other.remember(TURN) is not None

    def test_the_extractor_is_shown_what_memory_already_holds(self, tmp_path):
        mem = _memory(tmp_path, [ATLAS], [])
        seen = []
        original = mem.llm.call
        mem.llm.call = lambda messages: (seen.append(messages), original(messages))[1]
        mem.remember(TURN)
        mem.remember("User: what is the Atlas deadline again? Assistant: 30 July.")
        first, second = (messages[1]["content"] for messages in seen)
        assert "ALREADY IN MEMORY" not in first
        assert "- Atlas | deadline_is | 30 July" in second

    def test_the_prompt_takes_facts_from_the_user_only(self):
        from src.services.memory.engine.extract import _SYSTEM_PROMPT

        assert "ONLY from what the User says" in _SYSTEM_PROMPT
        assert "reuse that fact's subject and relation exactly" in _SYSTEM_PROMPT

    def test_save_hooks_receive_the_stored_facts(self, tmp_path):
        """How a chat run's trace gets its Memory Write rows."""
        mem = _memory(tmp_path, [ATLAS], [])
        landed = []
        mem.add_save_hook(landed.extend)
        mem.remember(TURN)
        mem.remember("User: thanks, that is all for today. Assistant: welcome.")
        assert [r.content for r in landed] == ["The Atlas deadline is 30 July."]


NEWS = "User: gather latest swiss news from today. Assistant: Swiss news roundup ..."
NEWS_NOTE = "Gathered a Swiss news roundup; top story was the Federal Court vote."


class TestRunNotes:
    """The episodic layer: a run that states nothing about the user still
    leaves one dated sentence saying what it was asked and produced."""

    def test_a_research_run_stores_a_dated_note_and_no_facts(self, tmp_path):
        mem = _memory(tmp_path, {"facts": [], "note": NEWS_NOTE})
        record = mem.remember(NEWS, source="chat", metadata={"execution_id": "run-9"})
        assert record.kind == "episodic"
        assert record.content.endswith(f": {NEWS_NOTE}")
        assert record.content[:2].isdigit()  # dated by the code, not the model
        assert record.metadata["execution_id"] == "run-9"

    def test_a_note_is_recalled_like_a_fact(self, tmp_path):
        mem = _memory(tmp_path, {"facts": [], "note": NEWS_NOTE})
        mem.remember(NEWS)
        (hit,) = mem.recall("Swiss news roundup Federal Court vote top story")
        assert NEWS_NOTE in hit.content and hit.kind == "episodic"

    def test_a_weakly_matching_note_is_not_recalled(self, tmp_path):
        """What the first real run showed: a Nestlé revenue request recalled a
        Lebanese-news note at 0.53. A note needs more than a fact does."""
        mem = _memory(tmp_path, {"facts": [], "note": NEWS_NOTE})
        mem.remember(NEWS)
        mem.storage.strict_min_similarity = 0.99
        assert mem.recall("Swiss news roundup Federal Court vote top story") == []

    def test_notes_do_not_ride_along_on_the_user_entity(self, tmp_path):
        """Notes hang off ``user``; the one-hop expansion must not follow them."""
        works = fact("user", "works_at", "Acme", "The user works at Acme.")
        mem = _memory(
            tmp_path,
            {"facts": [works], "note": ""},
            {"facts": [], "note": NEWS_NOTE},
            min_similarity=0.5,  # the note does not match this query by itself
        )
        mem.remember("User: remember that I work at Acme. Assistant: noted.")
        mem.remember(NEWS)
        contents = [r.content for r in mem.recall("the user works at Acme", limit=6)]
        assert contents == ["The user works at Acme."]

    def test_a_matched_note_brings_no_user_facts_with_it(self, tmp_path):
        works = fact("user", "works_at", "Acme", "The user works at Acme.")
        mem = _memory(
            tmp_path, {"facts": [works], "note": ""}, {"facts": [], "note": NEWS_NOTE}
        )
        mem.remember("User: remember that I work at Acme. Assistant: noted.")
        mem.remember(NEWS)
        hits = mem.recall("Swiss news roundup Federal Court vote top story", limit=6)
        assert [NEWS_NOTE in r.content for r in hits] == [True]

    def test_an_expired_note_is_no_longer_recalled(self, tmp_path):
        mem = _memory(tmp_path, {"facts": [], "note": NEWS_NOTE})
        mem.remember(NEWS)
        mem.storage.note_retention_days = 0.0
        assert mem.recall("Swiss news roundup Federal Court vote top story") == []
        assert mem.storage.count() == 1  # kept as history; only recall stops

    def test_notes_are_not_shown_to_the_extractor_as_known_facts(self, tmp_path):
        mem = _memory(tmp_path, {"facts": [], "note": NEWS_NOTE}, [])
        seen = []
        original = mem.llm.call
        mem.llm.call = lambda messages: (seen.append(messages), original(messages))[1]
        mem.remember(NEWS)
        mem.remember("User: and what about the weather there today? Assistant: Rain.")
        assert "run_note" not in seen[1][1]["content"]

    def test_the_model_cannot_use_the_reserved_relation(self):
        from src.services.memory.engine.extract import parse_facts, parse_note

        payload = {"facts": [fact("user", "run_note", "x", "Sneaky.")], "note": 7}
        assert parse_facts(payload) == [] and parse_note(payload) is None

    def test_retention_follows_the_existing_knob(self, tmp_path, monkeypatch):
        mem = TestSwitch()._build(
            tmp_path, monkeypatch, graph_memory=True, episodic_ttl_days=30
        )
        assert mem.storage.note_retention_days == 30


REVENUE = {
    **fact(
        "Nestlé",
        "revenue_fy2025",
        "CHF 89.5 bn",
        "Nestlé's FY2025 sales were CHF 89.5 bn.",
        single=True,
        kind="organization",
    ),
    "kind": "finding",
}


class TestFindings:
    """A run that establishes something lasting about a named entity stores it
    as a fact — "provide me the revenue of Nestlé in 2025" must leave Nestlé
    and its revenue in the graph, not only a sentence saying it was asked."""

    def test_a_finding_is_stored_as_an_entity_fact_marked_as_found(self, tmp_path):
        mem = _memory(tmp_path, {"facts": [REVENUE], "note": ""})
        record = mem.remember(
            "User: provide me the revenue of nestle in 2025. Assistant: CHF 89.5 bn."
        )
        assert record.kind == "semantic"
        assert (record.metadata["subject"], record.metadata["relation"]) == (
            "Nestlé",
            "revenue_fy2025",
        )
        assert record.metadata["kind"] == "finding"
        assert [e.name for e in mem.storage.list_entities()] == ["Nestlé"]

    def test_what_the_user_states_is_marked_as_theirs(self, tmp_path):
        mem = _memory(tmp_path, [ATLAS])
        assert mem.remember(TURN).metadata["kind"] == "semantic"

    def test_no_note_when_the_run_left_findings(self, tmp_path):
        mem = _memory(
            tmp_path,
            {"facts": [REVENUE], "note": "Nestlé FY2025 revenue: CHF 89.5 bn."},
        )
        mem.remember(
            "User: provide me the revenue of nestle in 2025. Assistant: CHF 89.5 bn."
        )
        assert [f.relation for f in mem.storage.list_facts()] == ["revenue_fy2025"]

    def test_a_corrected_figure_replaces_the_old_one(self, tmp_path):
        newer = {
            **REVENUE,
            "object": "CHF 89.49 bn",
            "statement": "Nestlé's FY2025 sales were CHF 89.49 bn.",
        }
        mem = _memory(tmp_path, {"facts": [REVENUE]}, {"facts": [newer]})
        mem.remember(
            "User: provide me the revenue of nestle in 2025. Assistant: CHF 89.5 bn."
        )
        mem.remember(
            "User: what exactly was nestle's revenue in 2025? Assistant: CHF 89.49 bn."
        )
        assert [f.object for f in mem.storage.list_facts()] == ["CHF 89.49 bn"]

    def test_a_finding_can_never_be_about_the_user(self):
        from src.services.memory.engine.extract import parse_facts

        sneaky = {
            **fact("user", "works_at", "Acme", "The user works at Acme."),
            "kind": "finding",
        }
        (parsed,) = parse_facts({"facts": [sneaky]})
        assert parsed.kind == "semantic"

    def test_a_comparison_is_not_cut_short(self):
        """Neither Graphiti nor Mem0 caps facts per write; a cap of 8 cost
        products their link to the company that sells them."""
        from src.services.memory.engine.extract import parse_facts

        many = [
            {
                **fact(
                    "Acme",
                    f"metric_{i}",
                    str(i),
                    f"Metric {i} is {i}.",
                    kind="organization",
                ),
                "kind": "finding",
            }
            for i in range(30)
        ]
        assert len(parse_facts({"facts": many})) == 30

    def test_one_embedding_request_per_write(self, tmp_path):
        mem = _memory(tmp_path, {"facts": [REVENUE, ATLAS]})
        calls = []
        original = mem.storage.embedder
        mem.storage.embedder = lambda texts: (
            calls.append(len(texts)),
            original(texts),
        )[1]
        mem.remember(
            "User: Atlas is due 30 July; nestle 2025 revenue? Assistant: 89.5 bn."
        )
        assert calls == [2] and mem.storage.count() == 2

    def test_a_write_survives_an_embedder_that_cannot_batch(self, tmp_path):
        mem = _memory(tmp_path, {"facts": [REVENUE, ATLAS]})
        original = mem.storage.embedder

        def one_at_a_time(texts):
            if len(texts) > 1:
                raise RuntimeError("batch not supported")
            return original(texts)

        mem.storage.embedder = one_at_a_time
        mem.remember(
            "User: Atlas is due 30 July; nestle 2025 revenue? Assistant: 89.5 bn."
        )
        assert mem.storage.count() == 2


TABLES = {
    **fact(
        "user",
        "preferred_output_format",
        "tables",
        "The user wants answers as tables.",
        single=True,
    ),
    "kind": "procedural",
    "standing_evidence": "always",
}


class TestKinds:
    """Kind decides where an item may come from, when it is recalled and how
    long it lives — one policy per kind."""

    def test_a_standing_preference_is_recalled_for_any_request(self, tmp_path):
        mem = _memory(tmp_path, [TABLES])
        mem.remember("User: always answer me in tables please. Assistant: will do.")
        (record,) = mem.recall("gather latest swiss news from today")
        assert record.kind == "procedural" and record.metadata["kind"] == "procedural"
        assert "similarity" not in record.metadata  # included, not matched

    def test_standing_preferences_come_before_what_matched(self, tmp_path):
        mem = _memory(tmp_path, [TABLES, ATLAS])
        mem.remember("User: always use tables; Atlas is due 30 July. Assistant: ok.")
        kinds = [r.kind for r in mem.recall("When is the Atlas deadline?")]
        assert kinds == ["procedural", "semantic"]

    def test_a_changed_preference_replaces_the_old_one(self, tmp_path):
        prose = {
            **TABLES,
            "object": "prose",
            "statement": "The user wants answers as prose.",
            "standing_evidence": "from now on",
        }
        mem = _memory(tmp_path, [TABLES], [prose])
        mem.remember("User: always answer me in tables please. Assistant: will do.")
        mem.remember("User: actually switch to prose from now on. Assistant: ok.")
        assert [r.content for r in mem.recall("anything at all")] == [
            "The user wants answers as prose."
        ]

    def test_an_instruction_for_one_request_is_not_a_standing_rule(self, tmp_path):
        """ "check only from postgres", said about one lookup, became "PostgreSQL
        only, no web lookups" for every run — and a later news request refused
        to search the web."""
        one_off = {
            **fact(
                "user", "preferred_data_source", "PostgreSQL only", "Postgres only."
            ),
            "kind": "procedural",
        }
        turn = "User: look up Dana Keller, check only from postgres. Assistant: ok."
        for evidence in ("", "always", "from now on"):  # none given, or not said
            mem = _memory(
                tmp_path / (evidence or "none"),
                [{**one_off, "standing_evidence": evidence}],
            )
            assert mem.remember(turn) is None
            assert mem.recall("gather latest swiss news from today") == []

    def test_a_finding_is_recalled_on_the_same_floor_as_a_user_fact(self, tmp_path):
        """Only run notes need the stricter match: right campaign findings for
        "what was the most effective marketing campaign" scored 0.53-0.60."""
        mem = _memory(tmp_path, {"facts": [REVENUE]}, min_similarity=0.0)
        mem.remember("User: provide me the revenue of nestle in 2025. Assistant: 89.5.")
        mem.storage.strict_min_similarity = 0.99
        assert len(mem.recall("Nestlé FY2025 sales")) == 1

    def test_only_the_user_can_hold_a_standing_preference(self):
        from src.services.memory.engine.extract import parse_facts

        not_user = {
            **fact(
                "Acme", "prefers", "tables", "Acme likes tables.", kind="organization"
            ),
            "kind": "procedural",
        }
        (parsed,) = parse_facts({"facts": [not_user]})
        assert parsed.kind == "semantic"

    def test_a_note_attaches_to_what_it_is_about(self, tmp_path):
        reply = {
            "facts": [],
            "note": NEWS_NOTE,
            "note_subject": "Switzerland",
            "note_subject_type": "place",
        }
        mem = _memory(tmp_path, reply)
        record = mem.remember(NEWS)
        assert record.metadata["subject"] == "Switzerland"
        assert record.metadata["kind"] == "episodic"
        assert [(e.name, e.type) for e in mem.storage.list_entities()] == [
            ("Switzerland", "place")
        ]

    def test_a_store_created_before_kinds_reads_as_semantic(self, tmp_path):
        import sqlite3

        mem = _memory(tmp_path, [ATLAS])
        mem.remember(TURN)
        mem.storage.close()
        raw = sqlite3.connect(tmp_path / "memory.db")
        raw.execute("ALTER TABLE graph_facts DROP COLUMN kind")
        raw.commit()
        raw.close()
        reopened = LocalGraphStore(tmp_path / "memory.db", embedder=embed, scope="/grp")
        assert [f.kind for f in reopened.list_facts()] == ["semantic"]


class TestRunLayerSeams:
    def test_persist_then_preamble_round_trip(self, tmp_path):
        mem = _memory(tmp_path, [ATLAS])
        remember_async(mem, TURN, source="chat")
        assert flush_memory_writes(timeout=10) == 0
        block = build_memory_preamble(mem, "When is the Atlas deadline?")
        assert "- [the user said] The Atlas deadline is 30 July." in block

    def test_the_block_says_what_recalled_facts_are_for(self, tmp_path):
        """A run handed Nestlé's revenue from memory still searched the web for
        it: the block called it background and gave the finding no date."""
        mem = _memory(tmp_path, {"facts": [REVENUE, TABLES]}, min_similarity=0.0)
        mem.storage.strict_min_similarity = 0.0
        mem.remember("User: nestle 2025 revenue; always in tables. Assistant: 89.5 bn.")
        block = build_memory_preamble(mem, "Nestlé FY2025 sales")
        assert block.startswith("What memory already holds for this request")
        assert "call a tool only for what it does not cover" in block
        assert "- [always applies] The user wants answers as tables." in block
        assert "- [found by a run on " in block and "CHF 89.5 bn" in block

    def test_a_recall_is_one_row_in_the_trace_not_two(self, tmp_path):
        mem = _memory(tmp_path, [ATLAS])
        mem.remember(TURN)
        stages = []
        with event_bus.scoped_handlers():

            @event_bus.on(MemoryQueryCompletedEvent)
            def _collect(source, event):
                if source is mem:
                    stages.append((event.stage, len(event.results)))

            build_memory_preamble(mem, "When is the Atlas deadline?")
        assert stages == [("selected", 1)]

    def test_a_direct_recall_is_reported_as_what_the_run_read(self, tmp_path):
        """The CrewAI engine's agent calls recall itself, with no recall block
        to report the selection. Reported as a search, Recalled stayed empty."""
        mem = _memory(tmp_path, [ATLAS])
        mem.remember(TURN)
        stages = []
        with event_bus.scoped_handlers():

            @event_bus.on(MemoryQueryCompletedEvent)
            def _collect(source, event):
                if source is mem:
                    stages.append((event.stage, [r.id for r in event.results]))

            (recalled,) = mem.recall("When is the Atlas deadline?")
        assert stages == [("selected", [recalled.id])]

    def test_maintenance_leaves_a_fact_store_alone(self, tmp_path):
        mem = _memory(tmp_path, [ATLAS])
        mem.remember(TURN)
        stats = run_memory_maintenance(mem)
        assert set(stats.values()) == {0} and mem.storage.count() == 1


class TestSwitch:
    def _config(self, **tuning):
        return MemoryBackendConfig(
            backend_type=MemoryBackendType.DEFAULT,
            cognitive_config=MemoryTuningConfig(**tuning),
        )

    def test_off_by_default(self):
        assert graph_enabled(self._config()) is False
        assert (
            graph_enabled(MemoryBackendConfig(backend_type=MemoryBackendType.DEFAULT))
            is False
        )
        assert graph_enabled(self._config(graph_memory=True)) is True

    def _build(self, tmp_path, monkeypatch, **tuning):
        service = CrewMemoryService({"group_id": "grp", "session_id": "s1"})
        monkeypatch.setattr(
            service,
            "_create_local_storage",
            lambda *a, **k: LocalStorageBackend(tmp_path / "memory.db", embedder=embed),
        )
        kwargs = service.configure_crew_memory_components(
            {"agents": [], "embedder": embed},
            self._config(**tuning),
            None,
            "grp_crew",
            memory_llm_override=ScriptedLLM(),
        )
        return kwargs["memory"]

    def test_switch_on_builds_graph_memory_in_the_same_file(
        self, tmp_path, monkeypatch
    ):
        mem = self._build(
            tmp_path, monkeypatch, graph_memory=True, relevance_threshold=0.4
        )
        assert isinstance(mem, GraphMemory)
        assert mem.root_scope == "/grp" and mem.min_similarity == 0.4
        assert mem.storage.db_path == tmp_path / "memory.db"
        assert (mem.storage.scope, mem.storage.session_id) == ("/grp", "s1")

    def test_switch_off_builds_record_memory(self, tmp_path, monkeypatch):
        assert isinstance(self._build(tmp_path, monkeypatch), Memory)

    def test_lakebase_backend_gets_the_lakebase_graph_store(self):
        from src.services.memory.storage.lakebase import LakebaseStorageBackend
        from src.services.memory.storage.lakebase_graph import LakebaseGraphStore

        backend = LakebaseStorageBackend(
            table_name="crew_memory",
            crew_id="grp_crew",
            group_id="grp",
            session_id="s1",
            embedder=embed,
            embedding_dimension=64,
            instance_name="inst",
            workspace_wide=False,
        )
        store = graph_store_for(backend, "/grp")
        assert isinstance(store, LakebaseGraphStore)
        assert (store.facts, store.entities) == (
            "crew_memory_facts",
            "crew_memory_entities",
        )
        assert (store.group_id, store.session_id) == ("grp", "s1")
        assert store.workspace_wide is False and store.embedding_dimension == 64
        # No second place that reaches Lakebase: the record backend's opener.
        assert store._open_session == backend.open_session
