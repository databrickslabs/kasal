"""LakebaseGraphStore's generated SQL. The store runs against Postgres, so
these pin what cannot be seen without one: every statement is tenant-scoped,
reads see current facts only, and the tables are created before first use.
(The behaviour itself matches ``test_local_graph_store.py`` by construction —
both stores rank seeds with the shared ``rank_hits``.)"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from src.services.memory.engine.extract import parse_facts
from src.services.memory.storage import lakebase_graph
from src.services.memory.storage.lakebase_graph import LakebaseGraphStore, graph_ddl

from .graph_fakes import fact


def _ctx(session):
    ctx = AsyncMock()
    ctx.__aenter__ = AsyncMock(return_value=session)
    ctx.__aexit__ = AsyncMock(return_value=False)
    return ctx


def _store(session=None, **kwargs):
    """A store handed its session opener, as ``graph_factory`` hands it the
    record backend's."""
    session = session if session is not None else AsyncMock()
    kwargs.setdefault("open_session", lambda: _ctx(session))
    return LakebaseGraphStore(
        table_name="crew_memory",
        group_id="grp",
        session_id="s1",
        embedding_dimension=4,
        **kwargs,
    )


def _result(rows=(), rowcount=1):
    return MagicMock(
        fetchall=MagicMock(return_value=list(rows)),
        fetchone=MagicMock(return_value=rows[0] if rows else None),
        rowcount=rowcount,
        scalar=MagicMock(return_value=0),
    )


async def _run(call, results=None, **store_kwargs):
    """Run ``call(store)`` on a mocked session; return its (sql, params) calls."""
    lakebase_graph.reset_graph_schema_cache()
    lakebase_graph._ready.add("crew_memory_facts")  # DDL covered separately
    session = AsyncMock()
    session.execute = AsyncMock(side_effect=results or (lambda *a, **k: _result()))
    value = await call(_store(session, **store_kwargs))
    calls = [
        (str(c[0][0]), c[0][1] if len(c[0]) > 1 else {})
        for c in session.execute.call_args_list
    ]
    return value, calls


def _fact(**kwargs):
    (parsed,) = parse_facts(
        {
            "facts": [
                fact(
                    "Atlas",
                    "deadline_is",
                    "30 July",
                    "Atlas is due 30 July.",
                    kind="project",
                    **kwargs,
                )
            ]
        }
    )
    return parsed


class TestDdl:
    def test_tables_indexes_and_the_one_current_copy_rule(self):
        ddl = "\n".join(graph_ddl("crew_memory", 1024))
        assert "CREATE TABLE IF NOT EXISTS crew_memory_entities" in ddl
        assert "ADD COLUMN IF NOT EXISTS kind TEXT NOT NULL" in ddl
        assert "embedding vector(1024)" in ddl
        assert "USING hnsw (embedding vector_cosine_ops)" in ddl
        assert "USING gin (tsv)" in ddl
        assert (
            "(group_id, subject_id, relation, object_key) WHERE valid_to IS NULL" in ddl
        )

    def test_table_name_is_validated_before_any_sql(self):
        with pytest.raises(ValueError):
            LakebaseGraphStore(
                table_name="m; DROP TABLE x", group_id="grp", open_session=_ctx
            )

    @pytest.mark.asyncio
    async def test_tables_are_created_once_before_first_use(self):
        lakebase_graph.reset_graph_schema_cache()
        session = AsyncMock()
        session.execute = AsyncMock(return_value=_result())
        await _store(session).acount()
        first = session.execute.call_count
        await _store(session).acount()
        assert first == len(graph_ddl("crew_memory", 4)) + 1
        assert session.execute.call_count == first + 1

    @pytest.mark.asyncio
    async def test_failed_creation_is_retried_next_time(self):
        lakebase_graph.reset_graph_schema_cache()

        def refuse():
            raise RuntimeError("no CREATE privilege")

        await _store(open_session=refuse)._ensure_tables()
        assert "crew_memory_facts" not in lakebase_graph._ready


class TestWriteSql:
    @pytest.mark.asyncio
    async def test_every_write_statement_is_tenant_scoped(self):
        stored, calls = await _run(
            lambda store: store.aadd_fact(
                _fact(), [0.1, 0.2, 0.3, 0.4], "chat", {"execution_id": "r1"}
            ),
            results=[_result(), _result([("ent-1",)]), _result(), _result()],
        )
        assert stored is not None and stored.subject_id == "ent-1"
        assert all(params.get("group_id") == "grp" for _, params in calls)
        insert_sql, insert_params = calls[-1]
        assert "ON CONFLICT DO NOTHING" in insert_sql
        assert insert_params["object_key"] == "30 july"
        assert insert_params["embedding"] == "[0.1,0.2,0.3,0.4]"

    @pytest.mark.asyncio
    async def test_a_fact_already_current_is_not_inserted(self):
        stored, calls = await _run(
            lambda store: store.aadd_fact(_fact(), [0.1, 0.2, 0.3, 0.4]),
            results=[
                _result(),
                _result([("ent-1",)]),
                _result([("f-old", "30 july", "{}")]),
            ],
        )
        assert stored is None
        assert not any("INSERT INTO crew_memory_facts" in sql for sql, _ in calls)

    @pytest.mark.asyncio
    async def test_single_valued_fact_closes_the_value_it_replaces_first(self):
        """The close precedes the insert: a correction keeps the old key, and
        only one current copy of a key may exist."""
        stored, calls = await _run(
            lambda store: store.aadd_fact(_fact(), [0.1, 0.2, 0.3, 0.4]),
            results=[
                _result(),
                _result([("ent-1",)]),
                _result([("f-old", "15 june", "{}")]),
                _result(),
                _result(),
            ],
        )
        (update_sql, update_params), (insert_sql, _) = calls[-2], calls[-1]
        assert "SET valid_to = NOW(), superseded_by = :winner" in update_sql
        assert update_params == {
            "winner": stored.id,
            "group_id": "grp",
            "ids": ["f-old"],
        }
        assert insert_sql.startswith("INSERT INTO crew_memory_facts")

    @pytest.mark.asyncio
    async def test_a_corrected_measure_replaces_the_fact_it_corrects(self):
        old = '{"qualifiers": {"roas": "4.10x"}}'
        stored, calls = await _run(
            lambda store: store.aadd_fact(
                _fact(single=False),
                [0.1, 0.2, 0.3, 0.4],
                metadata={"qualifiers": {"roas": "4.36x"}},
            ),
            results=[
                _result(),
                _result([("ent-1",)]),
                _result([("f-old", "30 july", old)]),
                _result(),
                _result(),
            ],
        )
        assert stored is not None
        assert calls[-2][1]["ids"] == ["f-old"]

    @pytest.mark.asyncio
    async def test_losing_an_insert_race_rolls_the_close_back(self):
        lakebase_graph.reset_graph_schema_cache()
        lakebase_graph._ready.add("crew_memory_facts")
        session = AsyncMock()
        session.execute = AsyncMock(
            side_effect=[
                _result(),
                _result([("ent-1",)]),
                _result([("f-old", "15 june", "{}")]),
                _result(),
                _result(rowcount=0),
            ]
        )
        ctx = _ctx(session)
        stored = await _store(session, open_session=lambda: ctx).aadd_fact(
            _fact(), [0.1, 0.2, 0.3, 0.4]
        )
        assert stored is None
        # The session saw an exception, so its owner rolls the close back.
        exc_type = ctx.__aexit__.call_args[0][0]
        assert exc_type is not None and exc_type.__name__ == "_LostRace"


class TestReadSql:
    @pytest.mark.asyncio
    async def test_search_reads_current_facts_of_this_tenant_only(self):
        _, calls = await _run(
            lambda store: store.asearch("Atlas deadline", [0.1, 0.2, 0.3, 0.4])
        )
        vector_sql, vector_params = calls[0]
        keyword_sql, keyword_params = calls[1]
        for sql in (vector_sql, keyword_sql):
            assert "f.group_id = :group_id AND f.valid_to IS NULL" in sql
            # Run notes past their retention period are not recalled.
            assert "NOT (f.kind = :episodic AND f.created_at < :note_cutoff)" in sql
        assert vector_params["episodic"] == "episodic"
        assert "f.embedding <=> CAST(:query_embedding AS vector)" in vector_sql
        assert keyword_params["tsquery"] == "atlas | deadline"
        assert vector_params["group_id"] == keyword_params["group_id"] == "grp"

    @pytest.mark.asyncio
    async def test_session_only_scope_adds_the_session_filter(self):
        _, calls = await _run(
            lambda store: store.asearch("Atlas deadline", [0.1, 0.2, 0.3, 0.4]),
            workspace_wide=False,
        )
        assert "f.session_id = :session_id" in calls[0][0]
        assert calls[0][1]["session_id"] == "s1"

    @pytest.mark.asyncio
    async def test_seeds_below_the_floor_stop_the_search(self):
        hits, calls = await _run(
            lambda store: store.asearch("Atlas deadline", [0.1, 0.2, 0.3, 0.4], 5, 0.5),
            results=[_result([("f1", 0.2, "semantic")]), _result([("f1",)])],
        )
        assert hits == [] and len(calls) == 2  # no fetch, no hop

    @pytest.mark.asyncio
    async def test_a_run_note_needs_a_closer_match_than_a_fact(self):
        """0.55 seeds a fact but not a note: a note from an unrelated run is a
        false memory of what happened."""
        _, calls = await _run(
            lambda store: store.asearch("Atlas deadline", [0.1, 0.2, 0.3, 0.4], 5, 0.5),
            results=[
                _result(
                    [
                        ("note", 0.55, "episodic"),
                        ("found", 0.55, "finding"),
                        ("fact", 0.55, "semantic"),
                    ]
                ),
                _result(),
                _result(),
            ],
        )
        assert calls[2][1]["ids"] == ["found", "fact"]

    @pytest.mark.asyncio
    async def test_standing_facts_are_this_tenants_procedural_facts(self):
        _, calls = await _run(lambda store: store.astanding_facts())
        sql, params = calls[0]
        assert "f.kind = :procedural" in sql and "f.valid_to IS NULL" in sql
        assert (params["procedural"], params["group_id"]) == ("procedural", "grp")

    @pytest.mark.asyncio
    async def test_retiring_a_fact_closes_it_for_this_tenant_only(self):
        done, calls = await _run(lambda store: store.aretire_fact("f1"))
        sql, params = calls[0]
        assert done is True
        assert "SET valid_to = NOW()" in sql and "group_id = :group_id" in sql
        assert params == {"id": "f1", "group_id": "grp"}

    @pytest.mark.asyncio
    async def test_reset_deletes_only_this_tenant(self):
        _, calls = await _run(lambda store: store.areset())
        assert [sql for sql, _ in calls] == [
            "DELETE FROM crew_memory_facts WHERE group_id = :group_id",
            "DELETE FROM crew_memory_entities WHERE group_id = :group_id",
        ]
