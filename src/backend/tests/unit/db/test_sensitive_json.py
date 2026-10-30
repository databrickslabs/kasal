"""SensitiveJSON: the ORM holds plaintext, the database only ever ciphertext.

Each test writes through the ORM (or a Core UPDATE, as the repositories do) and
reads the stored value back with raw SQL, which bypasses the column type.
"""

import json
import uuid
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine, text, update
from sqlalchemy.orm import Session

import src.db.all_models  # noqa: F401  registers every table on Base
from src.db.base import Base
from src.db.sensitive_json import SensitiveJSON
from src.models.agent import Agent
from src.models.crew import Crew
from src.models.group_tool import GroupTool
from src.models.task import Task
from src.models.tool import Tool
from src.utils.encryption_utils import EncryptionUtils

SECRET = "sp-secret-value-123"
CONFIG = {"PowerBI": {"client_secret": SECRET, "tenant_id": "tenant-1"}}


@pytest.fixture
def engine(isolated_encryption_keys):
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)
    yield engine
    engine.dispose()


def _raw(engine, table, column, row_id):
    with engine.connect() as conn:
        value = conn.execute(
            text(f"SELECT {column} FROM {table} WHERE id = :id"), {"id": row_id}
        ).scalar_one()
    return json.loads(value) if isinstance(value, str) else value


def _write_raw(engine, table, column, row_id, value):
    with engine.begin() as conn:
        conn.execute(
            text(f"UPDATE {table} SET {column} = :v WHERE id = :id"),
            {"v": json.dumps(value), "id": row_id},
        )


def _tool(session):
    tool = Tool(title="Power BI", description="d", icon="i", config={})
    session.add(tool)
    session.flush()
    return tool


def _rows(session):
    """One row per encrypted column: (table, column, row id, model, attribute)."""
    tool = _tool(session)
    agent = Agent(id="a1", name="a", role="r", goal="g", backstory="b", tools=[])
    crew = Crew(id=uuid.uuid4(), name="c")
    task = Task(id="t1", name="t", description="d", expected_output="o", tools=[])
    group_tool = GroupTool(tool_id=tool.id, group_id="ws", enabled=True)
    session.add_all([agent, crew, task, group_tool])
    session.flush()
    return [
        ("agents", "tool_configs", agent.id, agent),
        ("crews", "tool_configs", crew.id.hex, crew),
        ("crews", "nodes", crew.id.hex, crew),
        ("tasks", "tool_configs", task.id, task),
        ("tasks", "callback_config", task.id, task),
        ("tasks", "config", task.id, task),
        ("tools", "config", tool.id, tool),
        ("group_tools", "config", group_tool.id, group_tool),
    ]


COLUMNS = [
    "agents.tool_configs",
    "crews.tool_configs",
    "crews.nodes",
    "tasks.tool_configs",
    "tasks.callback_config",
    "tasks.config",
    "tools.config",
    "group_tools.config",
]


def _row_for(session, name):
    for table, column, row_id, obj in _rows(session):
        if f"{table}.{column}" == name:
            return table, column, row_id, obj
    raise AssertionError(name)


def _value_for(column):
    # crews.nodes is a list at the root.
    if column == "nodes":
        return [{"id": "task-1", "data": {"tool_configs": CONFIG}}]
    return CONFIG


def _secret_of(column, value):
    if column == "nodes":
        return value[0]["data"]["tool_configs"]["PowerBI"]["client_secret"]
    return value["PowerBI"]["client_secret"]


@pytest.mark.parametrize("name", COLUMNS)
class TestEveryColumn:
    def test_the_row_holds_ciphertext(self, engine, name):
        with Session(engine) as session:
            table, column, row_id, obj = _row_for(session, name)
            setattr(obj, column, _value_for(column))
            session.commit()
        stored = _raw(engine, table, column, row_id)
        assert _secret_of(column, stored).startswith("ENC:")
        assert SECRET not in json.dumps(stored)

    def test_the_orm_reads_plaintext(self, engine, name):
        with Session(engine) as session:
            table, column, row_id, obj = _row_for(session, name)
            setattr(obj, column, _value_for(column))
            session.commit()
            model, pk = type(obj), obj.id
        with Session(engine) as session:
            obj = session.get(model, pk)
            assert getattr(obj, column) == _value_for(column)

    def test_a_load_and_commit_never_rewrites_the_row(self, engine, name):
        with Session(engine) as session:
            table, column, row_id, obj = _row_for(session, name)
            setattr(obj, column, _value_for(column))
            session.commit()
            model, pk = type(obj), obj.id
        before = _raw(engine, table, column, row_id)
        with Session(engine) as session:
            obj = session.get(model, pk)
            # An equal value assigned back is not a change. Ciphertext is
            # randomised, so any UPDATE would show as a different stored value.
            setattr(obj, column, json.loads(json.dumps(getattr(obj, column))))
            session.commit()
        assert _raw(engine, table, column, row_id) == before


class TestCrewNodes:
    def test_nested_and_list_leaves_are_encrypted(self, engine):
        nodes = [
            {
                "id": "agent-1",
                "data": {
                    "label": "analyst",
                    "max_tokens": 4000,
                    "tool_configs": {
                        "MCP_SERVERS": {"servers": [{"name": "s", "auth_token": "t1"}]}
                    },
                    "knowledge_sources": [{"type": "volume", "access_token": "t2"}],
                    "advanced_config": {"api_keys": ["k1", "k2"]},
                },
            }
        ]
        with Session(engine) as session:
            crew = Crew(id=uuid.uuid4(), name="c", nodes=nodes)
            session.add(crew)
            session.commit()
            crew_id = crew.id
        data = _raw(engine, "crews", "nodes", crew_id.hex)[0]["data"]
        server = data["tool_configs"]["MCP_SERVERS"]["servers"][0]
        assert server["auth_token"].startswith("ENC:") and server["name"] == "s"
        assert data["knowledge_sources"][0]["access_token"].startswith("ENC:")
        assert all(k.startswith("ENC:") for k in data["advanced_config"]["api_keys"])
        assert data["max_tokens"] == 4000 and data["label"] == "analyst"
        with Session(engine) as session:
            assert session.get(Crew, crew_id).nodes == nodes


class TestKeysMaskingShows:
    """Masking shows a string count such as ``max_tokens``; storage does not."""

    def test_an_encrypted_value_read_and_saved_again_stays_encrypted(self, engine):
        from src.utils.sensitive_data_utils import encrypt_value

        with Session(engine) as session:
            tool_id = _tool(session).id
            session.commit()
        old = {"T": {"max_tokens": encrypt_value("4000"), "tokens": encrypt_value("t")}}
        _write_raw(engine, "tools", "config", tool_id, old)

        with Session(engine) as session:
            tool = session.get(Tool, tool_id)
            assert tool.config == {"T": {"max_tokens": "4000", "tokens": "t"}}
            tool.config = {**tool.config, "label": "edited"}
            session.commit()

        stored = _raw(engine, "tools", "config", tool_id)
        assert stored["label"] == "edited"
        assert stored["T"]["max_tokens"].startswith("ENC:")
        assert stored["T"]["tokens"].startswith("ENC:")


def _decrypt_spy():
    return patch.object(
        EncryptionUtils, "decrypt_value", wraps=EncryptionUtils.decrypt_value
    )


class TestDecryptCache:
    def _stored_agent(self, engine):
        with Session(engine) as session:
            session.add(
                Agent(
                    id="a1", name="a", role="r", goal="g", tools=[], tool_configs=CONFIG
                )
            )
            session.commit()

    def _load(self, engine):
        with Session(engine) as session:
            return session.get(Agent, "a1").tool_configs

    def test_a_repeated_load_does_not_decrypt_again(self, engine):
        self._stored_agent(engine)
        with _decrypt_spy() as decrypt:
            assert self._load(engine) == CONFIG
            assert self._load(engine) == CONFIG
        assert decrypt.call_count == 1

    def test_a_key_reset_decrypts_afresh(self, engine):
        self._stored_agent(engine)
        with _decrypt_spy() as decrypt:
            self._load(engine)
            EncryptionUtils.reset_key_cache()
            self._load(engine)
        assert decrypt.call_count == 2

    def test_a_failed_decrypt_is_not_cached(self, engine):
        self._stored_agent(engine)
        _write_raw(
            engine, "agents", "tool_configs", "a1", {"P": {"client_secret": "ENC:eA=="}}
        )
        with _decrypt_spy() as decrypt:
            self._load(engine)
            self._load(engine)
        assert decrypt.call_count == 2


class TestReading:
    def test_enc_text_under_an_ordinary_key_is_not_decrypted(self, engine):
        # A prompt or label may start with "ENC:"; only credential keys are
        # ever encrypted, so only they are decrypted.
        with Session(engine) as session:
            session.add(Task(id="t1", name="t", description="d", expected_output="o"))
            session.commit()
        value = {"llm_guardrail": {"description": "ENC: keep this as typed"}}
        _write_raw(engine, "tasks", "config", "t1", value)
        with _decrypt_spy() as decrypt:
            with Session(engine) as session:
                assert session.get(Task, "t1").config == value
        decrypt.assert_not_called()

    def test_a_value_that_cannot_be_decrypted_keeps_its_ciphertext(self, engine):
        with Session(engine) as session:
            agent = Agent(id="a1", name="a", role="r", goal="g", tools=[])
            session.add(agent)
            session.commit()
        unreadable = {"PowerBI": {"client_secret": "ENC:bm90LXZhbGlk"}}
        _write_raw(engine, "agents", "tool_configs", "a1", unreadable)
        with Session(engine) as session:
            agent = session.get(Agent, "a1")
            assert agent.tool_configs == unreadable  # never ''
            # Saving it again writes the same ciphertext back, not a blank.
            agent.tool_configs = dict(agent.tool_configs, extra={"k": "v"})
            session.commit()
        stored = _raw(engine, "agents", "tool_configs", "a1")
        assert stored["PowerBI"]["client_secret"] == "ENC:bm90LXZhbGlk"

    def test_a_legacy_plaintext_row_reads_unchanged(self, engine):
        with Session(engine) as session:
            session.add(Agent(id="a1", name="a", role="r", goal="g", tools=[]))
            session.commit()
        _write_raw(engine, "agents", "tool_configs", "a1", CONFIG)
        with Session(engine) as session:
            assert session.get(Agent, "a1").tool_configs == CONFIG

    def test_a_legacy_plaintext_row_is_encrypted_on_its_next_save(self, engine):
        with Session(engine) as session:
            session.add(Agent(id="a1", name="a", role="r", goal="g", tools=[]))
            session.commit()
        _write_raw(engine, "agents", "tool_configs", "a1", CONFIG)
        with Session(engine) as session:
            agent = session.get(Agent, "a1")
            agent.tool_configs = {**agent.tool_configs, "Other": {"x": 1}}
            session.commit()
        stored = _raw(engine, "agents", "tool_configs", "a1")
        assert stored["PowerBI"]["client_secret"].startswith("ENC:")


class TestWriting:
    def test_nothing_is_encrypted_twice(self, engine):
        with Session(engine) as session:
            agent = Agent(id="a1", name="a", role="r", goal="g", tools=[])
            agent.tool_configs = CONFIG
            session.add(agent)
            session.commit()
        first = _raw(engine, "agents", "tool_configs", "a1")
        with Session(engine) as session:
            agent = session.get(Agent, "a1")
            # A caller that hands back the stored ciphertext (e.g. a value that
            # failed to decrypt, echoed by a form) must not get it re-wrapped.
            agent.tool_configs = {**first, "Other": {"x": 1}}
            session.commit()
        second = _raw(engine, "agents", "tool_configs", "a1")
        assert second["PowerBI"] == first["PowerBI"]
        with Session(engine) as session:
            assert session.get(Agent, "a1").tool_configs["PowerBI"] == CONFIG["PowerBI"]

    def test_a_core_update_encrypts_too(self, engine):
        # base_repository.update and the group-tool repository use update().values().
        with Session(engine) as session:
            tool = _tool(session)
            session.commit()
            tool_id = tool.id
        with Session(engine) as session:
            session.execute(
                update(Tool).where(Tool.id == tool_id).values(config=CONFIG)
            )
            session.commit()
        stored = _raw(engine, "tools", "config", tool_id)
        assert stored["PowerBI"]["client_secret"].startswith("ENC:")

    @pytest.mark.parametrize("value", [None, {}, []])
    def test_empty_values_are_stored_as_before(self, engine, value):
        with Session(engine) as session:
            session.add(Crew(id=uuid.uuid4(), name="c", tool_configs=value))
            session.commit()
        with Session(engine) as session:
            (crew,) = session.query(Crew).all()
            assert crew.tool_configs == value

    @pytest.mark.parametrize("explicit", [True, False])
    def test_none_is_stored_exactly_as_plain_json_stores_it(self, engine, explicit):
        # tasks.output is plain JSON with callback_config's options (nullable,
        # no default): an explicit None is JSON null, an unset one SQL NULL.
        values = {"callback_config": None, "output": None} if explicit else {}
        with Session(engine) as session:
            session.add(
                Task(id="t1", name="t", description="d", expected_output="o", **values)
            )
            session.commit()
        with engine.connect() as conn:
            stored = conn.execute(
                text("SELECT callback_config, output FROM tasks WHERE id = 't1'")
            ).one()
        assert stored[0] == stored[1]
        assert stored[0] == ("null" if explicit else None)

    def test_an_encryption_failure_stores_nothing(self, engine):
        with Session(engine) as session:
            session.add(Agent(id="a1", name="a", role="r", goal="g", tools=[]))
            session.commit()
        with patch(
            "src.utils.sensitive_data_utils.EncryptionUtils.encrypt_value",
            side_effect=RuntimeError("no key"),
        ):
            with Session(engine) as session:
                session.get(Agent, "a1").tool_configs = CONFIG
                with pytest.raises(Exception):
                    session.commit()
        assert _raw(engine, "agents", "tool_configs", "a1") in ({}, None)


def test_the_database_type_is_still_json():
    """Changing only the Python type means no DDL: both render as JSON."""
    from sqlalchemy import JSON
    from sqlalchemy.dialects import postgresql, sqlite

    for dialect in (sqlite.dialect(), postgresql.dialect()):
        assert SensitiveJSON().compile(dialect=dialect) == JSON().compile(
            dialect=dialect
        )
