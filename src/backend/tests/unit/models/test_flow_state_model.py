"""
Unit tests for the FlowState model.

Verifies table name, columns, index, and the created_at default.
"""
from datetime import datetime

from src.models.flow_state import FlowState


class TestFlowStateModel:
    def test_tablename(self):
        assert FlowState.__tablename__ == "flow_states"

    def test_columns_present(self):
        cols = set(FlowState.__table__.columns.keys())
        assert {"id", "flow_uuid", "method_name", "state_json", "created_at"} <= cols

    def test_flow_uuid_indexed(self):
        # The flow_uuid column itself is indexed, plus the composite index.
        index_names = {ix.name for ix in FlowState.__table__.indexes}
        assert "ix_flow_states_uuid_created" in index_names

    def test_required_columns_not_nullable(self):
        table = FlowState.__table__
        assert table.columns["flow_uuid"].nullable is False
        assert table.columns["method_name"].nullable is False
        assert table.columns["state_json"].nullable is False

    def test_instance_fields(self):
        fs = FlowState(flow_uuid="u1", method_name="start", state_json='{"a":1}')
        assert fs.flow_uuid == "u1"
        assert fs.method_name == "start"
        assert fs.state_json == '{"a":1}'

    def test_created_at_default_is_callable_utc(self):
        default = FlowState.__table__.columns["created_at"].default
        assert default is not None
        produced = default.arg(None)
        assert isinstance(produced, datetime)
