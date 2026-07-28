"""
Comprehensive unit tests for FlowExecution and FlowNodeExecution SQLAlchemy models.

Tests all models in flow_execution.py including table structure and initialization logic.
"""
import pytest
from datetime import datetime
from sqlalchemy import Column, Integer, String, JSON, ForeignKey, DateTime, Text
from sqlalchemy.dialects.postgresql import UUID

from src.models.flow_execution import FlowExecution, FlowNodeExecution
from src.db.base import Base


class TestFlowExecution:
    """Test FlowExecution model."""

    def test_flow_execution_inherits_base(self):
        """Test FlowExecution inherits from Base."""
        assert issubclass(FlowExecution, Base)

    def test_flow_execution_tablename(self):
        """Test FlowExecution table name."""
        assert FlowExecution.__tablename__ == "flow_executions"

    def test_flow_execution_columns_exist(self):
        """Test FlowExecution has expected columns."""
        expected_columns = [
            'id', 'flow_id', 'job_id', 'status', 'config', 'result', 'error',
            'created_at', 'updated_at', 'completed_at'
        ]
        
        for column_name in expected_columns:
            assert hasattr(FlowExecution, column_name)

    def test_flow_execution_id_column_properties(self):
        """Test id column properties."""
        id_column = FlowExecution.id
        assert isinstance(id_column.property.columns[0], Column)
        assert isinstance(id_column.property.columns[0].type, Integer)
        assert id_column.property.columns[0].primary_key is True

    def test_flow_execution_flow_id_column_properties(self):
        """Test flow_id column properties."""
        flow_id_column = FlowExecution.flow_id
        column = flow_id_column.property.columns[0]
        assert isinstance(column, Column)
        assert isinstance(column.type, UUID)
        assert column.type.as_uuid is True
        # flow_id is nullable (optional reference to saved flow)
        assert column.nullable is True
        # No foreign key constraint in current model
        assert len(column.foreign_keys) == 0

    def test_flow_execution_job_id_column_properties(self):
        """Test job_id column properties."""
        job_id_column = FlowExecution.job_id
        column = job_id_column.property.columns[0]
        assert isinstance(column, Column)
        assert isinstance(column.type, String)
        assert column.nullable is False
        assert column.unique is True

    def test_flow_execution_status_column_properties(self):
        """Test status column properties."""
        status_column = FlowExecution.status
        column = status_column.property.columns[0]
        assert isinstance(column, Column)
        assert isinstance(column.type, String)
        assert column.nullable is False
        assert column.default.arg == "pending"

    def test_flow_execution_config_column_properties(self):
        """Test config column properties."""
        config_column = FlowExecution.config
        column = config_column.property.columns[0]
        assert isinstance(column, Column)
        assert isinstance(column.type, JSON)
        assert column.default is not None

    def test_flow_execution_result_column_properties(self):
        """Test result column properties."""
        result_column = FlowExecution.result
        column = result_column.property.columns[0]
        assert isinstance(column, Column)
        assert isinstance(column.type, JSON)
        assert column.nullable is True

    def test_flow_execution_error_column_properties(self):
        """Test error column properties."""
        error_column = FlowExecution.error
        column = error_column.property.columns[0]
        assert isinstance(column, Column)
        assert isinstance(column.type, Text)
        assert column.nullable is True

    def test_flow_execution_datetime_columns_properties(self):
        """Test datetime column properties."""
        created_at_column = FlowExecution.created_at
        column = created_at_column.property.columns[0]
        assert isinstance(column, Column)
        assert isinstance(column.type, DateTime)
        assert column.default is not None

        updated_at_column = FlowExecution.updated_at
        column = updated_at_column.property.columns[0]
        assert isinstance(column, Column)
        assert isinstance(column.type, DateTime)
        assert column.default is not None
        assert column.onupdate is not None

        completed_at_column = FlowExecution.completed_at
        column = completed_at_column.property.columns[0]
        assert isinstance(column, Column)
        assert isinstance(column.type, DateTime)
        assert column.nullable is True


class TestFlowExecutionInitialization:
    """Test FlowExecution initialization."""

    def test_flow_execution_minimal_initialization(self):
        """Test FlowExecution initialization with minimal required fields."""
        import uuid
        flow_id = uuid.uuid4()
        
        execution = FlowExecution(
            flow_id=flow_id,
            job_id="job-123"
        )
        
        assert execution.flow_id == flow_id
        assert execution.job_id == "job-123"
        assert execution.config == {}  # Default applied in __init__

    def test_flow_execution_initialization_with_all_fields(self):
        """Test FlowExecution initialization with all fields."""
        import uuid
        flow_id = uuid.uuid4()
        created_time = datetime.utcnow()
        updated_time = datetime.utcnow()
        completed_time = datetime.utcnow()
        
        execution = FlowExecution(
            flow_id=flow_id,
            job_id="job-123",
            status="running",
            config={"param": "value"},
            result={"output": "data"},
            error="Error message",
            created_at=created_time,
            updated_at=updated_time,
            completed_at=completed_time
        )
        
        assert execution.flow_id == flow_id
        assert execution.job_id == "job-123"
        assert execution.status == "running"
        assert execution.config == {"param": "value"}
        assert execution.result == {"output": "data"}
        assert execution.error == "Error message"
        assert execution.created_at == created_time
        assert execution.updated_at == updated_time
        assert execution.completed_at == completed_time

    def test_flow_execution_initialization_with_none_config(self):
        """Test FlowExecution initialization handles None config."""
        import uuid
        flow_id = uuid.uuid4()
        
        execution = FlowExecution(
            flow_id=flow_id,
            job_id="job-123",
            config=None
        )
        
        assert execution.config == {}  # None replaced with empty dict

    def test_flow_execution_default_values(self):
        """Test FlowExecution default values (applied at database level)."""
        import uuid
        flow_id = uuid.uuid4()
        
        execution = FlowExecution(
            flow_id=flow_id,
            job_id="job-123"
        )
        
        # These should be None until saved to database (defaults are applied at DB level)
        assert execution.status is None  # Will be "pending" when saved to DB
        assert execution.result is None
        assert execution.error is None
        assert execution.completed_at is None


class TestFlowNodeExecution:
    """Test FlowNodeExecution model."""

    def test_flow_node_execution_inherits_base(self):
        """Test FlowNodeExecution inherits from Base."""
        assert issubclass(FlowNodeExecution, Base)

    def test_flow_node_execution_tablename(self):
        """Test FlowNodeExecution table name."""
        assert FlowNodeExecution.__tablename__ == "flow_node_executions"

    def test_flow_node_execution_columns_exist(self):
        """Test FlowNodeExecution has expected columns."""
        expected_columns = [
            'id', 'flow_execution_id', 'node_id', 'status', 'agent_id', 'task_id',
            'result', 'error', 'created_at', 'updated_at', 'completed_at'
        ]
        
        for column_name in expected_columns:
            assert hasattr(FlowNodeExecution, column_name)

    def test_flow_node_execution_id_column_properties(self):
        """Test id column properties."""
        id_column = FlowNodeExecution.id
        assert isinstance(id_column.property.columns[0], Column)
        assert isinstance(id_column.property.columns[0].type, Integer)
        assert id_column.property.columns[0].primary_key is True

    def test_flow_node_execution_flow_execution_id_column_properties(self):
        """Test flow_execution_id column properties."""
        flow_execution_id_column = FlowNodeExecution.flow_execution_id
        column = flow_execution_id_column.property.columns[0]
        assert isinstance(column, Column)
        assert isinstance(column.type, Integer)
        assert column.nullable is False
        assert len(column.foreign_keys) == 1
        fk = list(column.foreign_keys)[0]
        assert str(fk.column) == "flow_executions.id"

    def test_flow_node_execution_node_id_column_properties(self):
        """Test node_id column properties."""
        node_id_column = FlowNodeExecution.node_id
        column = node_id_column.property.columns[0]
        assert isinstance(column, Column)
        assert isinstance(column.type, String)
        assert column.nullable is False

    def test_flow_node_execution_status_column_properties(self):
        """Test status column properties."""
        status_column = FlowNodeExecution.status
        column = status_column.property.columns[0]
        assert isinstance(column, Column)
        assert isinstance(column.type, String)
        assert column.nullable is False
        assert column.default.arg == "pending"

    def test_flow_node_execution_optional_id_columns_properties(self):
        """Test optional ID column properties."""
        agent_id_column = FlowNodeExecution.agent_id
        column = agent_id_column.property.columns[0]
        assert isinstance(column, Column)
        assert isinstance(column.type, Integer)
        assert column.nullable is True

        task_id_column = FlowNodeExecution.task_id
        column = task_id_column.property.columns[0]
        assert isinstance(column, Column)
        assert isinstance(column.type, Integer)
        assert column.nullable is True

    def test_flow_node_execution_result_column_properties(self):
        """Test result column properties."""
        result_column = FlowNodeExecution.result
        column = result_column.property.columns[0]
        assert isinstance(column, Column)
        assert isinstance(column.type, JSON)
        assert column.nullable is True

    def test_flow_node_execution_error_column_properties(self):
        """Test error column properties."""
        error_column = FlowNodeExecution.error
        column = error_column.property.columns[0]
        assert isinstance(column, Column)
        assert isinstance(column.type, Text)
        assert column.nullable is True

    def test_flow_node_execution_datetime_columns_properties(self):
        """Test datetime column properties."""
        created_at_column = FlowNodeExecution.created_at
        column = created_at_column.property.columns[0]
        assert isinstance(column, Column)
        assert isinstance(column.type, DateTime)
        assert column.default is not None

        updated_at_column = FlowNodeExecution.updated_at
        column = updated_at_column.property.columns[0]
        assert isinstance(column, Column)
        assert isinstance(column.type, DateTime)
        assert column.default is not None
        assert column.onupdate is not None

        completed_at_column = FlowNodeExecution.completed_at
        column = completed_at_column.property.columns[0]
        assert isinstance(column, Column)
        assert isinstance(column.type, DateTime)
        assert column.nullable is True


class TestFlowNodeExecutionInitialization:
    """Test FlowNodeExecution initialization."""

    def test_flow_node_execution_minimal_initialization(self):
        """Test FlowNodeExecution initialization with minimal required fields."""
        node_execution = FlowNodeExecution(
            flow_execution_id=1,
            node_id="node-123"
        )
        
        assert node_execution.flow_execution_id == 1
        assert node_execution.node_id == "node-123"

    def test_flow_node_execution_initialization_with_all_fields(self):
        """Test FlowNodeExecution initialization with all fields."""
        created_time = datetime.utcnow()
        updated_time = datetime.utcnow()
        completed_time = datetime.utcnow()
        
        node_execution = FlowNodeExecution(
            flow_execution_id=1,
            node_id="node-123",
            status="completed",
            agent_id=456,
            task_id=789,
            result={"output": "node_data"},
            error="Node error message",
            created_at=created_time,
            updated_at=updated_time,
            completed_at=completed_time
        )
        
        assert node_execution.flow_execution_id == 1
        assert node_execution.node_id == "node-123"
        assert node_execution.status == "completed"
        assert node_execution.agent_id == 456
        assert node_execution.task_id == 789
        assert node_execution.result == {"output": "node_data"}
        assert node_execution.error == "Node error message"
        assert node_execution.created_at == created_time
        assert node_execution.updated_at == updated_time
        assert node_execution.completed_at == completed_time

    def test_flow_node_execution_default_values(self):
        """Test FlowNodeExecution default values (applied at database level)."""
        node_execution = FlowNodeExecution(
            flow_execution_id=1,
            node_id="node-123"
        )
        
        # These should be None until saved to database (defaults are applied at DB level)
        assert node_execution.status is None  # Will be "pending" when saved to DB
        assert node_execution.agent_id is None
        assert node_execution.task_id is None
        assert node_execution.result is None
        assert node_execution.error is None
        assert node_execution.completed_at is None


class TestFlowExecutionTableStructure:
    """Test FlowExecution and FlowNodeExecution table structure."""

    def test_flow_execution_table_exists(self):
        """Test FlowExecution table exists in metadata."""
        assert hasattr(FlowExecution, '__table__')
        assert FlowExecution.__table__.name == "flow_executions"

    def test_flow_node_execution_table_exists(self):
        """Test FlowNodeExecution table exists in metadata."""
        assert hasattr(FlowNodeExecution, '__table__')
        assert FlowNodeExecution.__table__.name == "flow_node_executions"

    def test_flow_execution_primary_key(self):
        """Test FlowExecution primary key."""
        table = FlowExecution.__table__
        primary_key_columns = [col.name for col in table.primary_key.columns]
        assert primary_key_columns == ['id']

    def test_flow_node_execution_primary_key(self):
        """Test FlowNodeExecution primary key."""
        table = FlowNodeExecution.__table__
        primary_key_columns = [col.name for col in table.primary_key.columns]
        assert primary_key_columns == ['id']

    def test_flow_execution_foreign_keys(self):
        """Test FlowExecution foreign keys.

        Note: flow_id has no FK constraint in current model (optional reference).
        """
        table = FlowExecution.__table__

        flow_id_column = table.columns['flow_id']
        foreign_keys = list(flow_id_column.foreign_keys)
        # No FK constraint in current model
        assert len(foreign_keys) == 0

    def test_flow_node_execution_foreign_keys(self):
        """Test FlowNodeExecution foreign keys."""
        table = FlowNodeExecution.__table__
        
        flow_execution_id_column = table.columns['flow_execution_id']
        foreign_keys = list(flow_execution_id_column.foreign_keys)
        assert len(foreign_keys) == 1
        fk = foreign_keys[0]
        assert str(fk.column) == "flow_executions.id"

    def test_flow_execution_unique_constraints(self):
        """Test FlowExecution unique constraints."""
        table = FlowExecution.__table__
        
        job_id_column = table.columns['job_id']
        assert job_id_column.unique is True

    def test_flow_execution_nullable_columns(self):
        """Test FlowExecution nullable column configuration."""
        table = FlowExecution.__table__

        # Non-nullable columns
        non_nullable = ['id', 'job_id', 'status']
        for col_name in non_nullable:
            column = table.columns[col_name]
            assert not column.nullable, f"Column {col_name} should not be nullable"

        # Nullable columns (flow_id is nullable in current model)
        nullable = ['flow_id', 'result', 'error', 'completed_at']
        for col_name in nullable:
            column = table.columns[col_name]
            assert column.nullable, f"Column {col_name} should be nullable"

    def test_flow_node_execution_nullable_columns(self):
        """Test FlowNodeExecution nullable column configuration."""
        table = FlowNodeExecution.__table__
        
        # Non-nullable columns
        non_nullable = ['id', 'flow_execution_id', 'node_id', 'status']
        for col_name in non_nullable:
            column = table.columns[col_name]
            assert not column.nullable, f"Column {col_name} should not be nullable"
        
        # Nullable columns
        nullable = ['agent_id', 'task_id', 'result', 'error', 'completed_at']
        for col_name in nullable:
            column = table.columns[col_name]
            assert column.nullable, f"Column {col_name} should be nullable"

    def test_all_models_have_sqlalchemy_attributes(self):
        """Test all models have SQLAlchemy attributes."""
        models = [FlowExecution, FlowNodeExecution]
        
        for model in models:
            assert hasattr(model, '__table__')
            assert hasattr(model, '__mapper__')
            assert hasattr(model, 'metadata')
