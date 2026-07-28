"""
Comprehensive unit tests for billing SQLAlchemy models.

Tests all models in billing.py including table structure and utility functions.
"""
import pytest
from datetime import datetime, timezone
from decimal import Decimal
from sqlalchemy import Column, Integer, String, Numeric, DateTime, ForeignKey, JSON, Index
from uuid import UUID

from src.models.billing import (
    generate_billing_id, LLMUsageBilling, BillingPeriod, BillingAlert
)
from src.db.base import Base


class TestGenerateBillingId:
    """Test generate_billing_id utility function."""

    def test_generate_billing_id_returns_string(self):
        """Test generate_billing_id returns a string."""
        billing_id = generate_billing_id()
        
        assert isinstance(billing_id, str)

    def test_generate_billing_id_is_uuid(self):
        """Test generate_billing_id returns a valid UUID string."""
        billing_id = generate_billing_id()
        
        # Should be able to parse as UUID
        uuid_obj = UUID(billing_id)
        assert str(uuid_obj) == billing_id

    def test_generate_billing_id_unique(self):
        """Test generate_billing_id returns unique values."""
        id1 = generate_billing_id()
        id2 = generate_billing_id()
        
        assert id1 != id2

    def test_generate_billing_id_format(self):
        """Test generate_billing_id returns properly formatted UUID."""
        billing_id = generate_billing_id()
        
        # UUID4 format: 8-4-4-4-12 characters
        parts = billing_id.split('-')
        assert len(parts) == 5
        assert len(parts[0]) == 8
        assert len(parts[1]) == 4
        assert len(parts[2]) == 4
        assert len(parts[3]) == 4
        assert len(parts[4]) == 12


class TestLLMUsageBilling:
    """Test LLMUsageBilling model."""

    def test_llm_usage_billing_inherits_base(self):
        """Test LLMUsageBilling inherits from Base."""
        assert issubclass(LLMUsageBilling, Base)

    def test_llm_usage_billing_tablename(self):
        """Test LLMUsageBilling table name."""
        assert LLMUsageBilling.__tablename__ == "llm_usage_billing"

    def test_llm_usage_billing_columns_exist(self):
        """Test LLMUsageBilling has expected columns."""
        expected_columns = [
            'id', 'execution_id', 'execution_type', 'execution_name',
            'model_name', 'model_provider', 'prompt_tokens', 'completion_tokens',
            'total_tokens', 'cost_usd', 'cost_per_prompt_token', 'cost_per_completion_token',
            'duration_ms', 'request_count', 'status', 'error_message',
            'group_id', 'user_email', 'usage_date', 'created_at', 'updated_at',
            'billing_metadata'
        ]
        
        for column_name in expected_columns:
            assert hasattr(LLMUsageBilling, column_name)

    def test_llm_usage_billing_id_column_properties(self):
        """Test id column properties."""
        id_column = LLMUsageBilling.id
        assert isinstance(id_column.property.columns[0], Column)
        assert isinstance(id_column.property.columns[0].type, String)
        assert id_column.property.columns[0].primary_key is True
        assert id_column.property.columns[0].index is True
        assert id_column.property.columns[0].default is not None

    def test_llm_usage_billing_execution_id_column_properties(self):
        """Test execution_id column properties."""
        execution_id_column = LLMUsageBilling.execution_id
        column = execution_id_column.property.columns[0]
        assert isinstance(column, Column)
        assert isinstance(column.type, String)
        assert column.nullable is False
        assert column.index is True
        assert len(column.foreign_keys) == 1

    def test_llm_usage_billing_numeric_columns(self):
        """Test numeric column properties."""
        cost_usd_column = LLMUsageBilling.cost_usd.property.columns[0]
        assert isinstance(cost_usd_column.type, Numeric)
        assert cost_usd_column.type.precision == 10
        assert cost_usd_column.type.scale == 6

        cost_per_prompt_column = LLMUsageBilling.cost_per_prompt_token.property.columns[0]
        assert isinstance(cost_per_prompt_column.type, Numeric)
        assert cost_per_prompt_column.type.precision == 10
        assert cost_per_prompt_column.type.scale == 8

    def test_llm_usage_billing_integer_columns(self):
        """Test integer column properties."""
        prompt_tokens_column = LLMUsageBilling.prompt_tokens.property.columns[0]
        assert isinstance(prompt_tokens_column.type, Integer)
        assert prompt_tokens_column.default.arg == 0

        total_tokens_column = LLMUsageBilling.total_tokens.property.columns[0]
        assert isinstance(total_tokens_column.type, Integer)
        assert total_tokens_column.default.arg == 0

    def test_llm_usage_billing_datetime_columns(self):
        """Test datetime column properties."""
        usage_date_column = LLMUsageBilling.usage_date.property.columns[0]
        assert isinstance(usage_date_column.type, DateTime)
        assert usage_date_column.index is True

        created_at_column = LLMUsageBilling.created_at.property.columns[0]
        assert isinstance(created_at_column.type, DateTime)

        updated_at_column = LLMUsageBilling.updated_at.property.columns[0]
        assert isinstance(updated_at_column.type, DateTime)
        assert updated_at_column.onupdate is not None

    def test_llm_usage_billing_json_columns(self):
        """Test JSON column properties."""
        billing_metadata_column = LLMUsageBilling.billing_metadata.property.columns[0]
        assert isinstance(billing_metadata_column.type, JSON)
        assert billing_metadata_column.default is not None

    def test_llm_usage_billing_indexes(self):
        """Test LLMUsageBilling table indexes."""
        table = LLMUsageBilling.__table__
        index_names = [idx.name for idx in table.indexes]
        
        expected_indexes = [
            'idx_billing_group_date',
            'idx_billing_user_date', 
            'idx_billing_execution_model',
            'idx_billing_provider_date'
        ]
        
        for expected_index in expected_indexes:
            assert expected_index in index_names

    def test_llm_usage_billing_initialization(self):
        """Test LLMUsageBilling initialization."""
        billing = LLMUsageBilling()
        
        assert isinstance(billing, LLMUsageBilling)
        assert isinstance(billing, Base)

    def test_llm_usage_billing_initialization_with_values(self):
        """Test LLMUsageBilling initialization with values."""
        billing = LLMUsageBilling(
            execution_id="test-execution-123",
            execution_type="crew",
            model_name="gpt-4",
            model_provider="openai",
            prompt_tokens=100,
            completion_tokens=50,
            total_tokens=150,
            cost_usd=Decimal("0.003000"),
            group_id="test-group"
        )
        
        assert billing.execution_id == "test-execution-123"
        assert billing.execution_type == "crew"
        assert billing.model_name == "gpt-4"
        assert billing.model_provider == "openai"
        assert billing.prompt_tokens == 100
        assert billing.completion_tokens == 50
        assert billing.total_tokens == 150
        assert billing.cost_usd == Decimal("0.003000")
        assert billing.group_id == "test-group"


class TestBillingPeriod:
    """Test BillingPeriod model."""

    def test_billing_period_inherits_base(self):
        """Test BillingPeriod inherits from Base."""
        assert issubclass(BillingPeriod, Base)

    def test_billing_period_tablename(self):
        """Test BillingPeriod table name."""
        assert BillingPeriod.__tablename__ == "billing_periods"

    def test_billing_period_columns_exist(self):
        """Test BillingPeriod has expected columns."""
        expected_columns = [
            'id', 'period_start', 'period_end', 'period_type', 'group_id',
            'total_cost_usd', 'total_tokens', 'total_prompt_tokens',
            'total_completion_tokens', 'total_requests', 'model_breakdown',
            'status', 'created_at', 'updated_at', 'closed_at'
        ]
        
        for column_name in expected_columns:
            assert hasattr(BillingPeriod, column_name)

    def test_billing_period_id_column_properties(self):
        """Test id column properties."""
        id_column = BillingPeriod.id
        assert isinstance(id_column.property.columns[0], Column)
        assert isinstance(id_column.property.columns[0].type, String)
        assert id_column.property.columns[0].primary_key is True
        assert id_column.property.columns[0].index is True
        assert id_column.property.columns[0].default is not None

    def test_billing_period_datetime_columns(self):
        """Test datetime column properties."""
        period_start_column = BillingPeriod.period_start.property.columns[0]
        assert isinstance(period_start_column.type, DateTime)
        assert period_start_column.nullable is False
        assert period_start_column.index is True

        period_end_column = BillingPeriod.period_end.property.columns[0]
        assert isinstance(period_end_column.type, DateTime)
        assert period_end_column.nullable is False
        assert period_end_column.index is True

    def test_billing_period_numeric_columns(self):
        """Test numeric column properties."""
        total_cost_column = BillingPeriod.total_cost_usd.property.columns[0]
        assert isinstance(total_cost_column.type, Numeric)
        assert total_cost_column.type.precision == 10
        assert total_cost_column.type.scale == 2
        assert total_cost_column.default.arg == Decimal("0.00")

    def test_billing_period_string_columns(self):
        """Test string column properties."""
        period_type_column = BillingPeriod.period_type.property.columns[0]
        assert isinstance(period_type_column.type, String)
        assert period_type_column.nullable is False
        assert period_type_column.default.arg == "monthly"

        status_column = BillingPeriod.status.property.columns[0]
        assert isinstance(status_column.type, String)
        assert status_column.nullable is False
        assert status_column.default.arg == "active"

    def test_billing_period_indexes(self):
        """Test BillingPeriod table indexes."""
        table = BillingPeriod.__table__
        index_names = [idx.name for idx in table.indexes]
        
        expected_indexes = [
            'idx_period_group_dates',
            'idx_period_status_date'
        ]
        
        for expected_index in expected_indexes:
            assert expected_index in index_names

    def test_billing_period_initialization(self):
        """Test BillingPeriod initialization."""
        period = BillingPeriod()
        
        assert isinstance(period, BillingPeriod)
        assert isinstance(period, Base)

    def test_billing_period_initialization_with_values(self):
        """Test BillingPeriod initialization with values."""
        start_date = datetime(2023, 1, 1)
        end_date = datetime(2023, 1, 31)
        
        period = BillingPeriod(
            period_start=start_date,
            period_end=end_date,
            period_type="monthly",
            group_id="test-group",
            total_cost_usd=Decimal("25.50"),
            total_tokens=10000,
            status="active"
        )
        
        assert period.period_start == start_date
        assert period.period_end == end_date
        assert period.period_type == "monthly"
        assert period.group_id == "test-group"
        assert period.total_cost_usd == Decimal("25.50")
        assert period.total_tokens == 10000
        assert period.status == "active"


class TestBillingAlert:
    """Test BillingAlert model."""

    def test_billing_alert_inherits_base(self):
        """Test BillingAlert inherits from Base."""
        assert issubclass(BillingAlert, Base)

    def test_billing_alert_tablename(self):
        """Test BillingAlert table name."""
        assert BillingAlert.__tablename__ == "billing_alerts"

    def test_billing_alert_columns_exist(self):
        """Test BillingAlert has expected columns."""
        expected_columns = [
            'id', 'alert_name', 'alert_type', 'threshold_value', 'threshold_period',
            'group_id', 'user_email', 'is_active', 'current_value', 'last_triggered',
            'notification_emails', 'created_at', 'updated_at', 'alert_metadata'
        ]
        
        for column_name in expected_columns:
            assert hasattr(BillingAlert, column_name)

    def test_billing_alert_id_column_properties(self):
        """Test id column properties."""
        id_column = BillingAlert.id
        assert isinstance(id_column.property.columns[0], Column)
        assert isinstance(id_column.property.columns[0].type, String)
        assert id_column.property.columns[0].primary_key is True
        assert id_column.property.columns[0].index is True
        assert id_column.property.columns[0].default is not None

    def test_billing_alert_string_columns(self):
        """Test string column properties."""
        alert_name_column = BillingAlert.alert_name.property.columns[0]
        assert isinstance(alert_name_column.type, String)
        assert alert_name_column.nullable is False

        alert_type_column = BillingAlert.alert_type.property.columns[0]
        assert isinstance(alert_type_column.type, String)
        assert alert_type_column.nullable is False
        assert alert_type_column.default.arg == "cost_threshold"

        is_active_column = BillingAlert.is_active.property.columns[0]
        assert isinstance(is_active_column.type, String)
        assert is_active_column.nullable is False
        assert is_active_column.default.arg == "true"

    def test_billing_alert_numeric_columns(self):
        """Test numeric column properties."""
        threshold_value_column = BillingAlert.threshold_value.property.columns[0]
        assert isinstance(threshold_value_column.type, Numeric)
        assert threshold_value_column.type.precision == 10
        assert threshold_value_column.type.scale == 2
        assert threshold_value_column.nullable is False

        current_value_column = BillingAlert.current_value.property.columns[0]
        assert isinstance(current_value_column.type, Numeric)
        assert current_value_column.type.precision == 10
        assert current_value_column.type.scale == 2
        assert current_value_column.default.arg == Decimal("0.00")

    def test_billing_alert_json_columns(self):
        """Test JSON column properties."""
        notification_emails_column = BillingAlert.notification_emails.property.columns[0]
        assert isinstance(notification_emails_column.type, JSON)
        assert notification_emails_column.default is not None

        alert_metadata_column = BillingAlert.alert_metadata.property.columns[0]
        assert isinstance(alert_metadata_column.type, JSON)
        assert alert_metadata_column.default is not None

    def test_billing_alert_initialization(self):
        """Test BillingAlert initialization."""
        alert = BillingAlert()
        
        assert isinstance(alert, BillingAlert)
        assert isinstance(alert, Base)

    def test_billing_alert_initialization_with_values(self):
        """Test BillingAlert initialization with values."""
        alert = BillingAlert(
            alert_name="Monthly Cost Alert",
            alert_type="cost_threshold",
            threshold_value=Decimal("100.00"),
            threshold_period="monthly",
            group_id="test-group",
            is_active="true",
            notification_emails=["admin@example.com"]
        )
        
        assert alert.alert_name == "Monthly Cost Alert"
        assert alert.alert_type == "cost_threshold"
        assert alert.threshold_value == Decimal("100.00")
        assert alert.threshold_period == "monthly"
        assert alert.group_id == "test-group"
        assert alert.is_active == "true"
        assert alert.notification_emails == ["admin@example.com"]


class TestBillingModelInteroperability:
    """Test billing model interoperability and relationships."""

    def test_all_models_inherit_base(self):
        """Test all billing models inherit from Base."""
        assert issubclass(LLMUsageBilling, Base)
        assert issubclass(BillingPeriod, Base)
        assert issubclass(BillingAlert, Base)

    def test_all_models_have_sqlalchemy_attributes(self):
        """Test all models have SQLAlchemy attributes."""
        models = [LLMUsageBilling, BillingPeriod, BillingAlert]
        
        for model in models:
            assert hasattr(model, '__table__')
            assert hasattr(model, '__mapper__')
            assert hasattr(model, 'metadata')

    def test_all_models_have_id_primary_key(self):
        """Test all models have id as primary key."""
        models = [LLMUsageBilling, BillingPeriod, BillingAlert]
        
        for model in models:
            table = model.__table__
            primary_key_columns = [col.name for col in table.primary_key.columns]
            assert primary_key_columns == ['id']

    def test_all_models_use_generate_billing_id(self):
        """Test all models use generate_billing_id for default id."""
        models = [LLMUsageBilling, BillingPeriod, BillingAlert]
        
        for model in models:
            id_column = model.id.property.columns[0]
            assert id_column.default is not None

    def test_group_id_consistency(self):
        """Test group_id column consistency across models."""
        models_with_group_id = [LLMUsageBilling, BillingPeriod, BillingAlert]
        
        for model in models_with_group_id:
            group_id_column = model.group_id.property.columns[0]
            assert isinstance(group_id_column.type, String)
            assert group_id_column.index is True
            assert group_id_column.nullable is True
