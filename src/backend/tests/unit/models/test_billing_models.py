"""
Unit tests for billing models.

Tests the functionality of the LLMUsageBilling, BillingPeriod, and BillingAlert
database models including field validation, relationships, and data integrity.
"""
import pytest
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock

from src.models.billing import (
    LLMUsageBilling, 
    BillingPeriod, 
    BillingAlert, 
    generate_billing_id
)


class TestLLMUsageBilling:
    """Test cases for LLMUsageBilling model."""

    def test_llm_usage_billing_creation(self):
        """Test basic LLMUsageBilling model creation."""
        # Arrange
        execution_id = "exec-123"
        execution_type = "crew"
        model_name = "gpt-4"
        model_provider = "openai"
        prompt_tokens = 100
        completion_tokens = 50
        cost_usd = Decimal("0.002500")
        
        # Act
        billing = LLMUsageBilling(
            execution_id=execution_id,
            execution_type=execution_type,
            model_name=model_name,
            model_provider=model_provider,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            cost_usd=cost_usd
        )
        
        # Assert
        assert billing.execution_id == execution_id
        assert billing.execution_type == execution_type
        assert billing.model_name == model_name
        assert billing.model_provider == model_provider
        assert billing.prompt_tokens == prompt_tokens
        assert billing.completion_tokens == completion_tokens
        assert billing.cost_usd == cost_usd
        # Note: SQLAlchemy defaults are applied when saved to database
        # Here we test the column default configurations
        assert LLMUsageBilling.__table__.columns['total_tokens'].default.arg == 0
        assert LLMUsageBilling.__table__.columns['status'].default.arg == "success"
        assert LLMUsageBilling.__table__.columns['request_count'].default.arg == 1

    def test_llm_usage_billing_with_all_fields(self):
        """Test LLMUsageBilling model creation with all fields."""
        # Arrange
        execution_id = "exec-456"
        execution_type = "agent"
        execution_name = "Research Agent"
        model_name = "claude-3-sonnet"
        model_provider = "anthropic"
        prompt_tokens = 200
        completion_tokens = 100
        total_tokens = 300
        cost_usd = Decimal("0.005000")
        cost_per_prompt_token = Decimal("0.000015")
        cost_per_completion_token = Decimal("0.000075")
        duration_ms = 1500
        request_count = 3
        status = "success"
        group_id = "group-789"
        user_email = "user@example.com"
        billing_metadata = {"project": "test", "tag": "production"}
        
        # Act
        billing = LLMUsageBilling(
            execution_id=execution_id,
            execution_type=execution_type,
            execution_name=execution_name,
            model_name=model_name,
            model_provider=model_provider,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            total_tokens=total_tokens,
            cost_usd=cost_usd,
            cost_per_prompt_token=cost_per_prompt_token,
            cost_per_completion_token=cost_per_completion_token,
            duration_ms=duration_ms,
            request_count=request_count,
            status=status,
            group_id=group_id,
            user_email=user_email,
            billing_metadata=billing_metadata
        )
        
        # Assert
        assert billing.execution_name == execution_name
        assert billing.total_tokens == total_tokens
        assert billing.cost_per_prompt_token == cost_per_prompt_token
        assert billing.cost_per_completion_token == cost_per_completion_token
        assert billing.duration_ms == duration_ms
        assert billing.request_count == request_count
        assert billing.status == status
        assert billing.group_id == group_id
        assert billing.user_email == user_email
        assert billing.billing_metadata == billing_metadata

    def test_llm_usage_billing_error_status(self):
        """Test LLMUsageBilling model with error status."""
        # Arrange
        execution_id = "exec-error"
        execution_type = "task"
        model_name = "gpt-3.5-turbo"
        model_provider = "openai"
        status = "error"
        error_message = "API rate limit exceeded"
        
        # Act
        billing = LLMUsageBilling(
            execution_id=execution_id,
            execution_type=execution_type,
            model_name=model_name,
            model_provider=model_provider,
            status=status,
            error_message=error_message
        )
        
        # Assert
        assert billing.status == status
        assert billing.error_message == error_message
        # Note: SQLAlchemy defaults are applied when saved to database
        assert billing.cost_usd is None or billing.cost_usd == Decimal("0.000000")

    def test_llm_usage_billing_defaults(self):
        """Test LLMUsageBilling model with default values."""
        # Arrange & Act
        billing = LLMUsageBilling(
            execution_id="exec-123",
            execution_type="crew",
            model_name="gpt-4",
            model_provider="openai"
        )
        
        # Assert
        # Note: SQLAlchemy defaults are applied when saved to database
        # Here we test that the column defaults are configured correctly
        assert LLMUsageBilling.__table__.columns['prompt_tokens'].default.arg == 0
        assert LLMUsageBilling.__table__.columns['completion_tokens'].default.arg == 0
        assert LLMUsageBilling.__table__.columns['total_tokens'].default.arg == 0
        assert LLMUsageBilling.__table__.columns['request_count'].default.arg == 1
        assert LLMUsageBilling.__table__.columns['status'].default.arg == "success"
        # Check that billing_metadata default is callable (the dict function)
        assert callable(LLMUsageBilling.__table__.columns['billing_metadata'].default.arg)
        assert LLMUsageBilling.__table__.columns['billing_metadata'].default.arg.__name__ == 'dict'

    def test_llm_usage_billing_table_name(self):
        """Test that the table name is correctly set."""
        # Act & Assert
        assert LLMUsageBilling.__tablename__ == "llm_usage_billing"

    def test_llm_usage_billing_indexes(self):
        """Test that the model has the expected database indexes."""
        # Act
        indexes = LLMUsageBilling.__table_args__
        
        # Assert
        assert len(indexes) == 4
        
        # Check index names
        index_names = [index.name for index in indexes if hasattr(index, 'name')]
        expected_indexes = [
            'idx_billing_group_date',
            'idx_billing_user_date',
            'idx_billing_execution_model',
            'idx_billing_provider_date'
        ]
        
        for expected_index in expected_indexes:
            assert expected_index in index_names


class TestBillingPeriod:
    """Test cases for BillingPeriod model."""

    def test_billing_period_creation(self):
        """Test basic BillingPeriod model creation."""
        # Arrange
        period_start = datetime(2023, 1, 1, tzinfo=timezone.utc)
        period_end = datetime(2023, 1, 31, tzinfo=timezone.utc)
        period_type = "monthly"
        group_id = "group-123"
        
        # Act
        period = BillingPeriod(
            period_start=period_start,
            period_end=period_end,
            period_type=period_type,
            group_id=group_id
        )
        
        # Assert
        assert period.period_start == period_start
        assert period.period_end == period_end
        assert period.period_type == period_type
        assert period.group_id == group_id
        # Note: SQLAlchemy defaults are applied when saved to database
        assert BillingPeriod.__table__.columns['status'].default.arg == "active"
        assert BillingPeriod.__table__.columns['total_cost_usd'].default.arg == 0.00

    def test_billing_period_with_aggregated_data(self):
        """Test BillingPeriod model with aggregated billing data."""
        # Arrange
        period_start = datetime(2023, 2, 1, tzinfo=timezone.utc)
        period_end = datetime(2023, 2, 28, tzinfo=timezone.utc)
        total_cost_usd = Decimal("125.50")
        total_tokens = 50000
        total_prompt_tokens = 30000
        total_completion_tokens = 20000
        total_requests = 100
        model_breakdown = {
            "gpt-4": {"cost": 100.00, "tokens": 30000},
            "gpt-3.5-turbo": {"cost": 25.50, "tokens": 20000}
        }
        
        # Act
        period = BillingPeriod(
            period_start=period_start,
            period_end=period_end,
            total_cost_usd=total_cost_usd,
            total_tokens=total_tokens,
            total_prompt_tokens=total_prompt_tokens,
            total_completion_tokens=total_completion_tokens,
            total_requests=total_requests,
            model_breakdown=model_breakdown
        )
        
        # Assert
        assert period.total_cost_usd == total_cost_usd
        assert period.total_tokens == total_tokens
        assert period.total_prompt_tokens == total_prompt_tokens
        assert period.total_completion_tokens == total_completion_tokens
        assert period.total_requests == total_requests
        assert period.model_breakdown == model_breakdown

    def test_billing_period_status_transitions(self):
        """Test BillingPeriod status transitions."""
        # Arrange
        period_start = datetime(2023, 1, 1, tzinfo=timezone.utc)
        period_end = datetime(2023, 1, 31, tzinfo=timezone.utc)
        closed_at = datetime(2023, 2, 1, tzinfo=timezone.utc)
        
        # Act - Create closed period
        period = BillingPeriod(
            period_start=period_start,
            period_end=period_end,
            status="closed",
            closed_at=closed_at
        )
        
        # Assert
        assert period.status == "closed"
        assert period.closed_at == closed_at

    def test_billing_period_types(self):
        """Test different billing period types."""
        period_start = datetime(2023, 1, 1, tzinfo=timezone.utc)
        period_end = datetime(2023, 1, 31, tzinfo=timezone.utc)
        
        # Test different period types
        for period_type in ["daily", "weekly", "monthly", "custom"]:
            period = BillingPeriod(
                period_start=period_start,
                period_end=period_end,
                period_type=period_type
            )
            assert period.period_type == period_type

    def test_billing_period_table_name(self):
        """Test that the table name is correctly set."""
        # Act & Assert
        assert BillingPeriod.__tablename__ == "billing_periods"

    def test_billing_period_indexes(self):
        """Test that the model has the expected database indexes."""
        # Act
        indexes = BillingPeriod.__table_args__
        
        # Assert
        assert len(indexes) == 2
        
        # Check index names
        index_names = [index.name for index in indexes if hasattr(index, 'name')]
        expected_indexes = [
            'idx_period_group_dates',
            'idx_period_status_date'
        ]
        
        for expected_index in expected_indexes:
            assert expected_index in index_names


class TestBillingAlert:
    """Test cases for BillingAlert model."""

    def test_billing_alert_creation(self):
        """Test basic BillingAlert model creation."""
        # Arrange
        alert_name = "Monthly Cost Alert"
        alert_type = "cost_threshold"
        threshold_value = Decimal("100.00")
        threshold_period = "monthly"
        group_id = "group-123"
        
        # Act
        alert = BillingAlert(
            alert_name=alert_name,
            alert_type=alert_type,
            threshold_value=threshold_value,
            threshold_period=threshold_period,
            group_id=group_id
        )
        
        # Assert
        assert alert.alert_name == alert_name
        assert alert.alert_type == alert_type
        assert alert.threshold_value == threshold_value
        assert alert.threshold_period == threshold_period
        assert alert.group_id == group_id
        # Note: SQLAlchemy defaults are applied when saved to database
        assert BillingAlert.__table__.columns['is_active'].default.arg == "true"
        assert BillingAlert.__table__.columns['current_value'].default.arg == 0.00

    def test_billing_alert_with_notifications(self):
        """Test BillingAlert model with notification settings."""
        # Arrange
        alert_name = "Token Usage Alert"
        alert_type = "token_threshold"
        threshold_value = Decimal("1000000")
        notification_emails = ["admin@company.com", "billing@company.com"]
        user_email = "user@company.com"
        current_value = Decimal("750000")
        last_triggered = datetime(2023, 1, 15, tzinfo=timezone.utc)
        
        # Act
        alert = BillingAlert(
            alert_name=alert_name,
            alert_type=alert_type,
            threshold_value=threshold_value,
            notification_emails=notification_emails,
            user_email=user_email,
            current_value=current_value,
            last_triggered=last_triggered
        )
        
        # Assert
        assert alert.notification_emails == notification_emails
        assert alert.user_email == user_email
        assert alert.current_value == current_value
        assert alert.last_triggered == last_triggered

    def test_billing_alert_types(self):
        """Test different billing alert types."""
        threshold_value = Decimal("100.00")
        
        # Test different alert types
        for alert_type in ["cost_threshold", "token_threshold", "usage_spike"]:
            alert = BillingAlert(
                alert_name=f"Test {alert_type}",
                alert_type=alert_type,
                threshold_value=threshold_value
            )
            assert alert.alert_type == alert_type

    def test_billing_alert_periods(self):
        """Test different billing alert periods."""
        alert_name = "Test Alert"
        threshold_value = Decimal("50.00")
        
        # Test different periods
        for period in ["daily", "weekly", "monthly"]:
            alert = BillingAlert(
                alert_name=alert_name,
                alert_type="cost_threshold",
                threshold_value=threshold_value,
                threshold_period=period
            )
            assert alert.threshold_period == period

    def test_billing_alert_inactive(self):
        """Test BillingAlert in inactive state."""
        # Arrange & Act
        alert = BillingAlert(
            alert_name="Inactive Alert",
            alert_type="cost_threshold",
            threshold_value=Decimal("200.00"),
            is_active="false"
        )
        
        # Assert
        assert alert.is_active == "false"

    def test_billing_alert_with_metadata(self):
        """Test BillingAlert with metadata."""
        # Arrange
        alert_metadata = {
            "description": "Alert for production environment",
            "slack_webhook": "https://hooks.slack.com/webhook",
            "escalation_level": "high"
        }
        
        # Act
        alert = BillingAlert(
            alert_name="Production Alert",
            alert_type="cost_threshold",
            threshold_value=Decimal("500.00"),
            alert_metadata=alert_metadata
        )
        
        # Assert
        assert alert.alert_metadata == alert_metadata
        assert alert.alert_metadata["escalation_level"] == "high"

    def test_billing_alert_table_name(self):
        """Test that the table name is correctly set."""
        # Act & Assert
        assert BillingAlert.__tablename__ == "billing_alerts"


class TestGenerateBillingId:
    """Test cases for generate_billing_id function."""

    def test_generate_billing_id_function(self):
        """Test the generate_billing_id function."""
        # Act
        id1 = generate_billing_id()
        id2 = generate_billing_id()
        
        # Assert
        assert id1 is not None
        assert id2 is not None
        assert id1 != id2
        assert isinstance(id1, str)
        assert isinstance(id2, str)
        assert len(id1) == 36  # Standard UUID length
        assert len(id2) == 36

    def test_generate_billing_id_uniqueness(self):
        """Test that generate_billing_id generates unique IDs."""
        # Act
        ids = [generate_billing_id() for _ in range(10)]
        
        # Assert
        assert len(set(ids)) == 10  # All IDs should be unique


class TestBillingModelsIntegration:
    """Integration tests for billing models."""

    def test_billing_models_with_same_execution(self):
        """Test that billing models can reference the same execution."""
        # Arrange
        execution_id = "exec-integration-test"
        
        # Act - Create billing records for the same execution
        billing1 = LLMUsageBilling(
            execution_id=execution_id,
            execution_type="crew",
            model_name="gpt-4",
            model_provider="openai",
            cost_usd=Decimal("0.10")
        )
        
        billing2 = LLMUsageBilling(
            execution_id=execution_id,
            execution_type="crew",
            model_name="claude-3-sonnet",
            model_provider="anthropic",
            cost_usd=Decimal("0.08")
        )
        
        # Assert
        assert billing1.execution_id == billing2.execution_id
        assert billing1.model_name != billing2.model_name
        assert billing1.model_provider != billing2.model_provider

    def test_billing_period_aggregation_scenario(self):
        """Test a realistic billing period aggregation scenario."""
        # Arrange
        period_start = datetime(2023, 3, 1, tzinfo=timezone.utc)
        period_end = datetime(2023, 3, 31, tzinfo=timezone.utc)
        
        # Simulate aggregated data from multiple billing records
        total_cost = Decimal("245.75")
        model_costs = {
            "gpt-4": {"cost": 150.00, "requests": 50, "tokens": 75000},
            "gpt-3.5-turbo": {"cost": 75.25, "requests": 100, "tokens": 150000},
            "claude-3-sonnet": {"cost": 20.50, "requests": 25, "tokens": 30000}
        }
        
        # Act
        period = BillingPeriod(
            period_start=period_start,
            period_end=period_end,
            period_type="monthly",
            total_cost_usd=total_cost,
            total_tokens=255000,
            total_requests=175,
            model_breakdown=model_costs,
            group_id="production-group"
        )
        
        # Assert
        assert period.total_cost_usd == total_cost
        assert period.total_tokens == 255000
        assert period.total_requests == 175
        assert len(period.model_breakdown) == 3
        assert period.model_breakdown["gpt-4"]["cost"] == 150.00

    def test_billing_alert_threshold_scenarios(self):
        """Test different billing alert threshold scenarios."""
        # Test cost threshold alert
        cost_alert = BillingAlert(
            alert_name="High Cost Alert",
            alert_type="cost_threshold",
            threshold_value=Decimal("500.00"),
            threshold_period="monthly",
            current_value=Decimal("450.00"),
            group_id="production"
        )
        
        # Test token threshold alert
        token_alert = BillingAlert(
            alert_name="Token Usage Alert",
            alert_type="token_threshold",
            threshold_value=Decimal("1000000"),
            threshold_period="weekly",
            current_value=Decimal("850000"),
            user_email="heavy-user@company.com"
        )
        
        # Assert
        assert cost_alert.current_value < cost_alert.threshold_value
        assert token_alert.current_value < token_alert.threshold_value
        assert cost_alert.group_id == "production"
        assert token_alert.user_email == "heavy-user@company.com"