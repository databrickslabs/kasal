"""
Unit tests for scheduler schemas.

Tests the functionality of Pydantic schemas for scheduler job operations
including validation, serialization, and field constraints.
"""
import pytest
from datetime import datetime
from pydantic import ValidationError
from typing import Dict, Any

from src.schemas.scheduler import (
    SchedulerJobBase, SchedulerJobSchema, SchedulerJobCreate,
    SchedulerJobUpdate, SchedulerJobResponse
)


class TestSchedulerJobBase:
    """Test cases for SchedulerJobBase schema."""
    
    def test_valid_scheduler_job_base_minimal(self):
        """Test SchedulerJobBase with minimal required fields."""
        data = {
            "name": "test-job",
            "schedule": "0 9 * * MON-FRI"
        }
        job = SchedulerJobBase(**data)
        assert job.name == "test-job"
        assert job.schedule == "0 9 * * MON-FRI"
        assert job.description is None  # Default
        assert job.enabled is True  # Default
        assert job.job_data == {}  # Default

    def test_valid_scheduler_job_base_full(self):
        """Test SchedulerJobBase with all fields specified."""
        data = {
            "name": "full-job",
            "description": "A comprehensive scheduler job",
            "schedule": "0 */6 * * *",
            "enabled": False,
            "job_data": {
                "type": "data_processing",
                "source": "database",
                "output": "report.pdf",
                "notifications": ["admin@example.com"]
            }
        }
        job = SchedulerJobBase(**data)
        assert job.name == "full-job"
        assert job.description == "A comprehensive scheduler job"
        assert job.schedule == "0 */6 * * *"
        assert job.enabled is False
        assert job.job_data == {
            "type": "data_processing",
            "source": "database",
            "output": "report.pdf",
            "notifications": ["admin@example.com"]
        }

    def test_scheduler_job_base_missing_required_fields(self):
        """Test SchedulerJobBase validation with missing required fields."""
        with pytest.raises(ValidationError) as exc_info:
            SchedulerJobBase(name="incomplete-job")
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "schedule" in missing_fields

    def test_scheduler_job_base_boolean_conversions(self):
        """Test SchedulerJobBase boolean field conversions."""
        data = {
            "name": "bool-job",
            "schedule": "0 12 * * *",
            "enabled": "false"
        }
        job = SchedulerJobBase(**data)
        assert job.enabled is False

    def test_scheduler_job_base_empty_job_data(self):
        """Test SchedulerJobBase with empty job_data."""
        data = {
            "name": "empty-data-job",
            "schedule": "0 15 * * *",
            "job_data": {}
        }
        job = SchedulerJobBase(**data)
        assert job.job_data == {}

    def test_scheduler_job_base_complex_job_data(self):
        """Test SchedulerJobBase with complex job_data structure."""
        complex_data = {
            "workflow": {
                "steps": [
                    {"action": "extract", "source": "api"},
                    {"action": "transform", "rules": ["normalize", "validate"]},
                    {"action": "load", "destination": "warehouse"}
                ]
            },
            "retry_config": {
                "max_attempts": 3,
                "backoff_factor": 2,
                "timeout": 300
            },
            "monitoring": {
                "alerts": True,
                "metrics": ["duration", "success_rate", "error_count"]
            }
        }
        
        data = {
            "name": "complex-job",
            "schedule": "0 3 * * *",
            "job_data": complex_data
        }
        job = SchedulerJobBase(**data)
        assert job.job_data == complex_data
        assert job.job_data["workflow"]["steps"][0]["action"] == "extract"
        assert job.job_data["retry_config"]["max_attempts"] == 3
        assert job.job_data["monitoring"]["alerts"] is True


class TestSchedulerJobSchema:
    """Test cases for SchedulerJobSchema schema."""
    
    def test_valid_scheduler_job_schema(self):
        """Test SchedulerJobSchema with all required fields."""
        now = datetime.now()
        last_run = datetime(2023, 12, 1, 9, 0, 0)
        next_run = datetime(2023, 12, 2, 9, 0, 0)
        
        data = {
            "id": 1,
            "name": "schema-job",
            "description": "Test scheduler job schema",
            "schedule": "0 9 * * *",
            "enabled": True,
            "job_data": {"task": "backup", "location": "/backups"},
            "created_at": now,
            "updated_at": now,
            "last_run_at": last_run,
            "next_run_at": next_run
        }
        job = SchedulerJobSchema(**data)
        assert job.id == 1
        assert job.name == "schema-job"
        assert job.description == "Test scheduler job schema"
        assert job.schedule == "0 9 * * *"
        assert job.enabled is True
        assert job.job_data == {"task": "backup", "location": "/backups"}
        assert job.created_at == now
        assert job.updated_at == now
        assert job.last_run_at == last_run
        assert job.next_run_at == next_run

    def test_scheduler_job_schema_config(self):
        """Test SchedulerJobSchema model config."""
        assert hasattr(SchedulerJobSchema, 'model_config')
        assert SchedulerJobSchema.model_config['from_attributes'] is True

    def test_scheduler_job_schema_missing_fields(self):
        """Test SchedulerJobSchema validation with missing fields."""
        now = datetime.now()
        with pytest.raises(ValidationError) as exc_info:
            SchedulerJobSchema(
                name="incomplete-job",
                schedule="0 9 * * *",
                created_at=now,
                updated_at=now
            )
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "id" in missing_fields

    def test_scheduler_job_schema_optional_timestamps(self):
        """Test SchedulerJobSchema with optional timestamp fields."""
        now = datetime.now()
        data = {
            "id": 2,
            "name": "optional-timestamps-job",
            "schedule": "0 12 * * *",
            "enabled": True,
            "job_data": {},
            "created_at": now,
            "updated_at": now
        }
        job = SchedulerJobSchema(**data)
        assert job.id == 2
        assert job.last_run_at is None
        assert job.next_run_at is None

    def test_scheduler_job_schema_datetime_conversion(self):
        """Test SchedulerJobSchema with datetime string conversion."""
        data = {
            "id": 3,
            "name": "datetime-job",
            "schedule": "0 15 * * *",
            "enabled": True,
            "job_data": {},
            "created_at": "2023-01-01T10:00:00",
            "updated_at": "2023-01-01T11:00:00",
            "last_run_at": "2023-01-01T12:00:00",
            "next_run_at": "2023-01-02T12:00:00"
        }
        job = SchedulerJobSchema(**data)
        assert job.id == 3
        assert isinstance(job.created_at, datetime)
        assert isinstance(job.updated_at, datetime)
        assert isinstance(job.last_run_at, datetime)
        assert isinstance(job.next_run_at, datetime)


class TestSchedulerJobCreate:
    """Test cases for SchedulerJobCreate schema."""
    
    def test_scheduler_job_create_inheritance(self):
        """Test that SchedulerJobCreate inherits from SchedulerJobBase."""
        data = {
            "name": "create-job",
            "description": "Job for creation testing",
            "schedule": "0 10 * * *",
            "enabled": False,
            "job_data": {"operation": "cleanup"}
        }
        create_job = SchedulerJobCreate(**data)
        
        # Should have all base class attributes
        assert hasattr(create_job, 'name')
        assert hasattr(create_job, 'description')
        assert hasattr(create_job, 'schedule')
        assert hasattr(create_job, 'enabled')
        assert hasattr(create_job, 'job_data')
        
        # Should behave like base class
        assert create_job.name == "create-job"
        assert create_job.description == "Job for creation testing"
        assert create_job.schedule == "0 10 * * *"
        assert create_job.enabled is False
        assert create_job.job_data == {"operation": "cleanup"}

    def test_scheduler_job_create_minimal(self):
        """Test SchedulerJobCreate with minimal required fields."""
        data = {
            "name": "minimal-create-job",
            "schedule": "0 8 * * MON-FRI"
        }
        create_job = SchedulerJobCreate(**data)
        assert create_job.name == "minimal-create-job"
        assert create_job.schedule == "0 8 * * MON-FRI"
        assert create_job.description is None
        assert create_job.enabled is True  # Default
        assert create_job.job_data == {}  # Default

    def test_scheduler_job_create_with_complex_data(self):
        """Test SchedulerJobCreate with complex job data."""
        complex_job_data = {
            "pipeline": {
                "stages": ["ingestion", "processing", "validation", "output"],
                "config": {
                    "parallel": True,
                    "max_workers": 4,
                    "timeout_per_stage": 600
                }
            },
            "notifications": {
                "on_success": ["success@example.com"],
                "on_failure": ["alerts@example.com", "oncall@example.com"],
                "channels": ["email", "slack"]
            }
        }
        
        data = {
            "name": "complex-create-job",
            "description": "Complex data processing job",
            "schedule": "0 2 * * *",
            "job_data": complex_job_data
        }
        create_job = SchedulerJobCreate(**data)
        assert create_job.name == "complex-create-job"
        assert create_job.job_data == complex_job_data
        assert create_job.job_data["pipeline"]["config"]["max_workers"] == 4
        assert "slack" in create_job.job_data["notifications"]["channels"]


class TestSchedulerJobUpdate:
    """Test cases for SchedulerJobUpdate schema."""
    
    def test_scheduler_job_update_all_optional(self):
        """Test that all SchedulerJobUpdate fields are optional."""
        update = SchedulerJobUpdate()
        assert update.name is None
        assert update.description is None
        assert update.schedule is None
        assert update.enabled is None
        assert update.job_data is None

    def test_scheduler_job_update_partial(self):
        """Test SchedulerJobUpdate with partial fields."""
        update_data = {
            "name": "updated-job-name",
            "enabled": False
        }
        update = SchedulerJobUpdate(**update_data)
        assert update.name == "updated-job-name"
        assert update.enabled is False
        assert update.description is None
        assert update.schedule is None
        assert update.job_data is None

    def test_scheduler_job_update_full(self):
        """Test SchedulerJobUpdate with all fields."""
        update_data = {
            "name": "fully-updated-job",
            "description": "Updated job description",
            "schedule": "0 14 * * *",
            "enabled": True,
            "job_data": {
                "updated": True,
                "version": "2.0",
                "changes": ["performance improvements", "bug fixes"]
            }
        }
        update = SchedulerJobUpdate(**update_data)
        assert update.name == "fully-updated-job"
        assert update.description == "Updated job description"
        assert update.schedule == "0 14 * * *"
        assert update.enabled is True
        assert update.job_data == {
            "updated": True,
            "version": "2.0",
            "changes": ["performance improvements", "bug fixes"]
        }

    def test_scheduler_job_update_none_values(self):
        """Test SchedulerJobUpdate with explicit None values."""
        update_data = {
            "name": None,
            "description": None,
            "schedule": None,
            "enabled": None,
            "job_data": None
        }
        update = SchedulerJobUpdate(**update_data)
        assert update.name is None
        assert update.description is None
        assert update.schedule is None
        assert update.enabled is None
        assert update.job_data is None

    def test_scheduler_job_update_empty_job_data(self):
        """Test SchedulerJobUpdate with empty job_data."""
        update_data = {
            "job_data": {}
        }
        update = SchedulerJobUpdate(**update_data)
        assert update.job_data == {}
        assert update.name is None

    def test_scheduler_job_update_partial_job_data(self):
        """Test SchedulerJobUpdate with partial job_data updates."""
        update_data = {
            "enabled": False,
            "job_data": {
                "maintenance_mode": True,
                "reason": "scheduled maintenance"
            }
        }
        update = SchedulerJobUpdate(**update_data)
        assert update.enabled is False
        assert update.job_data["maintenance_mode"] is True
        assert update.job_data["reason"] == "scheduled maintenance"


class TestSchedulerJobResponse:
    """Test cases for SchedulerJobResponse schema."""
    
    def test_scheduler_job_response_inheritance(self):
        """Test that SchedulerJobResponse inherits from SchedulerJobSchema."""
        now = datetime.now()
        data = {
            "id": 10,
            "name": "response-job",
            "description": "Response testing job",
            "schedule": "0 16 * * *",
            "enabled": True,
            "job_data": {"response": "test"},
            "created_at": now,
            "updated_at": now
        }
        response_job = SchedulerJobResponse(**data)
        
        # Should have all SchedulerJobSchema attributes
        assert hasattr(response_job, 'id')
        assert hasattr(response_job, 'name')
        assert hasattr(response_job, 'description')
        assert hasattr(response_job, 'schedule')
        assert hasattr(response_job, 'enabled')
        assert hasattr(response_job, 'job_data')
        assert hasattr(response_job, 'created_at')
        assert hasattr(response_job, 'updated_at')
        assert hasattr(response_job, 'last_run_at')
        assert hasattr(response_job, 'next_run_at')
        
        # Should behave like SchedulerJobSchema
        assert response_job.id == 10
        assert response_job.name == "response-job"
        assert response_job.description == "Response testing job"
        assert response_job.schedule == "0 16 * * *"
        assert response_job.enabled is True
        assert response_job.job_data == {"response": "test"}
        assert response_job.created_at == now
        assert response_job.updated_at == now

    def test_scheduler_job_response_with_timestamps(self):
        """Test SchedulerJobResponse with all timestamp fields."""
        now = datetime.now()
        last_run = datetime(2023, 12, 1, 16, 0, 0)
        next_run = datetime(2023, 12, 2, 16, 0, 0)
        
        data = {
            "id": 11,
            "name": "timestamp-response-job",
            "schedule": "0 16 * * *",
            "enabled": True,
            "job_data": {},
            "created_at": now,
            "updated_at": now,
            "last_run_at": last_run,
            "next_run_at": next_run
        }
        response_job = SchedulerJobResponse(**data)
        assert response_job.id == 11
        assert response_job.last_run_at == last_run
        assert response_job.next_run_at == next_run


class TestSchemaIntegration:
    """Integration tests for scheduler schema interactions."""
    
    def test_scheduler_job_workflow(self):
        """Test complete scheduler job workflow."""
        # Create job
        create_data = {
            "name": "workflow-job",
            "description": "Complete workflow test job",
            "schedule": "0 9 * * MON-FRI",
            "enabled": True,
            "job_data": {
                "workflow_type": "data_processing",
                "input_source": "api",
                "output_destination": "database",
                "retry_policy": {
                    "max_retries": 3,
                    "retry_delay": 300
                }
            }
        }
        create_job = SchedulerJobCreate(**create_data)
        
        # Update job
        update_data = {
            "description": "Updated workflow test job",
            "enabled": False,
            "job_data": {
                "workflow_type": "data_processing",
                "input_source": "api",
                "output_destination": "warehouse",  # Changed
                "retry_policy": {
                    "max_retries": 5,  # Changed
                    "retry_delay": 300
                },
                "maintenance_mode": True  # Added
            }
        }
        update_job = SchedulerJobUpdate(**update_data)
        
        # Simulate database entity
        now = datetime.now()
        last_run = datetime(2023, 12, 1, 9, 0, 0)
        next_run = datetime(2023, 12, 4, 9, 0, 0)  # Next Monday
        
        db_data = {
            "id": 1,
            "name": create_job.name,
            "description": update_data["description"],
            "schedule": create_job.schedule,
            "enabled": update_data["enabled"],
            "job_data": update_data["job_data"],
            "created_at": now,
            "updated_at": now,
            "last_run_at": last_run,
            "next_run_at": next_run
        }
        job_response = SchedulerJobResponse(**db_data)
        
        # Verify the complete workflow
        assert create_job.name == "workflow-job"
        assert create_job.enabled is True
        assert create_job.job_data["retry_policy"]["max_retries"] == 3
        
        assert update_job.description == "Updated workflow test job"
        assert update_job.enabled is False
        assert update_job.job_data["retry_policy"]["max_retries"] == 5
        assert update_job.job_data["maintenance_mode"] is True
        
        assert job_response.id == 1
        assert job_response.name == "workflow-job"
        assert job_response.description == "Updated workflow test job"
        assert job_response.enabled is False
        assert job_response.job_data["output_destination"] == "warehouse"
        assert job_response.last_run_at == last_run
        assert job_response.next_run_at == next_run

    def test_scheduler_job_configuration_scenarios(self):
        """Test different scheduler job configuration scenarios."""
        # Basic scheduled task
        basic_job = SchedulerJobCreate(
            name="basic-cleanup-job",
            schedule="0 2 * * *",  # Daily at 2 AM
            job_data={"task": "cleanup_temp_files", "directory": "/tmp"}
        )
        assert basic_job.name == "basic-cleanup-job"
        assert basic_job.enabled is True  # Default
        assert basic_job.job_data["task"] == "cleanup_temp_files"
        
        # Complex data pipeline job
        pipeline_job = SchedulerJobCreate(
            name="data-pipeline-job",
            description="Multi-stage data processing pipeline",
            schedule="0 4 * * *",  # Daily at 4 AM
            job_data={
                "pipeline": {
                    "stages": [
                        {
                            "name": "extract",
                            "type": "api_call",
                            "config": {
                                "endpoint": "https://api.example.com/data",
                                "auth": {"type": "bearer_token"},
                                "rate_limit": 100
                            }
                        },
                        {
                            "name": "transform",
                            "type": "data_processing",
                            "config": {
                                "rules": ["normalize", "validate", "enrich"],
                                "parallel": True,
                                "max_workers": 4
                            }
                        },
                        {
                            "name": "load",
                            "type": "database_insert",
                            "config": {
                                "connection": "data_warehouse",
                                "table": "processed_data",
                                "batch_size": 1000
                            }
                        }
                    ]
                },
                "error_handling": {
                    "retry_policy": {"max_retries": 3, "backoff": "exponential"},
                    "notifications": ["admin@example.com"],
                    "fallback_action": "log_and_continue"
                }
            }
        )
        assert pipeline_job.name == "data-pipeline-job"
        assert len(pipeline_job.job_data["pipeline"]["stages"]) == 3
        assert pipeline_job.job_data["pipeline"]["stages"][1]["config"]["max_workers"] == 4
        assert pipeline_job.job_data["error_handling"]["retry_policy"]["max_retries"] == 3
        
        # Monitoring and alerting job
        monitoring_job = SchedulerJobCreate(
            name="system-monitoring-job",
            description="Monitor system health and send alerts",
            schedule="*/15 * * * *",  # Every 15 minutes
            job_data={
                "monitors": [
                    {
                        "name": "cpu_usage",
                        "threshold": 80,
                        "action": "alert"
                    },
                    {
                        "name": "memory_usage", 
                        "threshold": 90,
                        "action": "alert"
                    },
                    {
                        "name": "disk_space",
                        "threshold": 85,
                        "action": "alert"
                    }
                ],
                "alert_channels": {
                    "email": ["ops@example.com"],
                    "slack": ["#alerts"],
                    "pagerduty": {"service_key": "SERVICE_KEY"}
                }
            }
        )
        assert monitoring_job.schedule == "*/15 * * * *"
        assert len(monitoring_job.job_data["monitors"]) == 3
        assert monitoring_job.job_data["monitors"][0]["threshold"] == 80
        assert "slack" in monitoring_job.job_data["alert_channels"]

    def test_scheduler_job_update_scenarios(self):
        """Test different scheduler job update scenarios."""
        # Enable/disable job
        toggle_update = SchedulerJobUpdate(enabled=False)
        assert toggle_update.enabled is False
        assert toggle_update.name is None
        
        # Schedule change
        schedule_update = SchedulerJobUpdate(
            schedule="0 6 * * *",  # Change from 9 AM to 6 AM
            job_data={"note": "Moved to earlier time for better performance"}
        )
        assert schedule_update.schedule == "0 6 * * *"
        assert schedule_update.job_data["note"] == "Moved to earlier time for better performance"
        
        # Configuration update
        config_update = SchedulerJobUpdate(
            description="Updated with new configuration parameters",
            job_data={
                "performance_optimizations": {
                    "batch_size": 2000,  # Increased from 1000
                    "parallel_workers": 8,  # Increased from 4
                    "memory_limit": "4GB"  # Added memory limit
                },
                "monitoring": {
                    "enabled": True,
                    "metrics": ["duration", "throughput", "error_rate"],
                    "dashboard_url": "https://monitoring.example.com/job-123"
                }
            }
        )
        assert config_update.description == "Updated with new configuration parameters"
        assert config_update.job_data["performance_optimizations"]["batch_size"] == 2000
        assert config_update.job_data["monitoring"]["enabled"] is True
        
        # Maintenance mode update
        maintenance_update = SchedulerJobUpdate(
            enabled=False,
            job_data={
                "maintenance_mode": True,
                "maintenance_reason": "Database migration in progress",
                "estimated_downtime": "2 hours",
                "contact": "devops@example.com"
            }
        )
        assert maintenance_update.enabled is False
        assert maintenance_update.job_data["maintenance_mode"] is True
        assert maintenance_update.job_data["estimated_downtime"] == "2 hours"

    def test_scheduler_job_response_scenarios(self):
        """Test different scheduler job response scenarios."""
        now = datetime.now()
        
        # Successful job with execution history
        successful_job = SchedulerJobResponse(
            id=1,
            name="successful-job",
            description="Job that runs successfully",
            schedule="0 12 * * *",
            enabled=True,
            job_data={
                "status": "completed",
                "last_execution": {
                    "duration": 120,  # seconds
                    "records_processed": 1500,
                    "success_rate": 1.0
                }
            },
            created_at=now,
            updated_at=now,
            last_run_at=datetime(2023, 12, 1, 12, 0, 0),
            next_run_at=datetime(2023, 12, 2, 12, 0, 0)
        )
        assert successful_job.id == 1
        assert successful_job.job_data["status"] == "completed"
        assert successful_job.job_data["last_execution"]["records_processed"] == 1500
        
        # Failed job with error information
        failed_job = SchedulerJobResponse(
            id=2,
            name="failed-job",
            description="Job that encountered errors",
            schedule="0 14 * * *",
            enabled=True,
            job_data={
                "status": "failed",
                "last_execution": {
                    "error": "Connection timeout to database",
                    "error_code": "DB_TIMEOUT",
                    "retry_count": 3,
                    "next_retry_at": "2023-12-01T15:00:00Z"
                }
            },
            created_at=now,
            updated_at=now,
            last_run_at=datetime(2023, 12, 1, 14, 0, 0),
            next_run_at=datetime(2023, 12, 1, 15, 0, 0)  # Retry in 1 hour
        )
        assert failed_job.id == 2
        assert failed_job.job_data["status"] == "failed"
        assert failed_job.job_data["last_execution"]["error_code"] == "DB_TIMEOUT"
        
        # Disabled job
        disabled_job = SchedulerJobResponse(
            id=3,
            name="disabled-job",
            description="Job that is currently disabled",
            schedule="0 16 * * *",
            enabled=False,
            job_data={
                "status": "disabled",
                "disabled_reason": "Pending system upgrade",
                "disabled_at": "2023-11-30T10:00:00Z"
            },
            created_at=now,
            updated_at=now,
            last_run_at=datetime(2023, 11, 30, 16, 0, 0),
            next_run_at=None  # No next run since disabled
        )
        assert disabled_job.id == 3
        assert disabled_job.enabled is False
        assert disabled_job.job_data["disabled_reason"] == "Pending system upgrade"
        assert disabled_job.next_run_at is None