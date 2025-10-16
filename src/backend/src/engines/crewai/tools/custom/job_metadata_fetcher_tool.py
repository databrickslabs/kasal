"""
Job Metadata Fetcher Tool

CrewAI tool that fetches output from a completed Databricks job run.
This tool extracts the notebook exit output from any job that uses dbutils.notebook.exit().

Generic tool that can be used for any job type, not just Power BI metadata extraction.
"""

import json
import logging
from typing import Any, Dict, Optional, Type

from crewai.tools import BaseTool
from pydantic import BaseModel, Field
from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)


class JobMetadataFetcherSchema(BaseModel):
    """Input schema for Job Metadata Fetcher Tool."""

    run_id: str = Field(
        ...,
        description="Databricks job run ID from the completed job"
    )
    task_key: Optional[str] = Field(
        None,
        description="Optional task key for multi-task jobs. If the job has multiple tasks, specify which task's output to retrieve. "
                    "If not provided and job has only one task, will auto-detect."
    )
    extract_key: Optional[str] = Field(
        None,
        description="Optional key to extract from the result (e.g., 'metadata', 'compact_metadata'). "
                    "If not provided, returns the entire result."
    )


class JobMetadataFetcherTool(BaseTool):
    """
    Job Metadata Fetcher Tool for CrewAI agents.

    This tool fetches the notebook exit output from a completed Databricks job.
    Use this after running any notebook job via DatabricksJobsTool that exits with data.

    The tool expects jobs to use: dbutils.notebook.exit(json.dumps(result_data))

    Example workflow:
        1. Agent 1 uses DatabricksJobsTool to run a notebook job
        2. Agent 1 uses JobMetadataFetcherTool to get the output from run_id
        3. Agent 2 receives the output and processes it further

    Example notebook exit:
        result = {"status": "success", "metadata": {...}, "tables_count": 5}
        dbutils.notebook.exit(json.dumps(result))
    """

    name: str = "Job Metadata Fetcher"
    description: str = (
        "Fetch output from a completed Databricks notebook job. "
        "Provide the run_id from the job, and this tool will "
        "return the data that was passed to dbutils.notebook.exit(). "
        "For multi-task jobs, optionally specify task_key to retrieve output from a specific task. "
        "Optionally specify extract_key to get a specific field from the result."
    )
    args_schema: Type[BaseModel] = JobMetadataFetcherSchema

    def __init__(self, **kwargs: Any) -> None:
        """Initialize the Job Metadata Fetcher Tool."""
        super().__init__(**kwargs)
        self.workspace_client = WorkspaceClient()

    def _run(self, run_id: str, task_key: Optional[str] = None, extract_key: Optional[str] = None, **kwargs: Any) -> str:
        """
        Fetch output from job run.

        Args:
            run_id: Databricks job run ID
            task_key: Optional task key for multi-task jobs
            extract_key: Optional key to extract from result (e.g., 'metadata')

        Returns:
            JSON string containing job output or extracted field
        """
        try:
            logger.info(f"Fetching output from run_id: {run_id}")

            # First, check if this is a multi-task job
            run_info = self.workspace_client.jobs.get_run(int(run_id))

            # If job has multiple tasks, we need to get output from a specific task
            if run_info.tasks and len(run_info.tasks) > 1:
                logger.info(f"Multi-task job detected with {len(run_info.tasks)} tasks")

                # If no task_key specified, try to auto-detect
                if not task_key:
                    # Use the first task by default
                    task_key = run_info.tasks[0].task_key
                    logger.info(f"Auto-detected task_key: {task_key}")

                # Find the task run
                task_run = None
                for task in run_info.tasks:
                    if task.task_key == task_key:
                        task_run = task
                        break

                if not task_run:
                    available_tasks = [t.task_key for t in run_info.tasks]
                    return json.dumps({
                        "error": f"Task '{task_key}' not found in job run",
                        "run_id": run_id,
                        "available_tasks": available_tasks,
                        "details": f"Please specify one of the available task_key values: {', '.join(available_tasks)}"
                    })

                # Get output from the specific task run
                output = self.workspace_client.jobs.get_run_output(task_run.run_id)
                logger.info(f"Fetching output from task '{task_key}' (task_run_id: {task_run.run_id})")

            elif run_info.tasks and len(run_info.tasks) == 1:
                # Single task job - get output from the task run
                task_run = run_info.tasks[0]
                output = self.workspace_client.jobs.get_run_output(task_run.run_id)
                logger.info(f"Single task job - fetching output from task '{task_run.task_key}'")

            else:
                # Legacy single-task job without tasks array - try direct run output
                try:
                    output = self.workspace_client.jobs.get_run_output(int(run_id))
                    logger.info("Legacy job format - fetching output directly from run_id")
                except Exception as e:
                    if "multiple tasks" in str(e).lower():
                        return json.dumps({
                            "error": "Multi-task job detected but could not determine task structure",
                            "run_id": run_id,
                            "details": "Please provide task_key parameter to specify which task's output to retrieve"
                        })
                    raise

            if not output.notebook_output or not output.notebook_output.result:
                return json.dumps({
                    "error": "No output found from job run",
                    "run_id": run_id,
                    "task_key": task_key,
                    "details": "The notebook may not have called dbutils.notebook.exit() or the job failed."
                })

            # Parse result
            result_data = json.loads(output.notebook_output.result)

            # Extract specific key if requested
            if extract_key:
                if extract_key not in result_data:
                    return json.dumps({
                        "error": f"Key '{extract_key}' not found in job output",
                        "run_id": run_id,
                        "available_keys": list(result_data.keys())
                    })

                extracted_data = result_data[extract_key]
                logger.info(f"✅ Extracted '{extract_key}' from job output")

                return json.dumps({
                    "success": True,
                    "run_id": run_id,
                    "task_key": task_key,
                    "extract_key": extract_key,
                    "data": extracted_data,
                    "message": f"Successfully extracted '{extract_key}' from job output"
                }, indent=2)

            # Return full result if no extract_key specified
            logger.info(f"✅ Fetched complete job output with keys: {list(result_data.keys())}")

            return json.dumps({
                "success": True,
                "run_id": run_id,
                "task_key": task_key,
                "data": result_data,
                "available_keys": list(result_data.keys()),
                "message": "Successfully fetched complete job output"
            }, indent=2)

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse job output as JSON: {str(e)}")
            return json.dumps({
                "error": "Job output is not valid JSON",
                "run_id": run_id,
                "details": str(e)
            })
        except Exception as e:
            logger.error(f"Error fetching output from run_id {run_id}: {str(e)}", exc_info=True)
            return json.dumps({
                "error": f"Failed to fetch job output: {str(e)}",
                "run_id": run_id
            })
