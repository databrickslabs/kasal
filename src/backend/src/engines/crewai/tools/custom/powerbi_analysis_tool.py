import logging
import asyncio
import json
from typing import Any, Optional, Type

from crewai.tools import BaseTool
from pydantic import BaseModel, Field, PrivateAttr, model_validator

logger = logging.getLogger(__name__)


class PowerBIAnalysisToolSchema(BaseModel):
    """Input schema for PowerBIAnalysisTool."""

    dashboard_id: str = Field(
        ..., description="Power BI dashboard/semantic model ID to analyze"
    )
    questions: list = Field(
        ..., description="Business questions to analyze using DAX"
    )
    workspace_id: Optional[str] = Field(
        None, description="Power BI workspace ID (uses default if not provided)"
    )
    dax_statement: Optional[str] = Field(
        None, description="Pre-generated DAX statement (optional, will be generated if not provided)"
    )

    @model_validator(mode='after')
    def validate_input(self) -> 'PowerBIAnalysisToolSchema':
        """Validate the input parameters."""
        if not self.questions and not self.dax_statement:
            raise ValueError("Either 'questions' or 'dax_statement' must be provided")
        return self


class PowerBIAnalysisTool(BaseTool):
    """
    A tool for complex Power BI analysis using Databricks job execution.

    This tool is designed for:
    - Heavy computational analysis
    - Long-running DAX queries
    - Complex data transformations
    - Result persistence to Databricks volumes
    - Integration with other data sources

    For simple, interactive queries, use PowerBIDAXTool instead.

    Architecture:
    1. Accepts business questions or DAX statements
    2. Triggers Databricks job with parameters
    3. Databricks notebook executes DAX against Power BI
    4. Results are processed and optionally stored
    5. Returns analysis results to agent
    """

    name: str = "Power BI Analysis (Databricks)"
    description: str = (
        "Execute complex Power BI analysis using Databricks jobs. "
        "Suitable for heavy computations, long-running queries, and advanced analytics. "
        "Provide 'dashboard_id' and 'questions' for analysis, or a pre-generated 'dax_statement'. "
        "Results are processed in Databricks and can be persisted to volumes."
    )
    args_schema: Type[BaseModel] = PowerBIAnalysisToolSchema

    _group_id: Optional[str] = PrivateAttr(default=None)
    _databricks_job_id: Optional[int] = PrivateAttr(default=None)

    def __init__(
        self,
        group_id: Optional[str] = None,
        databricks_job_id: Optional[int] = None,
        **kwargs
    ):
        """
        Initialize PowerBIAnalysisTool.

        Args:
            group_id: Group ID for multi-tenant support
            databricks_job_id: Databricks job ID for Power BI analysis (if pre-configured)
            **kwargs: Additional keyword arguments for BaseTool
        """
        super().__init__(**kwargs)
        self._group_id = group_id
        self._databricks_job_id = databricks_job_id
        logger.info(f"PowerBIAnalysisTool initialized for group: {group_id or 'default'}")

    def _run(self, **kwargs: Any) -> str:
        """
        Execute a Power BI analysis action via Databricks job.

        Args:
            dashboard_id (str): Power BI dashboard/semantic model ID
            questions (list): Business questions to analyze
            workspace_id (Optional[str]): Workspace ID
            dax_statement (Optional[str]): Pre-generated DAX statement

        Returns:
            str: Formatted analysis results
        """
        # Create a new event loop for synchronous context
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(self._execute_analysis(**kwargs))
            return result
        finally:
            loop.close()

    async def _execute_analysis(self, **kwargs) -> str:
        """
        Async implementation of analysis execution via Databricks.

        Args:
            **kwargs: Analysis parameters

        Returns:
            str: Formatted analysis results
        """
        dashboard_id = kwargs.get('dashboard_id')
        questions = kwargs.get('questions', [])
        workspace_id = kwargs.get('workspace_id')
        dax_statement = kwargs.get('dax_statement')

        try:
            # Import here to avoid circular dependency
            from .databricks_jobs_tool import DatabricksJobsTool

            # Create DatabricksJobsTool instance
            databricks_tool = DatabricksJobsTool(
                # Pass through any databricks-specific config
                databricks_host=None,  # Will be auto-detected
                tool_config={},
                token_required=False
            )

            # Prepare job parameters
            job_params = {
                "dashboard_id": dashboard_id,
                "questions": questions,
                "workspace_id": workspace_id,
                "dax_statement": dax_statement
            }

            # If job_id is configured, run it; otherwise, return instructions
            if self._databricks_job_id:
                logger.info(f"Running Databricks job {self._databricks_job_id} for Power BI analysis")

                # Trigger job run
                run_result = databricks_tool._run(
                    action="run",
                    job_id=self._databricks_job_id,
                    job_params=job_params
                )

                # Parse run_id from result
                # Expected format: "✅ Job run started successfully\nRun ID: 12345\n..."
                run_id = self._extract_run_id(run_result)

                if run_id:
                    # Monitor job execution
                    logger.info(f"Monitoring Databricks job run {run_id}")

                    # Poll for completion
                    import time
                    max_wait = 300  # 5 minutes
                    poll_interval = 5  # 5 seconds
                    elapsed = 0

                    while elapsed < max_wait:
                        monitor_result = databricks_tool._run(
                            action="monitor",
                            run_id=run_id
                        )

                        if "completed successfully" in monitor_result.lower():
                            return self._format_success_result(monitor_result, dashboard_id, questions)
                        elif "failed" in monitor_result.lower() or "error" in monitor_result.lower():
                            return f"❌ Analysis Failed\n\n{monitor_result}"

                        # Still running, wait and retry
                        time.sleep(poll_interval)
                        elapsed += poll_interval

                    return f"⏱️ Analysis Timeout\n\nJob run {run_id} did not complete within {max_wait} seconds."
                else:
                    return f"❌ Failed to extract run ID from result:\n{run_result}"

            else:
                # No job configured - return instructions for setup
                return self._format_setup_instructions(dashboard_id, questions, job_params)

        except Exception as e:
            logger.error(f"Error executing Power BI analysis: {e}", exc_info=True)
            return f"❌ Error executing analysis: {str(e)}"

    def _extract_run_id(self, result: str) -> Optional[int]:
        """Extract run ID from Databricks job result."""
        try:
            # Look for "Run ID: 12345" pattern
            import re
            match = re.search(r'Run ID:\s*(\d+)', result)
            if match:
                return int(match.group(1))
        except Exception as e:
            logger.error(f"Error extracting run ID: {e}")
        return None

    def _format_success_result(self, result: str, dashboard_id: str, questions: list) -> str:
        """Format successful analysis result."""
        output = f"✅ Power BI Analysis Complete\n\n"
        output += f"📊 Dashboard: {dashboard_id}\n"
        output += f"❓ Questions Analyzed ({len(questions)}):\n"
        for i, q in enumerate(questions, 1):
            output += f"  {i}. {q}\n"
        output += f"\n{result}\n"
        return output

    def _format_setup_instructions(self, dashboard_id: str, questions: list, job_params: dict) -> str:
        """Format setup instructions when no Databricks job is configured."""
        output = f"⚙️ Power BI Analysis Setup Required\n\n"
        output += f"To execute Power BI analysis via Databricks, you need to:\n\n"
        output += f"1. **Create a Databricks job** with the Power BI analysis notebook\n"
        output += f"2. **Configure the job ID** in this tool\n\n"
        output += f"**Analysis Parameters:**\n"
        output += f"```json\n{json.dumps(job_params, indent=2)}\n```\n\n"
        output += f"**Notebook Location:**\n"
        output += f"`scripts/dax_analysis_job.py` (from your ask.md guide)\n\n"
        output += f"**Alternative:**\n"
        output += f"For simple queries, use `PowerBIDAXTool` instead for direct execution.\n"
        return output
