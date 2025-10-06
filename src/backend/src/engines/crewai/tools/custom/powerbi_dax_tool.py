import logging
import asyncio
from typing import Any, Optional, Type

from crewai.tools import BaseTool
from pydantic import BaseModel, Field, PrivateAttr, model_validator

from src.schemas.powerbi_config import DAXQueryRequest, DAXQueryResponse

logger = logging.getLogger(__name__)


class PowerBIDAXToolSchema(BaseModel):
    """Input schema for PowerBIDAXTool."""

    action: str = Field(
        ...,
        description="Action to perform: 'query' (execute DAX query), 'analyze' (analyze with questions)"
    )
    dax_query: Optional[str] = Field(
        None, description="DAX query to execute (required for 'query' action)"
    )
    semantic_model_id: Optional[str] = Field(
        None, description="Power BI semantic model ID (uses default if not provided)"
    )
    workspace_id: Optional[str] = Field(
        None, description="Power BI workspace ID (uses default if not provided)"
    )
    questions: Optional[list] = Field(
        None, description="Business questions to analyze (for 'analyze' action)"
    )

    @model_validator(mode='after')
    def validate_input(self) -> 'PowerBIDAXToolSchema':
        """Validate the input parameters based on action."""
        action = self.action.lower()

        if action not in ['query', 'analyze']:
            raise ValueError(f"Invalid action '{action}'. Must be one of: query, analyze")

        if action == 'query' and not self.dax_query:
            raise ValueError("dax_query is required for action 'query'")

        if action == 'analyze' and not self.questions:
            raise ValueError("questions are required for action 'analyze'")

        return self


class PowerBIDAXTool(BaseTool):
    """
    A tool for executing DAX queries against Power BI semantic models.

    This tool enables interaction with Power BI:
    - Execute DAX queries directly
    - Analyze data with business questions
    - Retrieve data from Power BI datasets

    Authentication methods supported:
    - Service Principal (client credentials)
    - Username/Password authentication
    """

    name: str = "Power BI DAX Analyzer"
    description: str = (
        "Execute DAX queries against Power BI semantic models or analyze data with business questions. "
        "Provide 'action' parameter with values: 'query' (execute DAX) or 'analyze' (answer questions). "
        "For 'query' action, provide 'dax_query'. For 'analyze' action, provide 'questions' list."
    )
    args_schema: Type[BaseModel] = PowerBIDAXToolSchema

    _group_id: Optional[str] = PrivateAttr(default=None)

    def __init__(
        self,
        group_id: Optional[str] = None,
        **kwargs
    ):
        """
        Initialize PowerBIDAXTool.

        Args:
            group_id: Group ID for multi-tenant support
            **kwargs: Additional keyword arguments for BaseTool
        """
        super().__init__(**kwargs)
        self._group_id = group_id
        logger.info(f"PowerBIDAXTool initialized for group: {group_id or 'default'}")

    def _run(self, **kwargs: Any) -> str:
        """
        Execute a Power BI DAX action.

        Args:
            action (str): Action to perform (query, analyze)
            dax_query (Optional[str]): DAX query to execute
            semantic_model_id (Optional[str]): Semantic model ID
            workspace_id (Optional[str]): Workspace ID
            questions (Optional[list]): Business questions to analyze

        Returns:
            str: Formatted results of the action
        """
        # Create a new event loop for synchronous context
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(self._execute_action(**kwargs))
            return result
        finally:
            loop.close()

    async def _execute_action(self, **kwargs) -> str:
        """
        Async implementation of action execution.

        Args:
            **kwargs: Action parameters

        Returns:
            str: Formatted action results
        """
        action = kwargs.get('action', '').lower()

        try:
            if action == 'query':
                return await self._execute_query(**kwargs)
            elif action == 'analyze':
                return await self._analyze_questions(**kwargs)
            else:
                return f"❌ Invalid action: {action}"

        except Exception as e:
            logger.error(f"Error executing Power BI action '{action}': {e}", exc_info=True)
            return f"❌ Error executing {action}: {str(e)}"

    async def _execute_query(self, **kwargs) -> str:
        """
        Execute a DAX query.

        Args:
            **kwargs: Query parameters

        Returns:
            str: Formatted query results
        """
        from src.core.unit_of_work import UnitOfWork
        from src.services.powerbi_service import PowerBIService

        dax_query = kwargs.get('dax_query')
        semantic_model_id = kwargs.get('semantic_model_id')
        workspace_id = kwargs.get('workspace_id')

        async with UnitOfWork() as uow:
            service = PowerBIService(uow._session, group_id=self._group_id)

            query_request = DAXQueryRequest(
                dax_query=dax_query,
                semantic_model_id=semantic_model_id,
                workspace_id=workspace_id
            )

            response: DAXQueryResponse = await service.execute_dax_query(query_request)

            if response.status == "success":
                result_text = f"✅ DAX Query Executed Successfully\n\n"
                result_text += f"📊 Rows returned: {response.row_count}\n"
                result_text += f"⏱️ Execution time: {response.execution_time_ms}ms\n\n"

                if response.columns:
                    result_text += f"📋 Columns: {', '.join(response.columns)}\n\n"

                if response.data and len(response.data) > 0:
                    result_text += "🔍 Sample Results (first 5 rows):\n"
                    for i, row in enumerate(response.data[:5], 1):
                        result_text += f"\nRow {i}:\n"
                        for key, value in row.items():
                            result_text += f"  {key}: {value}\n"

                    if response.row_count > 5:
                        result_text += f"\n... and {response.row_count - 5} more rows"

                return result_text
            else:
                return f"❌ Query Failed: {response.error}"

    async def _analyze_questions(self, **kwargs) -> str:
        """
        Analyze business questions using Power BI data.

        Args:
            **kwargs: Analysis parameters

        Returns:
            str: Analysis results
        """
        questions = kwargs.get('questions', [])
        semantic_model_id = kwargs.get('semantic_model_id')

        # This is a placeholder for DAX generation logic
        # In a full implementation, you would:
        # 1. Use an LLM to generate DAX from questions
        # 2. Execute the generated DAX
        # 3. Analyze the results

        result_text = f"📊 Power BI Analysis Request\n\n"
        result_text += f"Semantic Model: {semantic_model_id or 'default'}\n\n"
        result_text += f"Questions ({len(questions)}):\n"
        for i, q in enumerate(questions, 1):
            result_text += f"{i}. {q}\n"

        result_text += "\n⚠️ Analysis feature requires DAX generation implementation.\n"
        result_text += "Use 'query' action with a DAX query for now."

        return result_text
