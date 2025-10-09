"""
Power BI DAX Generator Tool

CrewAI custom tool for generating DAX queries from natural language questions.
This tool allows agents to work with Power BI semantic models by generating
DAX queries that can be executed in Databricks notebooks.
"""

import asyncio
import logging
from typing import Any, Dict, Optional, Type

from crewai.tools import BaseTool
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class PowerBIToolSchema(BaseModel):
    """Input schema for Power BI DAX Generator Tool."""

    question: str = Field(
        ...,
        description="Natural language question to convert into a DAX query"
    )


class PowerBITool(BaseTool):
    """
    Power BI DAX Generator Tool for CrewAI agents.

    This tool generates DAX queries from natural language questions based on
    Power BI dataset metadata. The generated queries can then be executed
    in Databricks notebooks against the Power BI XMLA endpoint.

    Example usage in agent configuration:
        ```python
        tool_config = {
            "xmla_endpoint": "powerbi://api.powerbi.com/v1.0/myorg/workspace",
            "dataset_name": "SalesDataset",
            "metadata": {...}  # Dataset metadata
        }
        ```
    """

    name: str = "Power BI DAX Generator"
    description: str = (
        "Generate DAX queries from natural language questions for Power BI datasets. "
        "Provide a 'question' parameter with your question about the data. "
        "The tool will return a DAX query that can be executed in Databricks."
    )
    args_schema: Type[BaseModel] = PowerBIToolSchema

    # Tool configuration
    xmla_endpoint: Optional[str] = None
    dataset_name: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    model_name: str = "databricks-meta-llama-3-1-405b-instruct"
    temperature: float = 0.1

    def __init__(
        self,
        xmla_endpoint: Optional[str] = None,
        dataset_name: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        model_name: str = "databricks-meta-llama-3-1-405b-instruct",
        temperature: float = 0.1,
        tool_config: Optional[Dict[str, Any]] = None,
        **kwargs: Any
    ) -> None:
        """
        Initialize the Power BI Tool.

        Args:
            xmla_endpoint: Power BI XMLA endpoint URL
            dataset_name: Name of the Power BI dataset
            metadata: Dataset metadata (tables, columns, relationships)
            model_name: LLM model to use for DAX generation
            temperature: Temperature for LLM generation
            tool_config: Additional tool configuration
            **kwargs: Additional keyword arguments for BaseTool
        """
        super().__init__(**kwargs)

        # Get configuration from tool_config if provided
        if tool_config:
            self.xmla_endpoint = tool_config.get('xmla_endpoint', xmla_endpoint)
            self.dataset_name = tool_config.get('dataset_name', dataset_name)
            self.metadata = tool_config.get('metadata', metadata)
            self.model_name = tool_config.get('model_name', model_name)
            self.temperature = tool_config.get('temperature', temperature)
        else:
            self.xmla_endpoint = xmla_endpoint
            self.dataset_name = dataset_name
            self.metadata = metadata
            self.model_name = model_name
            self.temperature = temperature

        # Validate configuration
        if not self.metadata:
            logger.warning(
                "Power BI tool initialized without metadata. "
                "DAX generation will not work until metadata is provided."
            )

        logger.info("Power BI DAX Generator Tool Configuration:")
        logger.info(f"XMLA Endpoint: {self.xmla_endpoint}")
        logger.info(f"Dataset: {self.dataset_name}")
        logger.info(f"Model: {self.model_name}")
        logger.info(f"Metadata tables: {len(self.metadata.get('tables', [])) if self.metadata else 0}")

    def _run(self, **kwargs: Any) -> str:
        """
        Execute the tool synchronously.

        Args:
            question: Natural language question

        Returns:
            Generated DAX query with execution instructions
        """
        question = kwargs.get("question")

        if not question:
            return "Error: No question provided"

        try:
            # Run async method in event loop
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                result = loop.run_until_complete(self._generate_dax(question))
                return result
            finally:
                loop.close()

        except Exception as e:
            logger.error(f"Error in Power BI tool: {str(e)}", exc_info=True)
            return f"Error generating DAX query: {str(e)}"

    async def _generate_dax(self, question: str) -> str:
        """
        Generate DAX query asynchronously.

        Args:
            question: Natural language question

        Returns:
            Formatted response with DAX query and instructions
        """
        try:
            # Validate metadata is available
            if not self.metadata:
                return (
                    "Error: Dataset metadata not configured. "
                    "Please provide metadata in tool configuration."
                )

            # Import DAX generator service
            from src.core.unit_of_work import UnitOfWork
            from src.services.dax_generator_service import DAXGeneratorService

            # Generate DAX query
            async with UnitOfWork() as uow:
                service = await DAXGeneratorService.from_unit_of_work(uow)

                result = await service.generate_dax_from_question(
                    question=question,
                    metadata=self.metadata,
                    model_name=self.model_name,
                    temperature=self.temperature
                )

            # Format response
            dax_query = result["dax_query"]
            explanation = result["explanation"]
            confidence = result["confidence"]

            response = f"""DAX Query Generated (Confidence: {confidence:.0%})

Question: {question}

DAX Query:
```dax
{dax_query}
```

Explanation: {explanation}

---
Execution Instructions:

To execute this DAX query in Databricks:

1. Use the Databricks Jobs Tool to create a notebook job
2. In the notebook, use the following Python code:

```python
import pyadomd

# Connection string
connection_string = (
    "Provider=MSOLAP;"
    "Data Source={self.xmla_endpoint};"
    "Initial Catalog={self.dataset_name};"
    "User ID=app:{{client_id}}@{{tenant_id}};"
    "Password={{client_secret}};"
)

# Execute DAX query
with Pyadomd(connection_string) as conn:
    cursor = conn.cursor()
    cursor.execute(\"\"\"
{dax_query}
    \"\"\")

    # Get results
    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()

    # Convert to list of dictionaries
    results = []
    for row in rows:
        results.append(dict(zip(columns, row)))

    # Display results
    display(results)
```

3. Provide the service principal credentials as job parameters:
   - client_id: Azure AD application/client ID
   - tenant_id: Azure AD tenant ID
   - client_secret: Service principal secret

---
"""
            return response

        except Exception as e:
            logger.error(f"Error generating DAX: {str(e)}", exc_info=True)
            return f"Error generating DAX query: {str(e)}"

    def update_metadata(self, metadata: Dict[str, Any]) -> None:
        """
        Update the dataset metadata.

        Args:
            metadata: New metadata dictionary
        """
        self.metadata = metadata
        logger.info(f"Updated Power BI metadata: {len(metadata.get('tables', []))} tables")

    def update_config(
        self,
        xmla_endpoint: Optional[str] = None,
        dataset_name: Optional[str] = None,
        model_name: Optional[str] = None,
        temperature: Optional[float] = None
    ) -> None:
        """
        Update tool configuration.

        Args:
            xmla_endpoint: New XMLA endpoint
            dataset_name: New dataset name
            model_name: New model name
            temperature: New temperature
        """
        if xmla_endpoint:
            self.xmla_endpoint = xmla_endpoint
        if dataset_name:
            self.dataset_name = dataset_name
        if model_name:
            self.model_name = model_name
        if temperature is not None:
            self.temperature = temperature

        logger.info("Updated Power BI tool configuration")
