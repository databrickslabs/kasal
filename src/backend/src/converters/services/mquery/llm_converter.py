"""
LLM-Powered M-Query to SQL Converter

This module uses an LLM (via Databricks Foundation Model API) to convert
Power BI M-Query expressions to Databricks SQL.

The conversion is primarily LLM-based - the raw M-Query expression is sent
to the LLM which parses and converts it to SQL.

Author: Kasal Team
Date: 2025
"""

import json
import logging
from typing import Dict, List, Optional, Any

import httpx

from .models import (
    MQueryExpression,
    ConversionResult,
    PowerBITable
)
from .parser import MQueryParser

logger = logging.getLogger(__name__)


class MQueryLLMConverter:
    """
    LLM-powered converter for M-Query to Databricks SQL.

    Uses Databricks Foundation Model API (Claude, Llama, etc.) for
    intelligent conversion of complex M-Query expressions.
    """

    def __init__(
        self,
        workspace_url: Optional[str] = None,
        token: Optional[str] = None,
        model: str = "databricks-claude-sonnet-4"
    ):
        """
        Initialize the LLM converter.

        Args:
            workspace_url: Databricks workspace URL
            token: Databricks API token (PAT or OAuth)
            model: Model endpoint name
        """
        self.workspace_url = workspace_url
        self.token = token
        self.model = model
        self.parser = MQueryParser()
        self.total_tokens = 0

    def _get_system_prompt(self) -> str:
        """Get the system prompt for M-Query conversion"""
        return """You are an expert in converting Power BI M language (Power Query) expressions to Databricks SQL and PySpark.

Your task is to analyze M-Query source expressions and generate equivalent Databricks code.

Key requirements:
1. Extract complete SQL queries (don't truncate)
2. Identify all parameters (variables used with & concatenation in M-Query)
3. Convert Power Query transformations to SQL/Python equivalents
4. Generate production-ready, idiomatic Databricks code
5. Document all assumptions and manual steps needed

Power Query transformation mappings:
- Table.SelectRows(_, each [condition]) → WHERE condition
- Table.ReplaceValue(_, null, "value", _, {"column"}) → COALESCE(column, 'value')
- Table.FirstN(_, limit) → LIMIT clause
- Table.AddColumn(_, "name", each expr) → SELECT with computed column
- Table.Join → JOIN clause
- [column] >= RangeStart and [column] < RangeEnd → Incremental refresh filters

For parameters detected (like '" & CurrencyFilter & "'), convert to:
- Databricks SQL: Widget syntax :parameter_name or ${parameter_name}
- PySpark: dbutils.widgets.get("parameter_name")

Always respond with valid JSON in this exact format:
{
  "success": true,
  "databricks_sql": "complete SQL code with comments",
  "create_view_sql": "CREATE OR REPLACE VIEW statement",
  "databricks_python": "complete PySpark code with comments (optional)",
  "parameters": [
    {"name": "ParamName", "type": "STRING", "default": "default_value", "description": "what it does"}
  ],
  "transformations": [
    {"type": "filter|replace_null|limit|join|computed_column", "original": "M-Query", "converted": "SQL equivalent"}
  ],
  "explanation": "Brief explanation of the conversion approach",
  "source_connection": {"server": "...", "database": "...", "catalog": "..."},
  "notes": "Any important caveats or edge cases"
}

Be thorough and accurate. This code will be used in production."""

    def _create_conversion_prompt(
        self,
        table_name: str,
        expression: MQueryExpression,
        columns: List[Dict[str, str]],
        target_catalog: Optional[str] = None,
        target_schema: Optional[str] = None
    ) -> str:
        """Create the conversion prompt for a specific expression"""

        # Build column info
        column_info = "\n".join([
            f"  - {col['name']}: {col['data_type']}"
            for col in columns
        ]) if columns else "  (columns not available)"

        prompt = f"""Convert this Power BI M-Query expression to Databricks SQL.

## Table Information
- **Table Name**: {table_name}
- **Expression Type**: {expression.expression_type.value}
- **Target Location**: {target_catalog or 'default_catalog'}.{target_schema or 'default_schema'}.{table_name}

## Columns
{column_info}

## M-Query Expression
```
{expression.raw_expression}
```
"""

        # Add connection metadata if available (for context)
        if expression.server or expression.catalog:
            prompt += f"""
## Source Connection (detected)
- Server/Workspace: {expression.server or 'N/A'}
- Catalog: {expression.catalog or 'N/A'}
- Database/Schema: {expression.database or 'N/A'}
- Warehouse: {expression.warehouse_path or 'N/A'}
"""

        prompt += """
## Instructions
1. Parse the M-Query expression to extract the SQL query
2. Note: M-Query escape sequences like #(lf) = newline, #(cr) = carriage return, #(tab) = tab
3. Generate a CREATE OR REPLACE VIEW statement for Databricks Unity Catalog
4. Convert any M-Query transformations (Table.SelectRows, Table.ReplaceValue, etc.) to SQL equivalents
5. If parameters are concatenated (e.g., '\" & ParamName & \"'), replace with Databricks widget syntax
6. If the source is already Databricks, extract the SQL and create a simple view

Respond with valid JSON only (no markdown code blocks around the JSON)."""

        return prompt

    async def _call_llm(self, prompt: str, system_prompt: str) -> Dict[str, Any]:
        """
        Call Databricks Foundation Model API.

        Args:
            prompt: User prompt
            system_prompt: System prompt

        Returns:
            Dict with response content and usage
        """
        if not self.workspace_url or not self.token:
            logger.warning("LLM credentials not configured, using rule-based conversion")
            return {"content": None, "usage": {}, "error": "LLM not configured"}

        url = f"{self.workspace_url}/serving-endpoints/{self.model}/invocations"

        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }

        payload = {
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": prompt}
            ],
            "max_tokens": 4000,
            "temperature": 0.1
        }

        try:
            async with httpx.AsyncClient(timeout=120.0) as client:
                response = await client.post(url, headers=headers, json=payload)
                response.raise_for_status()

                result = response.json()
                return {
                    "content": result["choices"][0]["message"]["content"],
                    "usage": result.get("usage", {})
                }
        except Exception as e:
            logger.error(f"LLM API call failed: {e}")
            return {"content": None, "usage": {}, "error": str(e)}

    def _parse_llm_response(self, response_text: str) -> Dict[str, Any]:
        """Parse and validate LLM response"""
        try:
            # Remove markdown code blocks if present
            if response_text.startswith("```json"):
                response_text = response_text.split("```json")[1].split("```")[0].strip()
            elif response_text.startswith("```"):
                response_text = response_text.split("```")[1].split("```")[0].strip()

            return json.loads(response_text)
        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse LLM response as JSON: {e}")
            return {
                "success": False,
                "error": "Failed to parse LLM response",
                "raw_response": response_text[:500]
            }

    def _rule_based_conversion(
        self,
        table_name: str,
        expression: MQueryExpression,
        columns: List[Dict[str, str]],  # noqa: ARG002
        target_catalog: Optional[str] = None,  # noqa: ARG002
        target_schema: Optional[str] = None  # noqa: ARG002
    ) -> ConversionResult:
        """
        Fallback conversion when LLM is not available.

        This is a simple fallback that indicates LLM conversion is required.
        The actual M-Query to SQL conversion should be done by the LLM.

        Note: columns, target_catalog, target_schema are kept for API compatibility
        but not used since LLM is required for actual conversion.
        """
        logger.warning(f"[LLM_CONVERTER] LLM not available for '{table_name}', returning raw expression")

        return ConversionResult(
            table_name=table_name,
            expression_type=expression.expression_type,
            success=False,
            original_expression=expression.raw_expression,
            error_message=(
                "LLM conversion required but not available. "
                "Please configure Databricks LLM credentials (llm_workspace_url and llm_token) "
                "to convert M-Query expressions to SQL."
            ),
            source_connection={
                "server": expression.server,
                "database": expression.database,
                "catalog": expression.catalog,
                "warehouse_path": expression.warehouse_path
            },
            notes="LLM required for M-Query conversion"
        )

    async def convert_expression(
        self,
        table_name: str,
        expression: MQueryExpression,
        columns: List[Dict[str, str]],
        target_catalog: Optional[str] = None,
        target_schema: Optional[str] = None,
        use_llm: bool = True
    ) -> ConversionResult:
        """
        Convert a single M-Query expression to Databricks SQL.

        Uses LLM-first approach - sends the raw M-Query directly to the LLM for conversion.
        Falls back to rule-based only if LLM is not configured.

        Args:
            table_name: Name of the table
            expression: Parsed MQueryExpression
            columns: List of column definitions
            target_catalog: Target Unity Catalog catalog
            target_schema: Target schema
            use_llm: Whether to use LLM for conversions (default: True)

        Returns:
            ConversionResult with generated SQL
        """
        logger.info(f"Converting expression for table '{table_name}' ({expression.expression_type.value})")

        # LLM-first approach: Always use LLM if available
        if use_llm and self.workspace_url and self.token:
            logger.info(f"[LLM_CONVERTER] Using LLM conversion for '{table_name}'")

            # Use LLM for conversion
            system_prompt = self._get_system_prompt()
            user_prompt = self._create_conversion_prompt(
                table_name, expression, columns, target_catalog, target_schema
            )

            llm_response = await self._call_llm(user_prompt, system_prompt)

            if llm_response.get("content"):
                # LLM succeeded - parse and return result
                parsed = self._parse_llm_response(llm_response["content"])
                tokens = llm_response.get("usage", {}).get("total_tokens", 0)
                self.total_tokens += tokens

                if parsed.get("success", False):
                    logger.info(f"[LLM_CONVERTER] LLM conversion successful for '{table_name}'")
                    return ConversionResult(
                        table_name=table_name,
                        expression_type=expression.expression_type,
                        success=True,
                        original_expression=expression.raw_expression,
                        databricks_sql=parsed.get("databricks_sql"),
                        create_view_sql=parsed.get("create_view_sql"),
                        databricks_python=parsed.get("databricks_python"),
                        parameters=parsed.get("parameters", []),
                        transformations=parsed.get("transformations", []),
                        llm_explanation=parsed.get("explanation"),
                        llm_model=self.model,
                        tokens_used=tokens,
                        source_connection=parsed.get("source_connection"),
                        notes=parsed.get("notes")
                    )
                else:
                    # LLM returned an error
                    logger.warning(f"[LLM_CONVERTER] LLM returned error for '{table_name}': {parsed.get('error')}")
                    return ConversionResult(
                        table_name=table_name,
                        expression_type=expression.expression_type,
                        success=False,
                        original_expression=expression.raw_expression,
                        error_message=parsed.get("error", "LLM conversion failed"),
                        llm_model=self.model,
                        tokens_used=tokens
                    )
            else:
                # LLM call failed - fall back to rule-based
                logger.warning(f"[LLM_CONVERTER] LLM call failed for '{table_name}': {llm_response.get('error')}")
                # Fall through to rule-based conversion
        else:
            logger.info(f"[LLM_CONVERTER] LLM not available, using rule-based conversion for '{table_name}'")

        # Fallback: Rule-based conversion (when LLM is not configured or failed)
        return self._rule_based_conversion(
            table_name, expression, columns, target_catalog, target_schema
        )

    async def convert_table(
        self,
        table: PowerBITable,
        target_catalog: Optional[str] = None,
        target_schema: Optional[str] = None,
        use_llm: bool = True
    ) -> List[ConversionResult]:
        """
        Convert all expressions in a table.

        Args:
            table: PowerBITable with parsed expressions
            target_catalog: Target Unity Catalog catalog
            target_schema: Target schema
            use_llm: Whether to use LLM for complex conversions

        Returns:
            List of ConversionResults (one per source expression)
        """
        results = []
        columns = [
            {"name": col.name, "data_type": col.data_type.value}
            for col in table.columns
        ]

        for expr in table.source_expressions:
            result = await self.convert_expression(
                table_name=table.name,
                expression=expr,
                columns=columns,
                target_catalog=target_catalog,
                target_schema=target_schema,
                use_llm=use_llm
            )
            results.append(result)

        return results
