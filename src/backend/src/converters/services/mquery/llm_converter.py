"""
LLM-Powered M-Query to SQL Converter

This module uses an LLM (via Databricks Foundation Model API) to convert
complex Power BI M-Query expressions to Databricks SQL.

For simple cases (embedded SQL), it extracts directly.
For complex cases (transformations, parameters), it uses LLM intelligence.

Author: Kasal Team
Date: 2025
"""

import json
import logging
import re
from typing import Dict, List, Optional, Any

import httpx

from .models import (
    MQueryExpression,
    ExpressionType,
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

        # Add pre-extracted info if available
        if expression.embedded_sql:
            prompt += f"""
## Pre-extracted SQL (from Value.NativeQuery)
```sql
{expression.embedded_sql}
```

"""

        if expression.parameters:
            params_str = "\n".join([
                f"  - {p['name']} ({p.get('type', 'STRING')}): {p.get('context', '')}"
                for p in expression.parameters
            ])
            prompt += f"""
## Detected Parameters
{params_str}

"""

        if expression.transformations:
            transforms_str = "\n".join([
                f"  - {t['type']}: {t.get('sql_hint', t.get('mquery', ''))}"
                for t in expression.transformations
            ])
            prompt += f"""
## Detected Transformations
{transforms_str}

"""

        if expression.server or expression.catalog:
            prompt += f"""
## Source Connection
- Server/Workspace: {expression.server or 'N/A'}
- Catalog: {expression.catalog or 'N/A'}
- Database/Schema: {expression.database or 'N/A'}
- Warehouse: {expression.warehouse_path or 'N/A'}

"""

        prompt += """
## Instructions
1. Generate a CREATE OR REPLACE VIEW statement for Databricks
2. Convert all M-Query transformations to SQL (WHERE, COALESCE, LIMIT, etc.)
3. Replace parameter concatenations with Databricks widget syntax
4. If the source is already Databricks, simplify the view to reference the source directly
5. Include comments explaining any complex conversions

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
        columns: List[Dict[str, str]],
        target_catalog: Optional[str] = None,
        target_schema: Optional[str] = None
    ) -> ConversionResult:
        """
        Rule-based conversion for simple cases (no LLM needed).

        Used when:
        - LLM is not configured
        - Expression has embedded SQL and no complex transformations
        """
        target_loc = f"{target_catalog or 'catalog'}.{target_schema or 'schema'}.{table_name}"

        # For native queries with embedded SQL
        if expression.expression_type == ExpressionType.NATIVE_QUERY and expression.embedded_sql:
            sql = expression.embedded_sql

            # Apply transformations
            sql_parts = [sql]
            where_clauses = []
            coalesce_columns = []
            limit_clause = None

            for transform in expression.transformations:
                if transform["type"] == "filter":
                    # Convert M-Query filter to SQL WHERE
                    condition = transform.get("condition", "")
                    # Basic conversion: [column] → column
                    condition = re.sub(r'\[(\w+)\]', r'\1', condition)
                    where_clauses.append(condition)
                elif transform["type"] == "replace_null":
                    coalesce_columns.append({
                        "column": transform["column"],
                        "replacement": transform["replacement"]
                    })
                elif transform["type"] == "limit":
                    limit_clause = transform.get("limit_variable", "1000")

            # Build final SQL
            if where_clauses or coalesce_columns or limit_clause:
                # Wrap in CTE
                final_sql = f"WITH base_query AS (\n{sql}\n)\nSELECT * FROM base_query"
                if where_clauses:
                    final_sql += f"\nWHERE {' AND '.join(where_clauses)}"
                if limit_clause:
                    final_sql += f"\nLIMIT {limit_clause}"
            else:
                final_sql = sql

            # Create view SQL
            create_view_sql = f"CREATE OR REPLACE VIEW {target_loc} AS\n{final_sql}"

            return ConversionResult(
                table_name=table_name,
                expression_type=expression.expression_type,
                success=True,
                databricks_sql=final_sql,
                create_view_sql=create_view_sql,
                parameters=expression.parameters,
                transformations=expression.transformations,
                source_connection={
                    "server": expression.server,
                    "database": expression.database,
                    "catalog": expression.catalog
                },
                notes="Rule-based conversion (no LLM)"
            )

        # For Databricks catalog sources
        elif expression.expression_type == ExpressionType.DATABRICKS_CATALOG:
            source_loc = f"{expression.catalog or 'catalog'}.{expression.database or 'schema'}"
            final_sql = f"SELECT * FROM {source_loc}.source_table"
            create_view_sql = f"CREATE OR REPLACE VIEW {target_loc} AS\n{final_sql}"

            return ConversionResult(
                table_name=table_name,
                expression_type=expression.expression_type,
                success=True,
                databricks_sql=final_sql,
                create_view_sql=create_view_sql,
                parameters=expression.parameters,
                transformations=expression.transformations,
                source_connection={
                    "workspace": expression.server,
                    "catalog": expression.catalog,
                    "database": expression.database
                },
                notes="Source is already Databricks - verify source table name"
            )

        # For other types, return partial result
        return ConversionResult(
            table_name=table_name,
            expression_type=expression.expression_type,
            success=False,
            error_message="Complex expression requires LLM conversion",
            parameters=expression.parameters,
            transformations=expression.transformations
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

        Args:
            table_name: Name of the table
            expression: Parsed MQueryExpression
            columns: List of column definitions
            target_catalog: Target Unity Catalog catalog
            target_schema: Target schema
            use_llm: Whether to use LLM for complex conversions

        Returns:
            ConversionResult with generated SQL
        """
        logger.info(f"Converting expression for table '{table_name}' ({expression.expression_type.value})")

        # For simple cases or when LLM is disabled, use rule-based conversion
        if not use_llm or (
            expression.expression_type == ExpressionType.NATIVE_QUERY
            and expression.embedded_sql
            and len(expression.transformations) <= 2
        ):
            return self._rule_based_conversion(
                table_name, expression, columns, target_catalog, target_schema
            )

        # Use LLM for complex conversions
        system_prompt = self._get_system_prompt()
        user_prompt = self._create_conversion_prompt(
            table_name, expression, columns, target_catalog, target_schema
        )

        llm_response = await self._call_llm(user_prompt, system_prompt)

        if not llm_response.get("content"):
            # Fall back to rule-based if LLM fails
            logger.warning(f"LLM conversion failed, falling back to rule-based: {llm_response.get('error')}")
            result = self._rule_based_conversion(
                table_name, expression, columns, target_catalog, target_schema
            )
            result.notes = f"LLM failed ({llm_response.get('error')}), used rule-based conversion"
            return result

        # Parse LLM response
        parsed = self._parse_llm_response(llm_response["content"])
        tokens = llm_response.get("usage", {}).get("total_tokens", 0)
        self.total_tokens += tokens

        if not parsed.get("success", False):
            return ConversionResult(
                table_name=table_name,
                expression_type=expression.expression_type,
                success=False,
                error_message=parsed.get("error", "LLM conversion failed"),
                llm_model=self.model,
                tokens_used=tokens
            )

        return ConversionResult(
            table_name=table_name,
            expression_type=expression.expression_type,
            success=True,
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
