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
    PowerBITable,
    TableColumn,
    CalculatedColumnResult
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
7. **IMPORTANT**: Do NOT add calculated/computed columns to the SQL. Only include columns that are directly from the source query. Calculated columns will be handled separately.

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
        use_llm: bool = True,
        include_calculated_columns: bool = True
    ) -> List[ConversionResult]:
        """
        Convert all expressions in a table, including calculated columns.

        Args:
            table: PowerBITable with parsed expressions
            target_catalog: Target Unity Catalog catalog
            target_schema: Target schema
            use_llm: Whether to use LLM for complex conversions
            include_calculated_columns: Whether to convert calculated columns (default: True)

        Returns:
            List of ConversionResults (one per source expression)
        """
        results = []

        # Get calculated columns from the table
        calculated_cols = [col for col in table.columns if col.is_calculated]

        # Filter out calculated columns from the list sent to LLM
        # (we handle them separately to avoid duplicates)
        columns = [
            {"name": col.name, "data_type": col.data_type.value}
            for col in table.columns
            if not col.is_calculated
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

            # Convert calculated columns and add to result
            if include_calculated_columns and calculated_cols:
                calc_results = await self.convert_calculated_columns(
                    table_name=table.name,
                    calculated_columns=calculated_cols,
                    use_llm=use_llm
                )
                result.calculated_columns = calc_results

                # Enhance the SQL with calculated columns
                if result.success and result.databricks_sql and calc_results:
                    result.databricks_sql = self._enhance_sql_with_calculated_columns(
                        result.databricks_sql,
                        calc_results
                    )
                    result.create_view_sql = self._enhance_sql_with_calculated_columns(
                        result.create_view_sql,
                        calc_results
                    ) if result.create_view_sql else None

            results.append(result)

        return results

    def _get_calculated_column_system_prompt(self) -> str:
        """Get the system prompt for calculated column DAX to SQL conversion"""
        return """You are an expert in converting Power BI DAX calculated column expressions to Databricks SQL.

Your task is to convert DAX expressions used in calculated columns to equivalent SQL expressions.

Key DAX to SQL mappings for calculated columns:
- SWITCH(TRUE(), condition1, result1, condition2, result2, default) → CASE WHEN condition1 THEN result1 WHEN condition2 THEN result2 ELSE default END
- IF(condition, true_value, false_value) → CASE WHEN condition THEN true_value ELSE false_value END
- table[column] → column (just the column name without table reference)
- AND(cond1, cond2) → cond1 AND cond2
- OR(cond1, cond2) → cond1 OR cond2
- ISBLANK(column) → column IS NULL
- BLANK() → NULL
- Arithmetic: +, -, *, / work the same
- Comparison operators: >=, <=, >, <, =, <> work the same

Important notes:
1. Remove table name prefixes from column references (e.g., doc_exception_agg[column] → column)
2. The result should be a valid SQL expression that can be used in a SELECT clause
3. Preserve the original data type intent

Always respond with valid JSON in this exact format:
{
  "success": true,
  "sql_expression": "CASE WHEN column >= 1000 THEN 'High Value' ELSE 'Low Value' END",
  "explanation": "Brief explanation of the conversion",
  "notes": "Any caveats or edge cases"
}

Be accurate and concise."""

    def _create_calculated_column_prompt(
        self,
        table_name: str,
        column_name: str,
        dax_expression: str,
        data_type: str
    ) -> str:
        """Create the conversion prompt for a calculated column"""
        return f"""Convert this Power BI DAX calculated column expression to Databricks SQL.

## Column Information
- **Table Name**: {table_name}
- **Column Name**: {column_name}
- **Data Type**: {data_type}

## DAX Expression
```
{dax_expression}
```

## Instructions
1. Convert the DAX expression to an equivalent SQL expression
2. Remove table name prefixes from column references (e.g., {table_name}[column] → column)
3. The result should be usable directly in a SQL SELECT clause
4. Preserve the data type semantics

Respond with valid JSON only (no markdown code blocks around the JSON)."""

    async def convert_calculated_columns(
        self,
        table_name: str,
        calculated_columns: List[TableColumn],
        use_llm: bool = True
    ) -> List[CalculatedColumnResult]:
        """
        Convert calculated column DAX expressions to SQL.

        Args:
            table_name: Name of the table
            calculated_columns: List of calculated TableColumn objects
            use_llm: Whether to use LLM for conversions

        Returns:
            List of CalculatedColumnResult objects
        """
        results = []

        for col in calculated_columns:
            if not col.expression:
                continue

            logger.info(f"Converting calculated column '{col.name}' in table '{table_name}'")

            # Try LLM conversion first
            if use_llm and self.workspace_url and self.token:
                result = await self._convert_calculated_column_llm(
                    table_name=table_name,
                    column=col
                )
            else:
                # Fallback to rule-based conversion
                result = self._convert_calculated_column_rules(
                    table_name=table_name,
                    column=col
                )

            results.append(result)

        return results

    async def _convert_calculated_column_llm(
        self,
        table_name: str,
        column: TableColumn
    ) -> CalculatedColumnResult:
        """Convert a calculated column using LLM"""
        system_prompt = self._get_calculated_column_system_prompt()
        user_prompt = self._create_calculated_column_prompt(
            table_name=table_name,
            column_name=column.name,
            dax_expression=column.expression or "",
            data_type=column.data_type.value
        )

        llm_response = await self._call_llm(user_prompt, system_prompt)

        if llm_response.get("content"):
            parsed = self._parse_llm_response(llm_response["content"])
            tokens = llm_response.get("usage", {}).get("total_tokens", 0)
            self.total_tokens += tokens

            if parsed.get("success", False):
                logger.info(f"LLM conversion successful for calculated column '{column.name}'")
                return CalculatedColumnResult(
                    column_name=column.name,
                    original_dax=column.expression or "",
                    sql_expression=parsed.get("sql_expression"),
                    data_type=column.data_type.value,
                    success=True,
                    notes=parsed.get("notes")
                )
            else:
                logger.warning(f"LLM returned error for calculated column '{column.name}': {parsed.get('error')}")
                return CalculatedColumnResult(
                    column_name=column.name,
                    original_dax=column.expression or "",
                    data_type=column.data_type.value,
                    success=False,
                    error_message=parsed.get("error", "LLM conversion failed")
                )

        # LLM call failed, try rule-based
        logger.warning(f"LLM call failed for calculated column '{column.name}', falling back to rules")
        return self._convert_calculated_column_rules(table_name, column)

    def _convert_calculated_column_rules(
        self,
        table_name: str,
        column: TableColumn
    ) -> CalculatedColumnResult:
        """
        Rule-based conversion for simple calculated column expressions.

        Handles common patterns:
        - Simple arithmetic: column * 0.20
        - SWITCH statements
        - IF statements
        - Column references
        """
        dax = (column.expression or "").strip()

        if not dax:
            return CalculatedColumnResult(
                column_name=column.name,
                original_dax="",
                data_type=column.data_type.value,
                success=False,
                error_message="No DAX expression provided"
            )

        try:
            sql_expr = self._dax_to_sql_basic(dax, table_name)
            return CalculatedColumnResult(
                column_name=column.name,
                original_dax=dax,
                sql_expression=sql_expr,
                data_type=column.data_type.value,
                success=True,
                notes="Converted using rule-based conversion"
            )
        except Exception as e:
            logger.warning(f"Rule-based conversion failed for '{column.name}': {e}")
            return CalculatedColumnResult(
                column_name=column.name,
                original_dax=dax,
                data_type=column.data_type.value,
                success=False,
                error_message=f"Conversion failed: {str(e)}. Consider enabling LLM conversion."
            )

    def _dax_to_sql_basic(self, dax: str, table_name: str) -> str:
        """
        Basic DAX to SQL conversion for common calculated column patterns.

        Handles:
        - Column references: table[column] → column
        - Simple arithmetic
        - SWITCH(TRUE(), ...) → CASE WHEN ... END
        - IF(...) → CASE WHEN ... END
        """
        import re

        # Normalize whitespace
        sql = ' '.join(dax.split())

        # Remove table references: table_name[column] → column
        # Pattern: table_name[column_name] or 'table name'[column_name]
        sql = re.sub(
            rf"'{re.escape(table_name)}'\s*\[([^\]]+)\]",
            r'`\1`',
            sql
        )
        sql = re.sub(
            rf"{re.escape(table_name)}\s*\[([^\]]+)\]",
            r'`\1`',
            sql
        )
        # Generic pattern for any table reference
        sql = re.sub(r"[A-Za-z_][A-Za-z0-9_]*\s*\[([^\]]+)\]", r'`\1`', sql)

        # Handle SWITCH(TRUE(), ...)
        switch_match = re.match(
            r'SWITCH\s*\(\s*TRUE\s*\(\s*\)\s*,(.+)\)',
            sql,
            re.IGNORECASE | re.DOTALL
        )
        if switch_match:
            args = switch_match.group(1)
            sql = self._convert_switch_to_case(args)

        # Handle IF(condition, true_val, false_val)
        if_match = re.match(
            r'IF\s*\(\s*(.+?)\s*,\s*(.+?)\s*,\s*(.+?)\s*\)',
            sql,
            re.IGNORECASE | re.DOTALL
        )
        if if_match:
            condition = if_match.group(1)
            true_val = if_match.group(2)
            false_val = if_match.group(3)
            sql = f"CASE WHEN {condition} THEN {true_val} ELSE {false_val} END"

        return sql

    def _convert_switch_to_case(self, args_str: str) -> str:
        """
        Convert SWITCH arguments to SQL CASE WHEN.

        SWITCH(TRUE(), cond1, result1, cond2, result2, default)
        → CASE WHEN cond1 THEN result1 WHEN cond2 THEN result2 ELSE default END
        """
        # Parse the comma-separated arguments (handling nested parentheses)
        args = []
        depth = 0
        current = []
        for char in args_str:
            if char == '(':
                depth += 1
                current.append(char)
            elif char == ')':
                depth -= 1
                current.append(char)
            elif char == ',' and depth == 0:
                args.append(''.join(current).strip())
                current = []
            else:
                current.append(char)
        if current:
            args.append(''.join(current).strip())

        # Build CASE WHEN statement
        # Args are: cond1, result1, cond2, result2, ..., default
        case_parts = ["CASE"]

        i = 0
        while i < len(args) - 1:  # -1 because last is default
            if i + 1 < len(args):
                condition = args[i]
                result = args[i + 1]
                case_parts.append(f"WHEN {condition} THEN {result}")
                i += 2
            else:
                break

        # Last argument is the default
        if len(args) % 2 == 1:  # Odd number = has default
            case_parts.append(f"ELSE {args[-1]}")

        case_parts.append("END")

        return " ".join(case_parts)

    def _enhance_sql_with_calculated_columns(
        self,
        sql: str,
        calculated_columns: List[CalculatedColumnResult]
    ) -> str:
        """
        Enhance the generated SQL by adding calculated columns to the SELECT clause.

        Args:
            sql: Original SQL query
            calculated_columns: List of converted calculated columns

        Returns:
            Enhanced SQL with calculated columns added
        """
        if not sql or not calculated_columns:
            return sql

        # Find successful conversions
        successful_cols = [c for c in calculated_columns if c.success and c.sql_expression]
        if not successful_cols:
            return sql

        # Build calculated column expressions
        calc_expressions = []
        for col in successful_cols:
            calc_expressions.append(f"  {col.sql_expression} AS `{col.column_name}`")

        # Add as comment block showing calculated columns
        calc_sql_block = ",\n".join(calc_expressions)
        comment = f"\n-- Calculated Columns (converted from DAX)\n{calc_sql_block}"

        # Try to inject before FROM clause
        import re
        from_match = re.search(r'\bFROM\b', sql, re.IGNORECASE)
        if from_match:
            insert_pos = from_match.start()
            # Find the last column before FROM
            pre_from = sql[:insert_pos].rstrip()
            if pre_from.endswith(','):
                pre_from = pre_from[:-1]
            enhanced_sql = pre_from + "," + comment + "\n" + sql[insert_pos:]
            return enhanced_sql

        # If no FROM found (unusual), append at the end
        return sql + comment
