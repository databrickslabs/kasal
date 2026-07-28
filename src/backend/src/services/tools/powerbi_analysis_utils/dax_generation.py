"""Turning a question into DAX: the LLM call, its trace, response extraction,
the self-correction loop, and executing the result.

Mixed into ``PowerBIAnalysisTool`` rather than composed, so this is pure
movement: every method still reads ``self`` exactly as it did in the single
3,506-line file, and every ``tool._method(...)`` call site is unchanged.
"""

import asyncio
import base64
import contextvars
import logging
import json
import re
from typing import Any, Optional, Type, Dict, List
from concurrent.futures import ThreadPoolExecutor
from datetime import date

from src.services.tools.base import BaseTool
from pydantic import BaseModel, Field, PrivateAttr
import httpx

from src.services.tools.tool_session_provider import ToolSessionProvider


logger = logging.getLogger(__name__)


class PowerBIDaxGenerationMixin:
    async def _generate_dax_with_llm(
        self,
        user_question: str,
        model_context: Dict[str, Any],
        config: Dict[str, Any]
    ) -> Optional[str]:
        """
        Generate DAX query using LLM with enriched context (Microsoft Copilot-style).

        This method now includes:
        - Full semantic model schema with descriptions
        - Business terminology mappings
        - Field synonyms
        - Active filters (auto-applied)
        - Conversation history
        - Sample data values
        """
        # DEBUG: Log config values at the start of DAX generation
        logger.info("=" * 80)
        logger.info("[DAX GENERATION CONFIG DEBUG] Config values received:")
        logger.info(f"[DAX GENERATION CONFIG DEBUG]   business_mappings: {type(config.get('business_mappings')).__name__} = {str(config.get('business_mappings', {}))[:200]}")
        logger.info(f"[DAX GENERATION CONFIG DEBUG]   field_synonyms: {type(config.get('field_synonyms')).__name__} = {str(config.get('field_synonyms', {}))[:200]}")
        logger.info(f"[DAX GENERATION CONFIG DEBUG]   active_filters: {type(config.get('active_filters')).__name__} = {str(config.get('active_filters', {}))[:200]}")
        logger.info(f"[DAX GENERATION CONFIG DEBUG]   conversation_history: {type(config.get('conversation_history')).__name__} = {str(config.get('conversation_history', []))[:200]}")
        logger.info("=" * 80)

        llm_workspace_url = config.get("llm_workspace_url")
        llm_token = config.get("llm_token")
        llm_model = config.get("llm_model", "databricks-claude-sonnet-4-5")

        if not llm_workspace_url or not llm_token:
            # Fallback: Generate simple DAX without LLM
            return self._generate_simple_dax(user_question, model_context)

        # Check if we have measures to work with
        measures = model_context.get("measures", [])
        if not measures:
            logger.warning("[DAX Generation] No measures available - cannot generate meaningful query")
            return None

        # Build ENRICHED context (Microsoft Copilot-style)
        enriched_context = self._build_enriched_semantic_context(model_context, config)

        # Log context size for debugging
        logger.info(f"[DAX Generation] Enriched context size: {len(enriched_context)} characters")
        logger.info(f"[DAX Generation] Active filters: {config.get('active_filters', {})}")
        logger.info(f"[DAX Generation] Business mappings: {len(config.get('business_mappings', {}))} terms")
        logger.info(f"[DAX Generation] Field synonyms: {len(config.get('field_synonyms', {}))} fields")

        # Build the enhanced prompt
        prompt = f"""{enriched_context}

## 🎯 USER QUESTION
{user_question}

## 📋 DAX GENERATION INSTRUCTIONS

### Context Understanding
1. **Interpret natural language**: Use business term mappings and synonyms to understand the question
2. **Apply implicit filters**: Automatically apply filters from "CURRENT VIEW STATE" even if not mentioned in question
3. **Use conversation history**: Consider previous questions for context
4. **Leverage sample values**: Use sample data to understand value formats and patterns

### Query Generation Rules
1. **ONLY use measure names from "Available Measures"** - DO NOT invent or modify names
2. **ONLY use table/column names from "SEMANTIC MODEL SCHEMA"** - DO NOT guess names
3. **Use exact syntax**: Measure references must match exactly (e.g., [Total Sales] not [Total_Sales])
4. **Apply active filters**: Include filters from "CURRENT VIEW STATE" automatically
5. **Natural language translation**: Use "Business Term Mappings" to convert phrases to DAX expressions
   Example: If user says "Complete CGR", use the mapped expression from business mappings

### DAX Syntax Requirements

#### Basic Structure
- Use `EVALUATE` with `SUMMARIZECOLUMNS`, `ADDCOLUMNS`, or other table functions
- **Filter order**: In SUMMARIZECOLUMNS, filters MUST come BEFORE measure name/expression pairs
- **Correct**: `SUMMARIZECOLUMNS(Column, FILTER(...), "Name", [Measure])`
- **Wrong**: `SUMMARIZECOLUMNS(Column, "Name", [Measure], FILTER(...))`
- Return ONLY the DAX query - no explanations, no markdown code blocks

#### String Prefix Filtering (CRITICAL - STARTSWITH NOT SUPPORTED)
**NEVER use STARTSWITH() function** - Power BI API does not recognize this function and will return "Failed to resolve name 'STARTSWITH'" error.

❌ **WRONG - Using STARTSWITH (not supported):**
```dax
FILTER(VALUES(Table[Column]), STARTSWITH(Table[Column], "7"))
FILTER(VALUES(Table[Column]), NOT(STARTSWITH(Table[Column], "7")))
```

✅ **CORRECT - Use LEFT() function instead:**
```dax
FILTER(VALUES(Table[Column]), LEFT(Table[Column], 1) = "7")
FILTER(VALUES(Table[Column]), LEFT(Table[Column], LEN("ABC")) = "ABC")
FILTER(VALUES(Table[Column]), NOT(LEFT(Table[Column], 1) = "7"))
```

✅ **ALTERNATIVE - Use comparison operators for single character:**
```dax
FILTER(VALUES(Table[Column]), Table[Column] >= "7" && Table[Column] < "8")
FILTER(VALUES(Table[Column]), NOT(Table[Column] >= "7" && Table[Column] < "8"))
```

✅ **ALTERNATIVE - Use SEARCH() function:**
```dax
FILTER(VALUES(Table[Column]), SEARCH("ABC", Table[Column], 1, 0) = 1)
```

**Examples:**
- ✅ LEFT(account_code, 1) = "7" (check if starts with "7")
- ✅ LEFT(account_code, 3) = "ABC" (check if starts with "ABC")
- ✅ NOT(LEFT(account_code, 1) = "7") (exclude codes starting with "7")
- ❌ STARTSWITH(account_code, "7") ← Will cause "Failed to resolve name 'STARTSWITH'" error

#### Multi-Table Filtering (CRITICAL)
**NEVER use ALL() with columns from different tables** - this causes "must be from the same table" error

❌ **WRONG - Columns from multiple tables in ALL():**
```dax
FILTER(
    ALL(Table1[Col1], Table2[Col2], Table3[Col3]),  ← ERROR!
    Table1[Col1] = "X" && Table2[Col2] = "Y"
)
```

✅ **CORRECT - Use TREATAS or separate filters:**
```dax
SUMMARIZECOLUMNS(
    TREATAS({"FilterValue1"}, DimTable1[Column1]),
    TREATAS({"FilterValue2"}, DimTable2[Column2]),
    FILTER(VALUES(FactTable[Column3]), FactTable[Column3] = "FilterValue3"),
    "Result", SUM(FactTable[Measure])
)
```

✅ **CORRECT - Use CALCULATETABLE:**
```dax
EVALUATE
CALCULATETABLE(
    SUMMARIZECOLUMNS("Result", SUM(FactTable[Measure])),
    FactTable[Col1] = "Value1",
    DimTable1[Col2] = "Value2",
    DimTable2[Col3] = "Value3"
)
```

#### IN Operator for Multiple Values (CRITICAL)
❌ **WRONG**: `Column = "Value1 OR Value2"` (treats as single literal string)
❌ **WRONG**: `Column IN ("Value1", "Value2")` (parentheses not supported in this context)
✅ **CORRECT**: `Column IN {{"Value1", "Value2"}}` (MUST use curly braces)

**Examples:**
- ✅ mandatory_version IN {{"Landline OR Mobile"}}
- ✅ BU IN {{"Italy", "Germany", "France"}}
- ❌ mandatory_version IN ("Landline", "Mobile") ← Will cause "Operator not supported" error

### Example: Natural Language to DAX (Multi-Table Filtering)
Question: "What is the number of customers with Complete CGR in the Italian BU in week 1 where Mandatory Version is Landline or Mobile?"

Step 1 - Identify business terms and tables:
- "number of customers" → SUM(tbl_initial_sizing_tracking[num_customers])
- "Complete CGR" → tbl_initial_sizing_tracking[description] = 'Complete CGR'
- "Italian BU" → dim_country[BU] = 'Italy' (related table via country_code)
- "week 1" → dim_weeks[Week] = 1 (related table via loading_date)
- "Landline or Mobile" → tbl_initial_sizing_tracking[mandatory_version] IN ('Landline OR Mobile')

Step 2 - Check relationships:
- tbl_initial_sizing_tracking → dim_country (via country_code)
- tbl_initial_sizing_tracking → dim_weeks (via loading_date)

Step 3 - Generate DAX with multi-table filtering:

✅ **CORRECT Approach - Using TREATAS for related tables:**
```dax
EVALUATE
SUMMARIZECOLUMNS(
    TREATAS({{"Italy"}}, dim_country[BU]),
    TREATAS({{1}}, dim_weeks[Week]),
    FILTER(
        VALUES(tbl_initial_sizing_tracking[description]),
        tbl_initial_sizing_tracking[description] = "Complete CGR"
    ),
    FILTER(
        VALUES(tbl_initial_sizing_tracking[mandatory_version]),
        tbl_initial_sizing_tracking[mandatory_version] IN {{"Landline OR Mobile"}}
    ),
    "num_customers", SUM(tbl_initial_sizing_tracking[num_customers])
)
```

✅ **ALTERNATIVE - Using CALCULATETABLE:**
```dax
EVALUATE
CALCULATETABLE(
    SUMMARIZECOLUMNS("num_customers", SUM(tbl_initial_sizing_tracking[num_customers])),
    tbl_initial_sizing_tracking[description] = "Complete CGR",
    dim_country[BU] = "Italy",
    dim_weeks[Week] = 1,
    tbl_initial_sizing_tracking[mandatory_version] IN {{"Landline OR Mobile"}}
)
```

## ✅ YOUR GENERATED DAX QUERY

**CRITICAL OUTPUT REQUIREMENTS:**
1. Return ONLY the DAX query - NO explanations, NO markdown, NO commentary
2. Start with EVALUATE and end with the closing parenthesis )
3. Do NOT add any text like "Key Changes:", "Note:", "**", or explanations after the query
4. The query must be executable as-is without any modifications

**Example of CORRECT output format:**
```
EVALUATE
CALCULATETABLE(
    SUMMARIZECOLUMNS("result", SUM(Table[Column])),
    Table[Filter] = "Value"
)
```

**Example of WRONG output format (DO NOT DO THIS):**
```
EVALUATE
CALCULATETABLE(...)
)
**Key Changes Made:**
1. Fixed the syntax...
```

**YOUR DAX QUERY (no explanations):**
"""

        # Log the full prompt for debugging
        logger.info("=" * 80)
        logger.info("[DAX Generation - Enhanced] LLM Prompt:")
        logger.info(prompt)
        logger.info("=" * 80)

        # Log the full prompt for debugging
        logger.info("=" * 80)
        logger.info("[DAX Generation] LLM Prompt:")
        logger.info(prompt)
        logger.info("=" * 80)

        # Emit trace event for LLM prompt (appears in UI technical trace)
        self._emit_llm_trace(
            event_context="DAX Generation - Prompt",
            prompt=prompt,
            model=llm_model,
            operation="generate_dax"
        )

        # Call LLM via LLMManager
        from src.core.llm_manager import LLMManager
        from src.utils.telemetry import get_user_agent_header, KasalProduct

        try:
            content = await LLMManager.completion(
                messages=[{"role": "user", "content": prompt}],
                model=llm_model,
                temperature=0.1,
                max_tokens=1000,
                extra_headers=get_user_agent_header(KasalProduct.POWERBI),
            )

            # Log the raw LLM response
            logger.info("=" * 80)
            logger.info("[DAX Generation] LLM Raw Response:")
            logger.info(content)
            logger.info("=" * 80)

            # Emit trace event for LLM response (appears in UI technical trace)
            self._emit_llm_trace(
                event_context="DAX Generation - Response",
                prompt=prompt,
                response=content,
                model=llm_model,
                operation="generate_dax"
            )

            # Clean up: extract just the DAX query
            dax = self._extract_dax_from_llm_response(content)

            # Log the extracted DAX
            logger.info(f"[DAX Generation] Extracted DAX Query:")
            logger.info(dax)
            logger.info("=" * 80)

            # Validate that generated DAX uses only available measures
            available_measure_names = [m["name"] for m in model_context.get("measures", [])]

            # Check for hallucinated measures - extract all [measure] references
            dax_pattern = r'\[([^\]]+)\]'
            all_references = re.findall(dax_pattern, dax)

            # Filter to get only measure references (not table[column] references)
            table_columns = set()
            for table in model_context.get("tables", []):
                table_name = table["name"]
                for col in table.get("columns", []):
                    table_columns.add(f"{table_name}[{col}]")

            # Find references that look like measures (not part of table[column])
            potential_measures = []
            for ref in all_references:
                # Check if this is part of a table[column] pattern
                is_table_column = any(f"{table['name']}[{ref}]" in dax for table in model_context.get("tables", []))
                if not is_table_column:
                    potential_measures.append(ref)

            # Check for measures not in available list
            hallucinated = [m for m in potential_measures if m not in available_measure_names]
            if hallucinated:
                logger.warning(f"[DAX Generation] LLM may have used non-existent measures: {hallucinated}")
                logger.warning(f"[DAX Generation] Available measures are: {available_measure_names[:10]}")

            # Auto-wrap with report-level filters if present
            dax = self._auto_wrap_with_report_filters(dax, config)

            return dax

        except Exception as e:
            logger.error(f"LLM DAX generation error: {e}")
            # Fallback to simple generation
            return self._generate_simple_dax(user_question, model_context)

    def _emit_llm_trace(
        self,
        event_context: str,
        prompt: str,
        model: str,
        operation: str,
        response: Optional[str] = None
    ) -> None:
        """
        Emit a trace event for LLM operations that appears in the UI technical trace.

        This method uses the trace_context that was injected into the tool during
        crew preparation to emit custom llm_call trace events.

        Args:
            event_context: Context description (e.g., "DAX Generation - Prompt")
            prompt: The LLM prompt text
            model: The model name used
            operation: The operation type (e.g., "generate_dax")
            response: Optional LLM response text
        """
        try:
            # Check if trace_context was injected (happens during crew preparation)
            trace_ctx = getattr(self, 'trace_context', None)

            # Use logger.info instead of debug to ensure visibility
            logger.info(f"[PowerBIAnalysisTool] TRACE EMISSION DEBUG - trace_context present: {trace_ctx is not None}")

            if not trace_ctx:
                logger.info("[PowerBIAnalysisTool] No trace_context available, skipping llm_call trace emission")
                return

            logger.info(f"[PowerBIAnalysisTool] TRACE EMISSION DEBUG - job_id: {trace_ctx.get('job_id')}")

            if not trace_ctx.get('job_id'):
                logger.info("[PowerBIAnalysisTool] trace_context missing job_id, skipping llm_call trace emission")
                return

            # Get the trace queue
            from src.services.trace.queue import get_trace_queue
            queue = get_trace_queue()

            # Build trace data with prompt and response (limit size to avoid issues)
            # Use up to 3000 chars which is enough to see the content but not too large for storage
            max_display_length = 3000

            trace_output = {
                'operation': operation,
                'model': model,
                'prompt_length': len(prompt),
                'prompt': prompt[:max_display_length] + ('...[truncated]' if len(prompt) > max_display_length else ''),
                'prompt_preview': prompt[:200] if len(prompt) > 200 else prompt
            }

            if response:
                trace_output['response_length'] = len(response)
                trace_output['response'] = response[:max_display_length] + ('...[truncated]' if len(response) > max_display_length else '')
                trace_output['response_preview'] = response[:200] if len(response) > 200 else response

            # Emit the trace event
            # Note: Frontend expects agent_role and model in extra_data for llm_call events
            trace_event = {
                'job_id': trace_ctx.get('job_id'),
                'event_type': 'llm_call',
                'event_source': 'PowerBI Analysis Tool',
                'event_context': event_context,
                'output': trace_output,
                'extra_data': {
                    'agent_role': 'PowerBI Analysis Tool',  # Frontend uses this for display
                    'model': model  # Frontend uses this for display
                },
                'trace_metadata': {
                    'tool_name': 'PowerBIAnalysisTool',
                    'operation': operation,
                    'model': model
                },
                'group_context': trace_ctx.get('group_context')
            }

            logger.info(f"[PowerBIAnalysisTool] TRACE EMISSION DEBUG - About to emit trace event: {event_context}")
            queue.put_nowait(trace_event)
            logger.info(f"[PowerBIAnalysisTool] TRACE EMISSION DEBUG - Successfully emitted llm_call trace event: {event_context}")

        except Exception as e:
            # Don't fail the tool execution if trace emission fails
            logger.error(f"[PowerBIAnalysisTool] TRACE EMISSION DEBUG - Failed to emit llm_call trace: {e}", exc_info=True)

    def _extract_dax_from_llm_response(self, content: str) -> str:
        """
        Extract clean DAX query from LLM response.

        Handles various response formats:
        - Markdown code blocks
        - Extra explanatory text before/after query
        - Markdown formatting in the response
        """
        # Remove markdown code blocks
        content = re.sub(r'```dax\s*', '', content)
        content = re.sub(r'```\s*', '', content)

        # Find EVALUATE statement
        evaluate_match = re.search(r'(EVALUATE[\s\S]+?)(?:\n\n|$)', content, re.IGNORECASE)
        if evaluate_match:
            dax_query = evaluate_match.group(1).strip()
        else:
            dax_query = content.strip()

        # Clean up: Remove any markdown or explanatory text that might follow the query
        # Look for the last closing parenthesis that's part of the actual DAX
        # and remove everything after it

        # Strategy: Find EVALUATE, then find the matching closing parenthesis
        # Everything after the last ) that closes a CALCULATETABLE/SUMMARIZECOLUMNS is extra text

        # Remove lines starting with markdown formatting (**, ##, -, etc.) after the query
        lines = dax_query.split('\n')
        clean_lines = []
        found_closing = False
        paren_depth = 0

        for line in lines:
            # Track parenthesis depth
            paren_depth += line.count('(') - line.count(')')

            # If we're at depth 0 after this line, we've closed all parens
            if paren_depth == 0 and ('EVALUATE' in clean_lines or clean_lines):
                clean_lines.append(line)
                found_closing = True
                # Stop after the first complete query (depth returns to 0)
                break

            # Keep building the query
            clean_lines.append(line)

        dax_query = '\n'.join(clean_lines).strip()

        # Final cleanup: Remove any trailing markdown or text after the last )
        # Find the last ) and cut everything after it
        last_paren = dax_query.rfind(')')
        if last_paren != -1:
            # Check if there's any non-whitespace after the last )
            after_paren = dax_query[last_paren + 1:].strip()
            if after_paren and (after_paren.startswith('**') or after_paren.startswith('#') or after_paren.startswith('-')):
                # There's markdown text after the query - remove it
                dax_query = dax_query[:last_paren + 1]

        return dax_query.strip()

    async def _generate_dax_with_self_correction(
        self,
        user_question: str,
        model_context: Dict[str, Any],
        config: Dict[str, Any],
        previous_attempts: List[Dict[str, Any]]
    ) -> Optional[str]:
        """
        Generate DAX with self-correction based on previous failed attempts.
        Uses enriched context (Microsoft Copilot-style) for better error recovery.
        """
        llm_workspace_url = config.get("llm_workspace_url")
        llm_token = config.get("llm_token")
        llm_model = config.get("llm_model", "databricks-claude-sonnet-4-5")

        if not llm_workspace_url or not llm_token:
            return None

        # Build context about previous attempts
        attempts_text = "\n\n".join([
            f"### Attempt {att['attempt']}\n"
            f"**DAX Query:**\n```dax\n{att['dax']}\n```\n"
            f"**Result:** {'✅ SUCCESS' if att['success'] else '❌ FAILED'}\n"
            f"**Error:** {att['error']}" if not att['success'] else ""
            for att in previous_attempts
        ])

        # Build enriched context (same as initial generation)
        enriched_context = self._build_enriched_semantic_context(model_context, config)

        # Create self-correction prompt with enriched context
        prompt = f"""{enriched_context}

## ⚠️ SELF-CORRECTION MODE
Your previous attempt(s) to generate a DAX query failed. Analyze the errors and generate a CORRECTED query.

## 🎯 USER QUESTION
{user_question}

## ❌ PREVIOUS FAILED ATTEMPTS
{attempts_text}

## 📋 ERROR ANALYSIS & CORRECTION INSTRUCTIONS

### Step 1: Analyze the Error
- Read the error message carefully
- Identify the root cause (table name? column name? syntax? filter order?)
- Check if you used non-existent measures, tables, or columns

### Step 2: Use Available Resources
- **ONLY use resources from "SEMANTIC MODEL SCHEMA"** above
- **Check "Business Term Mappings"** for correct filter expressions
- **Check "Field Synonyms"** for correct field names
- **Apply "CURRENT VIEW STATE"** filters automatically

### Step 3: Common Error Patterns & Fixes
1. **"Table/Column doesn't exist"**:
   - Check spelling against "SEMANTIC MODEL SCHEMA"
   - Use exact names (case-sensitive)
   - Don't invent table or column names

2. **"Relationship doesn't exist"**:
   - Use only relationships from schema
   - Use explicit FILTER functions if relationship missing

3. **"All column arguments must be from the same table"**:
   - ❌ NEVER use ALL() with columns from multiple tables
   - ✅ Use TREATAS() for related dimension tables
   - ✅ Use separate FILTER(VALUES(...)) for each table
   - ✅ Or use CALCULATETABLE with direct filters
   - **Example Fix:**
     - Wrong: `ALL(Table1[Col], Table2[Col], Table3[Col])`
     - Right: `TREATAS({"Value"}, Table1[Col])` + `TREATAS({"Value"}, Table2[Col])`

4. **"Syntax error" or "expects column name"**:
   - Check SUMMARIZECOLUMNS argument order
   - Filters MUST come BEFORE measure name/expression pairs

5. **"Type mismatch"**:
   - Check "SAMPLE DATA VALUES" for correct value types
   - String values need quotes: "Italy" not Italy
   - Numeric values don't need quotes: 1 not "1"

6. **"OR operator" not working**:
   - ❌ Don't use: `Column = "Value1 OR Value2"`
   - ✅ Use: `Column IN ("Value1", "Value2")` or `Column IN {"Value1", "Value2"}`

7. **"Failed to resolve name 'STARTSWITH'"**:
   - ❌ NEVER use STARTSWITH() function - not supported by Power BI API
   - ✅ Use LEFT() function instead: `LEFT(Column, 1) = "7"` or `LEFT(Column, LEN("ABC")) = "ABC"`
   - ✅ Use comparison operators: `Column >= "7" && Column < "8"`
   - ✅ Use SEARCH() function: `SEARCH("ABC", Column, 1, 0) = 1`
   - **Example Fix:**
     - Wrong: `STARTSWITH(account_code, "7")`
     - Right: `LEFT(account_code, 1) = "7"`
     - Wrong: `NOT(STARTSWITH(account_code, "7"))`
     - Right: `NOT(LEFT(account_code, 1) = "7")`

### Step 4: Generate Different Query
- **DO NOT repeat the same query** - try a different approach
- If previous query used SUMMARIZECOLUMNS, try ADDCOLUMNS or SELECTCOLUMNS
- Simplify complex filters if needed
- Use step-by-step logic

## ✅ YOUR CORRECTED DAX QUERY
"""

        # Log the self-correction prompt
        logger.info("=" * 80)
        logger.info(f"[DAX Self-Correction] Attempt {len(previous_attempts) + 1} Prompt:")
        logger.info(prompt)
        logger.info("=" * 80)

        # Call LLM via LLMManager
        from src.core.llm_manager import LLMManager
        from src.utils.telemetry import get_user_agent_header, KasalProduct

        try:
            content = await LLMManager.completion(
                messages=[{"role": "user", "content": prompt}],
                model=llm_model,
                temperature=0.1,
                max_tokens=1000,
                extra_headers=get_user_agent_header(KasalProduct.POWERBI),
            )

            # Log response
            logger.info("=" * 80)
            logger.info("[DAX Self-Correction] LLM Response:")
            logger.info(content)
            logger.info("=" * 80)

            # Extract DAX
            dax = self._extract_dax_from_llm_response(content)

            # Validate against available measures
            available_measure_names = [m["name"] for m in model_context.get("measures", [])]

            # Check for hallucinated measures - extract all [measure] references
            dax_pattern = r'\[([^\]]+)\]'
            all_references = re.findall(dax_pattern, dax)

            # Find references that look like measures (not part of table[column])
            potential_measures = []
            for ref in all_references:
                is_table_column = any(f"{table['name']}[{ref}]" in dax for table in model_context.get("tables", []))
                if not is_table_column:
                    potential_measures.append(ref)

            hallucinated = [m for m in potential_measures if m not in available_measure_names]
            if hallucinated:
                logger.warning(f"[DAX Self-Correction] LLM may have used non-existent measures: {hallucinated}")

            logger.info(f"[DAX Self-Correction] Extracted DAX: {dax[:100]}...")
            return dax

        except Exception as e:
            logger.error(f"LLM self-correction error: {e}")
            return None

    def _generate_simple_dax(self, user_question: str, model_context: Dict[str, Any]) -> Optional[str]:
        """Generate a simple DAX query without LLM."""
        measures = model_context.get("measures", [])
        if not measures:
            return None

        # Find best matching measure based on question keywords
        question_lower = user_question.lower()
        best_measure = None

        for measure in measures:
            measure_name_lower = measure["name"].lower()
            if any(word in question_lower for word in measure_name_lower.split()):
                best_measure = measure
                break

        if not best_measure:
            best_measure = measures[0]

        # Generate simple EVALUATE query
        return f"""EVALUATE
SUMMARIZECOLUMNS(
    "Result", [{best_measure['name']}]
)"""

    async def _execute_dax_query(
        self,
        workspace_id: str,
        dataset_id: str,
        access_token: str,
        dax_query: str
    ) -> Dict[str, Any]:
        """Execute DAX query via Power BI Execute Queries API."""
        url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/executeQueries"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        payload = {
            "queries": [{"query": dax_query}],
            "serializerSettings": {"includeNulls": True}
        }

        result = {
            "success": False,
            "data": [],
            "row_count": 0,
            "columns": [],
            "error": None
        }

        async with httpx.AsyncClient(timeout=120.0) as client:
            try:
                response = await client.post(url, headers=headers, json=payload)
                response.raise_for_status()
                data = response.json()

                # Check for errors in response
                if "error" in data:
                    result["error"] = data["error"].get("message", str(data["error"]))
                    return result

                # Extract results
                tables = data.get("results", [{}])[0].get("tables", [])
                if tables:
                    rows = tables[0].get("rows", [])
                    result["data"] = rows
                    result["row_count"] = len(rows)
                    result["success"] = True

                    # Extract column names
                    if rows:
                        result["columns"] = list(rows[0].keys())

                return result

            except httpx.HTTPStatusError as e:
                error_text = e.response.text if hasattr(e.response, 'text') else str(e)
                result["error"] = f"HTTP {e.response.status_code}: {error_text}"
                return result
            except Exception as e:
                result["error"] = str(e)
                return result

    def _extract_measures_from_dax(self, dax_query: str, available_measures: List[str]) -> List[str]:
        """Extract measure names used in a DAX query."""
        used_measures = []
        for measure in available_measures:
            # Check for [MeasureName] pattern
            if f"[{measure}]" in dax_query:
                used_measures.append(measure)
        return used_measures
