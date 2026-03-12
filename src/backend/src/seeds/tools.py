"""
Seed the tools table with default tool data.
"""
import json
import logging
from datetime import datetime
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.session import async_session_factory
from src.models.tool import Tool

# Configure logging
logger = logging.getLogger(__name__)

# Tool data as a list of tuples (id, title, description, icon)
# Only keeping the safe and approved tools
tools_data = [
    (6, "Dall-E Tool", "A creative image generation tool that uses the DALL-E API to transform text descriptions into visual imagery. The tool allows agents to generate custom images based on detailed textual prompts, with configurable parameters including model selection (DALL-E 3), image size, quality settings, and quantity. Perfect for creating visualizations, concept art, illustrations, and other visual content based on textual descriptions.", "ai"),
    (16, "SerperDevTool", "A sophisticated search tool that integrates with the Serper.dev API to perform high-quality web searches and return structured results. It offers customizable search parameters including result count, geographic targeting (country and locale), and location specificity. This tool excels at retrieving current information from the web, making it essential for real-time research, market analysis, and gathering the latest data on any topic.", "development"),
    (26, "ScrapeWebsiteTool", "A comprehensive web scraping tool for extracting entire website content and converting it into structured, usable data. It handles various website complexities including JavaScript rendering, authentication, and cookie management to ensure complete content extraction. Ideal for content aggregation, data collection for analysis, competitive research, and building searchable archives of web content.", "web"),
    (31, "PerplexityTool", "A powerful search and question-answering tool that leverages the Perplexity AI platform to provide detailed, accurate answers to complex queries. It combines web search capabilities with advanced language processing to generate comprehensive responses with references and citations. Ideal for research tasks, fact-checking, gathering detailed information on specialized topics, and obtaining nuanced explanations of complex subjects.", "search"),
    (35, "GenieTool", "A sophisticated database querying tool that enables natural language access to database tables and content. It translates plain language questions into optimized database queries, allowing non-technical users to retrieve complex information from databases without SQL knowledge. Perfect for data analysis, business intelligence applications, and providing database access within conversational interfaces.", "database"),
    (71, "AgentBricksTool", "A powerful tool for querying Databricks AgentBricks (Mosaic AI Agent Bricks) endpoints. AgentBricks is Databricks' no-code AI agent builder platform that enables users to create sophisticated AI agents without writing code. This tool provides seamless integration with AgentBricks serving endpoints, allowing CrewAI agents to leverage pre-built Databricks AI agents for specialized tasks. It supports full authentication (OBO, PAT, Service Principal), customizable inputs, and execution tracing. Ideal for integrating Databricks-native AI agents into multi-agent workflows, accessing domain-specific agents, and building hybrid AI systems that combine CrewAI orchestration with Databricks AgentBricks capabilities.", "databricks"),
    (36, "DatabricksKnowledgeSearchTool", "A powerful knowledge search tool that enables semantic search across documents uploaded to Databricks Vector Search. It provides RAG (Retrieval-Augmented Generation) capabilities by searching through indexed documents based on vector similarity. This tool allows agents to access and retrieve relevant information from uploaded knowledge files including PDFs, Word documents, text files, and other document formats. Essential for building context-aware AI applications with access to custom knowledge bases.", "search"),
    (69, "MCPTool", "An advanced adapter for Model Context Protocol (MCP) servers that enables access to thousands of specialized tools from the MCP ecosystem. This tool establishes and manages connections with MCP servers through SSE (Server-Sent Events), providing seamless integration with community-built tool collections. Perfect for extending agent capabilities with domain-specific tools without requiring custom development or direct integration work.", "integration"),
    (70, "DatabricksJobsTool", "A comprehensive Databricks Jobs management tool using REST API 2.2 for optimal performance. IMPORTANT WORKFLOW: Always use 'get_notebook' action FIRST to analyze job notebooks and understand required parameters before running any job with custom parameters. Available actions: (1) 'list' - List all jobs with optional name/ID filtering and pagination, (2) 'list_my_jobs' - List only jobs created by current user, (3) 'get' - Get detailed job configuration and recent run history, (4) 'get_notebook' - Analyze notebook content to understand parameters and widgets, (5) 'run' - Trigger job execution with custom parameters (dict for notebook/SQL, list for Python), (6) 'monitor' - Track real-time execution status, (7) 'create' - Create new jobs, (8) 'get_output' - Get output/results of a completed run, (9) 'submit' - Submit a one-time run without creating a persistent job. Supports PAT token authentication (note: Jobs API does NOT support OBO scopes). All operations use direct REST API 2.2 calls for fast execution. Destructive actions (delete, cancel, update) are excluded for agent safety.", "database"),
    (72, "Power BI Comprehensive Analysis Tool", "Answer ad-hoc business questions by converting natural language queries into DAX queries with intelligent self-correction (up to 5 retries). This tool extracts Power BI model context (measures, tables, relationships), uses LLM to generate DAX from user questions, executes queries via Power BI Execute Queries API, and identifies which reports use the queried measures. Features measure hallucination detection to prevent incorrect data and enhanced logging for debugging. Perfect for data exploration, self-service BI, validating measure logic, and learning DAX through LLM-generated examples. Requires Service Principal with SemanticModel.ReadWrite.All permission or user OAuth token. Supports both LLM-powered intelligent DAX generation and keyword-based fallback for simple queries.", "database"),
    (73, "Measure Conversion Pipeline", "Universal measure conversion pipeline for converting business metrics between different BI platforms and formats. Supports multiple inbound connectors (Power BI, YAML) and outbound formats (DAX, SQL, UC Metrics, YAML). Perfect for migrating Power BI measures to Databricks SQL, generating UC Metrics from YAML definitions, or converting between different BI platforms. Configure the source format (FROM) and target format (TO) along with authentication credentials in the task configuration. Supports both static configuration (values entered in UI) and dynamic mode (values provided at runtime via execution inputs).", "transform"),
    (74, "M-Query Conversion Pipeline", "Extracts M-Query (Power Query) expressions from Power BI semantic models using the Admin API and converts them to Databricks SQL. This tool scans Power BI workspaces to extract table definitions including Value.NativeQuery (embedded SQL), DatabricksMultiCloud.Catalogs connections, Sql.Database connections, and various Table.* transformations. It generates CREATE VIEW statements for Unity Catalog. Supports both rule-based conversion for simple expressions and LLM-powered conversion for complex M-Query transformations. Perfect for migrating Power BI data models to Databricks, extracting SQL logic for documentation, or analyzing M-Query patterns for migration planning. Requires Service Principal with Power BI Admin API permissions.", "transform"),
    (75, "Power BI Relationships Tool", "Extracts relationships from Power BI semantic models using the Execute Queries API with INFO.VIEW.RELATIONSHIPS() DAX function. Generates Unity Catalog Foreign Key constraint statements (NOT ENFORCED). IMPORTANT: Requires a Service Principal that is a WORKSPACE MEMBER with dataset read permissions - this is different from the Admin API which requires admin-level permissions. Perfect for migrating Power BI relationships to Unity Catalog as informational FKs, documenting data model relationships, or generating DDL for Databricks tables.", "transform"),
    (76, "Power BI Hierarchies Tool", "Extracts hierarchies from Microsoft Fabric semantic models using the Fabric API getDefinition endpoint (TMDL format). Parses TMDL to extract hierarchy definitions and generates Unity Catalog dimension views with hierarchy_path columns plus metadata table DDL. IMPORTANT: Requires a Service Principal with SemanticModel.ReadWrite.All permissions and works with Fabric workspaces only (not legacy Power BI Service). Perfect for migrating Power BI hierarchies to Databricks as dimension views, documenting drill-down structures, or generating DDL for dimension tables.", "transform"),
    (77, "Power BI Field Parameters & Calculation Groups Tool", "Extracts Field Parameters and Calculation Groups from Microsoft Fabric semantic models using the Fabric API getDefinition endpoint (TMDL format). Field Parameters allow users to dynamically switch between measures in reports using NAMEOF() DAX functions. Calculation Groups provide reusable time intelligence calculations (YTD, PY, YoY%, MTD) using SELECTEDMEASURE() patterns. Generates Unity Catalog metadata tables with parameter/calculation item details and SQL patterns for implementing equivalent logic. IMPORTANT: Requires a Service Principal with SemanticModel.ReadWrite.All permissions and works with Fabric workspaces only (not legacy Power BI Service). Perfect for documenting Power BI dynamic measure switching, migrating time intelligence patterns to Databricks, or generating SQL equivalents for calculation group logic.", "transform"),
    (78, "Power BI Report References Tool", "Extracts visual-to-measure and visual-to-table references from Microsoft Fabric reports using the Fabric Report Definition API (PBIR format). Shows which measures, tables, and fields are used in each report page and visual. Output formats include markdown (grouped by page, measure, or table), JSON, and matrix view. IMPORTANT: Requires a Service Principal with Report.ReadWrite.All permissions and works only with Fabric reports in PBIR format. Perfect for understanding report dependencies, impact analysis for measure/table changes, identifying unused measures, and documenting report-to-semantic-model relationships.", "transform"),
    (79, "Power BI Semantic Model Fetcher", "Extracts and caches semantic model metadata (measures, tables, relationships, columns, sample data, default filters) from Power BI. Uses 3-tier fallback: Fabric TMDL API, Admin Scanner API, or DAX-based extraction. Output is JSON that can be fed directly into the 'Power BI Semantic Model DAX Generator' tool for multi-step workflows. Caches metadata for same-day reuse. Requires Service Principal with SemanticModel.ReadWrite.All permission or user OAuth token.", "database"),
    (80, "Power BI Semantic Model DAX Generator", "Generates and executes DAX queries from natural language questions using LLM with self-correction retry loop (up to N retries). Accepts model context JSON from the 'Power BI Semantic Model Fetcher' tool output, or reads from cache as fallback. Features business term mappings, field synonyms, active filter auto-application, and optional visual reference lookup. Requires Service Principal or user OAuth token for DAX execution, plus Databricks LLM endpoint for DAX generation.", "database"),
    (81, "Power BI Metadata Reducer", "Intelligently reduces semantic model metadata to only what's relevant for a specific question. Uses fuzzy matching, LLM-powered table/measure selection, and measure dependency resolution to filter the full model context from the Fetcher tool. Produces a focused, reduced JSON that dramatically improves DAX generation accuracy. Place between Fetcher and DAX Generator tools in multi-step workflows. Pass the Fetcher output as 'model_context_json' and the user's business question as 'user_question'.", "database"),
]

def get_tool_configs():
    """Return the default configurations for each tool."""
    return {
        "6": {
            "model": "dall-e-3",
            "size": "1024x1024",
            "quality": "standard",
            "n": 1,
            "result_as_answer": False
        },  # DallETool
        "16": {
            "n_results": 10,
            "search_url": "https://google.serper.dev/search",
            "country": "us",
            "locale": "en",
            "location": "",
            "result_as_answer": False
        },  # SerperDevTool
        "26": {
            "result_as_answer": False
        },  # ScrapeWebsiteTool
        "31": {
            "model": "sonar",  # Options: sonar, sonar-pro, sonar-deep-research, sonar-reasoning, sonar-reasoning-pro, r1-1776
            "max_tokens": 2000,  # Max output tokens (default: 2000, documented limit: 4000)
            "temperature": 0.1,  # Controls randomness (0.0-1.0)
            "top_p": 0.9,  # Nucleus sampling parameter
            "top_k": 0,  # Top-k sampling parameter
            "presence_penalty": 0.0,  # Penalizes new topics (-2.0 to 2.0)
            "frequency_penalty": 1.0,  # Penalizes repetition (-2.0 to 2.0)
            "search_recency_filter": "month",  # Options: day, week, month, year
            "search_domain_filter": ["<any>"],  # List of domains to search or ["<any>"] for all
            "return_images": False,  # Include images in response
            "return_related_questions": False,  # Include related questions
            "web_search_options": {
                "search_context_size": "high"  # Options: low, medium, high
            },
            "result_as_answer": False
        },  # PerplexityTool
        "35": {
            "result_as_answer": False,
            "max_calls": 5,
            "max_result_rows": 200
        },  # GenieTool
        "71": {
            "result_as_answer": True,
            "timeout": 120  # Timeout in seconds for AgentBricks endpoint queries
        },  # AgentBricksTool
        "36": {
            "result_as_answer": False
        },  # DatabricksKnowledgeSearchTool
        "69": {
            "result_as_answer": False,
            "server_type": "sse",  # Type of MCP server: "sse" or "stdio"
            "server_url": "http://localhost:8001/sse",  # For SSE server type
            "command": "uvx",  # For STDIO server type - command to run the MCP server
            "args": ["--quiet", "pubmedmcp@0.1.3"],  # For STDIO server type - arguments for the command
            "env": {}  # For STDIO server type - additional environment variables
        },   # MCPTool
        "70": {
            "result_as_answer": False,
            "DATABRICKS_HOST": "",  # Databricks workspace URL (e.g., "e2-demo-field-eng.cloud.databricks.com")
        },   # DatabricksJobsTool
        "72": {
            "result_as_answer": False,
            "databricks_job_id": None,  # Required: Databricks job ID for Power BI analysis
            "tenant_id": "",  # Azure AD Tenant ID (required)
            "client_id": "",  # Azure AD Application/Client ID (required)
            "workspace_id": "",  # Default Power BI Workspace ID (optional, can be overridden per task)
            "semantic_model_id": "",  # Default Power BI Semantic Model ID (optional, can be overridden per task)
            "auth_method": None,  # Authentication method: "service_principal" or "service_account" (None = use UI selection)
            "username": "",  # Service Account username/UPN (for service_account auth)
            "password": "",  # Service Account password (for service_account auth)
        },  # PowerBIAnalysisTool
        "73": {
            "result_as_answer": True,
            "mode": "static",  # Configuration mode: "static" (UI-configured) or "dynamic" (runtime inputs)
            "inbound_connector": "powerbi",  # Source: "powerbi" or "yaml"
            "outbound_format": "uc_metrics",  # Target: "dax", "sql", "uc_metrics", "yaml"
            # Power BI inbound configuration
            "powerbi_semantic_model_id": "",
            "powerbi_group_id": "",
            # Service Principal Authentication
            "powerbi_tenant_id": "",
            "powerbi_client_id": "",
            "powerbi_client_secret": "",
            # Service Account Authentication
            "powerbi_username": "",
            "powerbi_password": "",
            "powerbi_auth_method": None,  # "service_principal", "service_account", or None for auto-detect
            # User OAuth Authentication
            "powerbi_access_token": "",
            # Other Power BI settings
            "powerbi_info_table_name": "Info Measures",
            "powerbi_include_hidden": False,
            "powerbi_filter_pattern": "",
            # SQL outbound configuration
            "sql_dialect": "databricks",
            "sql_include_comments": True,
            "sql_process_structures": True,
            # UC Metrics outbound configuration
            "uc_catalog": "main",
            "uc_schema": "default",
            "uc_process_structures": True,
            # DAX outbound configuration
            "dax_process_structures": True
        },  # Measure Conversion Pipeline
        "74": {
            "result_as_answer": True,
            "mode": "static",  # Configuration mode: "static" (UI-configured) or "dynamic" (runtime inputs)
            # Power BI Admin API configuration
            "workspace_id": "",
            "dataset_id": "",
            # Service Principal Authentication
            "tenant_id": "",
            "client_id": "",
            "client_secret": "",
            # Service Account Authentication
            "username": "",
            "password": "",
            "auth_method": None,  # None for auto-detect, or explicit "service_principal"/"service_account"/"user_oauth"
            # User OAuth Authentication
            "access_token": "",
            # LLM Configuration (optional)
            "llm_workspace_url": "",
            "llm_token": "",
            "llm_model": "databricks-claude-sonnet-4",
            "use_llm": True,
            # Scan Options
            "include_lineage": True,
            "include_datasource_details": True,
            "include_dataset_schema": True,
            "include_dataset_expressions": True,
            "include_hidden_tables": False,
            "skip_static_tables": True,
            # Output Options
            "include_summary": True
        },  # M-Query Conversion Pipeline
        "75": {
            "result_as_answer": True,
            "mode": "static",  # Configuration mode: "static" (UI-configured) or "dynamic" (runtime inputs with {placeholders})
            # Power BI Configuration (supports {placeholder} syntax in dynamic mode)
            "workspace_id": "",
            "dataset_id": "",
            # Service Principal Authentication
            "tenant_id": "",
            "client_id": "",
            "client_secret": "",
            # Service Account Authentication
            "username": "",
            "password": "",
            "auth_method": None,  # None for auto-detect, or explicit "service_principal"/"service_account"/"user_oauth"
            # User OAuth Authentication
            "access_token": "",
            # Unity Catalog Target (supports {placeholder} syntax in dynamic mode)
            "target_catalog": "main",
            "target_schema": "default",
            # Output Options
            "include_inactive": False,
            "skip_system_tables": True
        },  # Power BI Relationships Tool
        "76": {
            "result_as_answer": True,
            "mode": "static",  # Configuration mode: "static" (UI-configured) or "dynamic" (runtime inputs with {placeholders})
            # Power BI / Fabric Configuration (supports {placeholder} syntax in dynamic mode)
            "workspace_id": "",
            "dataset_id": "",
            # Service Principal Authentication
            "tenant_id": "",
            "client_id": "",
            "client_secret": "",
            # Service Account Authentication
            "username": "",
            "password": "",
            "auth_method": None,  # None for auto-detect, or explicit "service_principal"/"service_account"/"user_oauth"
            # User OAuth Authentication
            "access_token": "",
            # Unity Catalog Target (supports {placeholder} syntax in dynamic mode)
            "target_catalog": "main",
            "target_schema": "default",
            # Output Options
            "skip_system_tables": True,
            "include_hidden": False
        },   # Power BI Hierarchies Tool
        "77": {
            "result_as_answer": True,
            "mode": "static",  # Configuration mode: "static" (UI-configured) or "dynamic" (runtime inputs with {placeholders})
            # Power BI / Fabric Configuration (supports {placeholder} syntax in dynamic mode)
            "workspace_id": "",
            "dataset_id": "",
            # Service Principal Authentication
            "tenant_id": "",
            "client_id": "",
            "client_secret": "",
            # Service Account Authentication
            "username": "",
            "password": "",
            "auth_method": None,  # None for auto-detect, or explicit "service_principal"/"service_account"/"user_oauth"
            # User OAuth Authentication
            "access_token": "",
            # Unity Catalog Target (supports {placeholder} syntax in dynamic mode)
            "target_catalog": "main",
            "target_schema": "default",
            # Output Options
            "output_format": "markdown",  # Output format: "markdown", "json", or "sql"
            "skip_system_tables": True,
            "include_hidden": False
        },  # Power BI Field Parameters & Calculation Groups Tool
        "78": {
            # Mode: "static" = use values below, "dynamic" = resolve from execution_inputs
            "mode": "static",
            # Power BI Configuration (supports {placeholder} syntax in dynamic mode)
            "workspace_id": "",
            "dataset_id": "",  # Recommended: discovers ALL reports using this dataset
            "report_id": "",   # Alternative: single specific report (ignored if dataset_id provided)
            # Service Principal authentication (requires Report.ReadWrite.All)
            "tenant_id": "",
            "client_id": "",
            "client_secret": "",
            # Output Options
            "output_format": "markdown",  # Output format: "markdown", "json", or "matrix"
            "include_visual_details": True,
            "group_by": "page"  # Group results by: "page", "measure", or "table"
        },   # Power BI Report References Tool
        "79": {
            "result_as_answer": True,
            "tenant_id": "",
            "client_id": "",
            "client_secret": "",
            "workspace_id": "",
            "semantic_model_id": "",  # Alias for dataset_id
            "auth_method": None,
            "username": "",
            "password": "",
            "output_format": "json",
        },  # Power BI Semantic Model Fetcher
        "80": {
            "result_as_answer": False,
            "tenant_id": "",
            "client_id": "",
            "client_secret": "",
            "workspace_id": "",
            "semantic_model_id": "",  # Alias for dataset_id
            "auth_method": None,
            "username": "",
            "password": "",
            "llm_model": "databricks-claude-sonnet-4",
            "max_dax_retries": 5,
        },  # Power BI Semantic Model DAX Generator
        "81": {
            "result_as_answer": True,
            "strategy": "combined",
            "synonym_threshold": 70,
            "synonym_boost_min": 60.0,
            "max_tables": 15,
            "max_measures": 30,
            "enable_value_normalization": True,
            "dataset_id": "",
            "workspace_id": "",
            "llm_model": "databricks-claude-sonnet-4",
        },  # Power BI Metadata Reducer
    }

async def seed_async():
    """Seed tools into the database using async session."""
    logger.info("Seeding tools table (async)...")

    # Get existing tool IDs to avoid duplicates
    async with async_session_factory() as session:
        result = await session.execute(select(Tool.id))
        existing_ids = set(result.scalars().all())

    tools_added = 0
    tools_updated = 0
    tools_skipped = 0
    tools_error = 0

    # List of tool IDs that should be enabled
    enabled_tool_ids = [6, 16, 26, 31, 35, 36, 67, 69, 70, 71, 72, 73, 74, 75, 76, 77, 79, 80, 81]

    for tool_id, title, description, icon in tools_data:
        try:
            async with async_session_factory() as session:
                if tool_id not in existing_ids:
                    # Add new tool - all tools in the list are enabled by default
                    tool = Tool(
                        id=tool_id,
                        title=title,
                        description=description,
                        icon=icon,
                        config=get_tool_configs().get(str(tool_id), {}),
                        enabled=True,  # All tools in this curated list are enabled
                        group_id=None,  # Global tools available to all groups
                        created_at=datetime.now().replace(tzinfo=None),
                        updated_at=datetime.now().replace(tzinfo=None)
                    )
                    session.add(tool)
                    tools_added += 1
                else:
                    # Update existing tool
                    result = await session.execute(
                        select(Tool).filter(Tool.id == tool_id)
                    )
                    existing_tool = result.scalars().first()
                    if existing_tool:
                        existing_tool.title = title
                        existing_tool.description = description
                        existing_tool.icon = icon
                        existing_tool.config = get_tool_configs().get(str(tool_id), {})
                        existing_tool.enabled = True  # All tools in this curated list are enabled
                        existing_tool.group_id = None  # Ensure global tools are available to all groups
                        existing_tool.updated_at = datetime.now().replace(tzinfo=None)
                        tools_updated += 1

                try:
                    await session.commit()
                except Exception as e:
                    await session.rollback()
                    logger.error(f"Failed to commit tool {tool_id}: {str(e)}")
                    tools_error += 1
        except Exception as e:
            logger.error(f"Error processing tool {tool_id}: {str(e)}")
            tools_error += 1

    logger.info(f"Tools seeding summary: Added {tools_added}, Updated {tools_updated}, Skipped {tools_skipped}, Errors {tools_error}")

def seed_sync():
    """Seed tools into the database using sync session."""
    logger.info("Seeding tools table (sync)...")

    # Get existing tool IDs to avoid duplicates
    with SessionLocal() as session:
        result = session.execute(select(Tool.id))
        existing_ids = set(result.scalars().all())

    tools_added = 0
    tools_updated = 0
    tools_skipped = 0
    tools_error = 0

    # List of tool IDs that should be enabled
    enabled_tool_ids = [6, 16, 26, 31, 35, 36, 67, 69, 70, 71, 72, 73, 74, 75, 76, 77, 79, 80, 81]

    for tool_id, title, description, icon in tools_data:
        try:
            with SessionLocal() as session:
                if tool_id not in existing_ids:
                    # Add new tool - all tools in the list are enabled by default
                    tool = Tool(
                        id=tool_id,
                        title=title,
                        description=description,
                        icon=icon,
                        config=get_tool_configs().get(str(tool_id), {}),
                        enabled=True,  # All tools in this curated list are enabled
                        group_id=None,  # Global tools available to all groups
                        created_at=datetime.now().replace(tzinfo=None),
                        updated_at=datetime.now().replace(tzinfo=None)
                    )
                    session.add(tool)
                    tools_added += 1
                else:
                    # Update existing tool
                    result = session.execute(
                        select(Tool).filter(Tool.id == tool_id)
                    )
                    existing_tool = result.scalars().first()
                    if existing_tool:
                        existing_tool.title = title
                        existing_tool.description = description
                        existing_tool.icon = icon
                        existing_tool.config = get_tool_configs().get(str(tool_id), {})
                        existing_tool.enabled = True  # All tools in this curated list are enabled
                        existing_tool.updated_at = datetime.now().replace(tzinfo=None)
                        tools_updated += 1

                try:
                    session.commit()
                except Exception as e:
                    session.rollback()
                    logger.error(f"Failed to commit tool {tool_id}: {str(e)}")
                    tools_error += 1
        except Exception as e:
            logger.error(f"Error processing tool {tool_id}: {str(e)}")
            tools_error += 1

    logger.info(f"Tools seeding summary: Added {tools_added}, Updated {tools_updated}, Skipped {tools_skipped}, Errors {tools_error}")

# Main entry point for seeding - can be called directly or by seed_runner
async def seed():
    """Main entry point for seeding tools."""
    logger.info("Tools seed function called")
    try:
        logger.info("Attempting to call seed_async in tools.py")
        await seed_async()
        logger.info("Tools seed_async completed successfully")
    except Exception as e:
        logger.error(f"Error in tools seed function: {str(e)}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise

# For direct external calls - call seed() instead
if __name__ == "__main__":
    import asyncio
    asyncio.run(seed())