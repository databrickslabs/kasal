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
    (70, "DatabricksJobsTool", "A comprehensive Databricks Jobs management tool using direct REST API calls for optimal performance. IMPORTANT WORKFLOW: Always use 'get_notebook' action FIRST to analyze job notebooks and understand required parameters before running any job with custom parameters. This ensures proper parameter construction and prevents job failures. Available actions: (1) 'list' - List all jobs in workspace with optional name/ID filtering, (2) 'list_my_jobs' - List only jobs created by current user, (3) 'get' - Get detailed job configuration and recent run history, (4) 'get_notebook' - Analyze notebook content to understand parameters, widgets, and logic (REQUIRED before running jobs with parameters), (5) 'run' - Trigger job execution with custom parameters (use dict for notebook/SQL tasks, list for Python tasks), (6) 'monitor' - Track real-time execution status and task progress, (7) 'create' - Create new jobs with custom configurations. The tool provides intelligent parameter analysis, suggesting proper parameter structures based on notebook patterns (search jobs, ETL jobs, etc.). Supports OAuth/OBO authentication, PAT tokens, and Databricks CLI profiles. All operations use direct REST API calls avoiding SDK overhead for faster execution. Essential for automating data pipelines, orchestrating workflows, and integrating Databricks jobs into AI agent systems.", "database"),
    (72, "PowerBIAnalysisTool", "Execute complex Power BI analysis via Databricks jobs for heavy computational workloads. This tool wraps DAX queries in Databricks job execution, enabling large-scale data processing, multi-query analysis, and resource-intensive computations. Perfect for year-over-year analysis, trend detection, comprehensive reporting, and complex business intelligence tasks that require significant compute resources. Integrates with DatabricksJobsTool for job orchestration and monitoring. IMPORTANT: To enable this tool, you MUST configure the following API Keys in Settings → API Keys: POWERBI_CLIENT_SECRET, POWERBI_USERNAME, POWERBI_PASSWORD, and DATABRICKS_API_KEY (or DATABRICKS_TOKEN).", "database"),
    (73, "Measure Conversion Pipeline", "Universal measure conversion pipeline for converting business metrics between different BI platforms and formats. Supports multiple inbound connectors (Power BI, YAML) and outbound formats (DAX, SQL, UC Metrics, YAML). Perfect for migrating Power BI measures to Databricks SQL, generating UC Metrics from YAML definitions, or converting between different BI platforms. Configure the source format (FROM) and target format (TO) along with authentication credentials in the task configuration. Supports both static configuration (values entered in UI) and dynamic mode (values provided at runtime via execution inputs).", "transform"),
    (74, "M-Query Conversion Pipeline", "Extracts M-Query (Power Query) expressions from Power BI semantic models using the Admin API and converts them to Databricks SQL. This tool scans Power BI workspaces to extract table definitions including Value.NativeQuery (embedded SQL), DatabricksMultiCloud.Catalogs connections, Sql.Database connections, and various Table.* transformations. It generates CREATE VIEW statements for Unity Catalog. Supports both rule-based conversion for simple expressions and LLM-powered conversion for complex M-Query transformations. Perfect for migrating Power BI data models to Databricks, extracting SQL logic for documentation, or analyzing M-Query patterns for migration planning. Requires Service Principal with Power BI Admin API permissions.", "transform"),
    (75, "Power BI Relationships Tool", "Extracts relationships from Power BI semantic models using the Execute Queries API with INFO.VIEW.RELATIONSHIPS() DAX function. Generates Unity Catalog Foreign Key constraint statements (NOT ENFORCED). IMPORTANT: Requires a Service Principal that is a WORKSPACE MEMBER with dataset read permissions - this is different from the Admin API which requires admin-level permissions. Perfect for migrating Power BI relationships to Unity Catalog as informational FKs, documenting data model relationships, or generating DDL for Databricks tables.", "transform"),
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
            "result_as_answer": True
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
            "auth_method": "service_principal"  # Authentication method: "service_principal" or "device_code"
        },  # PowerBIAnalysisTool
        "73": {
            "result_as_answer": True,
            "mode": "static",  # Configuration mode: "static" (UI-configured) or "dynamic" (runtime inputs)
            "inbound_connector": "powerbi",  # Source: "powerbi" or "yaml"
            "outbound_format": "uc_metrics",  # Target: "dax", "sql", "uc_metrics", "yaml"
            # Power BI inbound configuration
            "powerbi_semantic_model_id": "",
            "powerbi_group_id": "",
            "powerbi_tenant_id": "",
            "powerbi_client_id": "",
            "powerbi_client_secret": "",
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
            # Admin API Service Principal authentication (required)
            "tenant_id": "",
            "client_id": "",
            "client_secret": "",
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
            # Service Principal authentication (must be workspace member)
            "tenant_id": "",
            "client_id": "",
            "client_secret": "",
            # Unity Catalog Target (supports {placeholder} syntax in dynamic mode)
            "target_catalog": "main",
            "target_schema": "default",
            # Output Options
            "include_inactive": False,
            "skip_system_tables": True
        }   # Power BI Relationships Tool
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
    enabled_tool_ids = [6, 16, 26, 31, 35, 36, 67, 69, 70, 71, 72, 73, 74, 75]

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
    enabled_tool_ids = [6, 16, 26, 31, 35, 36, 67, 69, 70, 71, 72, 73, 74, 75]

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