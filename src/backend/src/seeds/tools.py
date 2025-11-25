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
    (36, "DatabricksKnowledgeSearchTool", "A powerful knowledge search tool that enables semantic search across documents uploaded to Databricks Vector Search. It provides RAG (Retrieval-Augmented Generation) capabilities by searching through indexed documents based on vector similarity. This tool allows agents to access and retrieve relevant information from uploaded knowledge files including PDFs, Word documents, text files, and other document formats. Essential for building context-aware AI applications with access to custom knowledge bases.", "search"),
    (69, "MCPTool", "An advanced adapter for Model Context Protocol (MCP) servers that enables access to thousands of specialized tools from the MCP ecosystem. This tool establishes and manages connections with MCP servers through SSE (Server-Sent Events), providing seamless integration with community-built tool collections. Perfect for extending agent capabilities with domain-specific tools without requiring custom development or direct integration work.", "integration"),
    (70, "DatabricksJobsTool", "A comprehensive Databricks Jobs management tool using direct REST API calls for optimal performance. IMPORTANT WORKFLOW: Always use 'get_notebook' action FIRST to analyze job notebooks and understand required parameters before running any job with custom parameters. This ensures proper parameter construction and prevents job failures. Available actions: (1) 'list' - List all jobs in workspace with optional name/ID filtering, (2) 'list_my_jobs' - List only jobs created by current user, (3) 'get' - Get detailed job configuration and recent run history, (4) 'get_notebook' - Analyze notebook content to understand parameters, widgets, and logic (REQUIRED before running jobs with parameters), (5) 'run' - Trigger job execution with custom parameters (use dict for notebook/SQL tasks, list for Python tasks), (6) 'monitor' - Track real-time execution status and task progress, (7) 'create' - Create new jobs with custom configurations. The tool provides intelligent parameter analysis, suggesting proper parameter structures based on notebook patterns (search jobs, ETL jobs, etc.). Supports OAuth/OBO authentication, PAT tokens, and Databricks CLI profiles. All operations use direct REST API calls avoiding SDK overhead for faster execution. Essential for automating data pipelines, orchestrating workflows, and integrating Databricks jobs into AI agent systems.", "database"),
    (71, "PowerBIDAXTool", "Execute DAX queries directly against Power BI semantic models for fast, interactive business intelligence analysis. This tool provides immediate query execution with configurable semantic model and workspace targeting. Perfect for simple data retrieval, dashboard queries, and low-latency analytics. Supports dynamic DAX query construction and returns structured tabular results with execution metrics. Ideal for agents performing real-time Power BI data analysis and reporting tasks.", "database"),
    (72, "PowerBIAnalysisTool", "Execute complex Power BI analysis via Databricks jobs for heavy computational workloads. This tool wraps DAX queries in Databricks job execution, enabling large-scale data processing, multi-query analysis, and resource-intensive computations. Perfect for year-over-year analysis, trend detection, comprehensive reporting, and complex business intelligence tasks that require significant compute resources. Integrates with DatabricksJobsTool for job orchestration and monitoring.", "database"),
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
        "71": {
            "result_as_answer": False
        },   # PowerBIDAXTool
        "72": {
            "result_as_answer": False,
            "databricks_job_id": None  # Required: Databricks job ID for Power BI analysis
        }   # PowerBIAnalysisTool
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
    enabled_tool_ids = [6, 16, 26, 31, 35, 36, 67, 69, 70, 71, 72]

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
    enabled_tool_ids = [6, 16, 26, 31, 35, 36, 67, 69, 70, 71, 72]

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