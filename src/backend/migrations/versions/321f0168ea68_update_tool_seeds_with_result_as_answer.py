"""update_tool_seeds_with_result_as_answer

Revision ID: 321f0168ea68
Revises: 038466d3fd55
Create Date: 2025-04-13 13:30:40.946696

"""
from typing import Sequence, Union
import json

from alembic import op
import sqlalchemy as sa
from sqlalchemy.sql import text


# revision identifiers, used by Alembic.
revision: str = '321f0168ea68'
down_revision: Union[str, None] = '038466d3fd55'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Update existing tool seeds with the result_as_answer parameter."""
    # Create a dictionary of tool IDs and their updated configurations
    tool_configs = {
        # BrowserbaseLoadTool
        1: {"result_as_answer": False},
        # CodeDocsSearchTool
        2: {"result_as_answer": False},
        # CodeInterpreterTool
        3: {"timeout": 60, "working_directory": "./code_interpreter_workspace", "result_as_answer": False},
        # ComposioTool
        4: {"composio_api_key": "your-composio-api-key-here", "result_as_answer": False},
        # CSVSearchTool
        5: {"result_as_answer": False},
        # Dall-E Tool
        6: {"model": "dall-e-3", "size": "1024x1024", "quality": "standard", "n": 1, "result_as_answer": False},
        # DirectorySearchTool
        7: {"result_as_answer": False},
        # DOCXSearchTool
        8: {"result_as_answer": False},
        # DirectoryReadTool
        9: {"directory": "./", "result_as_answer": False},
        # EXASearchTool
        10: {"result_as_answer": False},
        # FileReadTool
        11: {"file_path": "", "result_as_answer": False},
        # FirecrawlSearchTool
        12: {"api_key": "", "page_options": {"onlyMainContent": True, "includeHtml": False, "fetchPageContent": True}, 
             "search_options": {"limit": 10}, "result_as_answer": False},
        # FirecrawlCrawlWebsiteTool
        13: {"api_key": "", "page_options": {"onlyMainContent": True, "includeHtml": False, "fetchPageContent": True}, 
             "result_as_answer": False},
        # FirecrawlScrapeWebsiteTool
        14: {"api_key": "", "page_options": {"onlyMainContent": True, "includeHtml": False, "fetchPageContent": True}, 
             "result_as_answer": False},
        # GithubSearchTool
        15: {"content_types": ["code", "repo", "pr", "issue"], 
             "config": {"llm": {"model": "gpt-4", "temperature": 0.0}, "embedder": {"model": "text-embedding-ada-002"}}, 
             "result_as_answer": False},
        # SerperDevTool
        16: {"n_results": 10, "search_url": "https://google.serper.dev/search", "country": "us", 
             "locale": "en", "location": "", "result_as_answer": False},
        # TXTSearchTool
        17: {"result_as_answer": False},
        # JSONSearchTool
        18: {"result_as_answer": False},
        # LlamaIndexTool
        19: {"result_as_answer": False},
        # MDXSearchTool
        20: {"result_as_answer": False},
        # PDFSearchTool
        21: {"config": {"llm": {"model": "gpt-4", "temperature": 0.0}, 
                        "embedder": {"model": "text-embedding-ada-002"}}, 
             "result_as_answer": False},
        # PGSearchTool
        22: {"result_as_answer": False},
        # Vision Tool
        23: {"model": "gpt-4-vision-preview", "result_as_answer": False},
        # RagTool
        24: {"result_as_answer": False},
        # ScrapeElementFromWebsiteTool
        25: {"result_as_answer": False},
        # ScrapeWebsiteTool
        26: {"result_as_answer": False},
        # WebsiteSearchTool
        27: {"config": {"llm": {"model": "gpt-4", "temperature": 0.0}, 
                        "embedder": {"model": "text-embedding-ada-002"}}, 
             "result_as_answer": False},
        # XMLSearchTool
        28: {"result_as_answer": False},
        # YoutubeChannelSearchTool
        29: {"config": {"llm": {"model": "gpt-4", "temperature": 0.0}, 
                        "embedder": {"model": "text-embedding-ada-002"}}, 
             "result_as_answer": False},
        # YoutubeVideoSearchTool
        30: {"config": {"llm": {"model": "gpt-4", "temperature": 0.0}, 
                        "embedder": {"model": "text-embedding-ada-002"}}, 
             "result_as_answer": False},
        # PerplexityTool
        31: {"result_as_answer": False},
        # GoogleSlidesTool
        32: {"result_as_answer": False},
        # SpiderTool
        34: {"result_as_answer": False},
        # GenieTool
        35: {"result_as_answer": False},
        # SendPulseEmailTool
        36: {"result_as_answer": False},
        # NixtlaTimeGPTTool
        37: {"result_as_answer": False},
        # BrowserUseTool
        38: {"result_as_answer": False},
        # FileWriterTool
        39: {"result_as_answer": False},
        # New tools
        40: {"result_as_answer": False},  # AIMindTool
        41: {"result_as_answer": False},  # ApifyActorsTool
        42: {"result_as_answer": False},  # BraveSearchTool
        43: {"result_as_answer": False},  # DatabricksQueryTool
        44: {"result_as_answer": False},  # HyperbrowserLoadTool
        45: {"result_as_answer": False},  # LinkupSearchTool
        46: {"result_as_answer": False},  # MultiOnTool
        47: {"result_as_answer": False},  # MySQLSearchTool
        48: {"result_as_answer": False},  # NL2SQLTool
        49: {"result_as_answer": False},  # PatronusEvalTool
        50: {"result_as_answer": False},  # PatronusLocalEvaluatorTool
        51: {"result_as_answer": False},  # PatronusPredefinedCriteriaEvalTool
        52: {"result_as_answer": False},  # QdrantVectorSearchTool
        53: {"result_as_answer": False},  # ScrapegraphScrapeTool
        54: {"result_as_answer": False},  # ScrapflyScrapeWebsiteTool
        55: {"result_as_answer": False},  # SeleniumScrapingTool
        56: {"result_as_answer": False},  # SerpApiGoogleSearchTool
        57: {"result_as_answer": False},  # SerpApiGoogleShoppingTool
        58: {"result_as_answer": False},  # SerplyJobSearchTool
        59: {"result_as_answer": False},  # SerplyNewsSearchTool
        60: {"result_as_answer": False},  # SerplyScholarSearchTool
        61: {"result_as_answer": False},  # SerplyWebSearchTool
        62: {"result_as_answer": False},  # SerplyWebpageToMarkdownTool
        63: {"result_as_answer": False},  # SnowflakeSearchTool
        64: {"result_as_answer": False},  # WeaviateVectorSearchTool
    }
    
    # Update each tool in the database
    connection = op.get_bind()
    for tool_id, config in tool_configs.items():
        # Convert config to JSON string
        config_json = json.dumps(config)
        
        # Update the tool's config in the database
        query = text(
            "UPDATE tools SET config = :config "
            "WHERE id = :id"
        )
        connection.execute(query, {"config": config_json, "id": tool_id})


def downgrade() -> None:
    """This migration doesn't require a downgrade as it's just updating seed data."""
    pass 