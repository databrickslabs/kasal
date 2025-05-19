"""
Seed the tools table with default tool data.
"""
import json
import logging
from datetime import datetime
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.session import async_session_factory, SessionLocal
from src.models.tool import Tool

# Configure logging
logger = logging.getLogger(__name__)

# Tool data as a list of tuples (id, title, description, icon)
tools_data = [
    (1, "BrowserbaseLoadTool", "A powerful tool for interacting with and extracting data from web browsers. It leverages the Browserbase platform to reliably run, manage, and monitor headless browsers, allowing your agents to extract information from complex web UIs, handle stealth mode operations with automatic captcha solving, and access a session debugger for inspection with networks timeline and logs.", "browser"),
    (2, "CodeDocsSearchTool", "A specialized RAG (Retrieval-Augmented Generation) tool optimized for semantic searches within code documentation and related technical documents. It enables efficient discovery of specific information within code documentation by narrowing down searches to specific documentation sites or searching across known documentation sources. Ideal for developers needing to find implementation details, API references, or programming concepts.", "code"),
    (3, "CodeInterpreterTool", "A sophisticated tool for executing and interpreting Python code in a secure environment. It allows agents to write, run, and analyze code dynamically, making it ideal for data analysis, algorithmic problem-solving, visualization creation, and rapid prototyping. The tool provides a sandboxed environment with configurable timeout limits and a dedicated working directory to ensure safe execution.", "code"),
    (4, "ComposioTool", "Enables seamless integration with Composio's ecosystem of tools and services, allowing agents to leverage Composio's specialized capabilities through a unified API interface. This tool extends agent functionality by providing access to Composio's automation workflows, data processing capabilities, and integration services, enhancing the ability to connect with external systems and perform complex operations.", "integration"),
    (5, "CSVSearchTool", "A highly optimized RAG tool designed specifically for searching within CSV files, tailored to handle structured tabular data efficiently. It enables semantic search capabilities across CSV content, allowing agents to extract insights, find patterns, and retrieve specific information from spreadsheet data without having to parse the entire file. Ideal for data analysis, report generation, and information extraction from structured datasets.", "data"),
    (6, "Dall-E Tool", "A creative image generation tool that uses the DALL-E API to transform text descriptions into visual imagery. The tool allows agents to generate custom images based on detailed textual prompts, with configurable parameters including model selection (DALL-E 3), image size, quality settings, and quantity. Perfect for creating visualizations, concept art, illustrations, and other visual content based on textual descriptions.", "ai"),
    (7, "DirectorySearchTool", "A comprehensive RAG tool for semantically searching within file directories, useful for navigating through complex file systems and finding relevant documents based on content rather than just filenames. It enables intelligent exploration of directory structures, facilitating the discovery of relevant files based on their content and context. Ideal for knowledge management, document retrieval, and content organization tasks.", "folder"),
    (8, "DOCXSearchTool", "A specialized RAG tool aimed at searching within Microsoft Word (DOCX) documents, ideal for processing business documents, reports, and other richly formatted text files. It enables semantic search across document content, allowing agents to extract specific information, analyze document structure, and retrieve relevant passages from large documents without needing to read the entire file.", "document"),
    (9, "DirectoryReadTool", "An effective tool for reading and processing entire directory structures and their contents, allowing agents to navigate, analyze, and extract information from file systems efficiently. It provides systematic access to file metadata and content across directories, facilitating batch processing, inventory management, and comprehensive file system analysis.", "folder"),
    (10, "EXASearchTool", "A powerful tool designed for performing exhaustive semantic searches across various web data sources using the EXA search engine. It enables highly accurate search capabilities with neural search technology to find the most relevant content across the web, returning comprehensive results with highlighted excerpts. Ideal for research, content discovery, and gathering up-to-date information from diverse online sources.", "search"),
    (11, "FileReadTool", "A versatile tool for reading and extracting data from various file formats. It supports a wide range of text-based file types including TXT, CSV, JSON, and more, with specialized functionality for different formats (such as converting JSON to Python dictionaries). The tool is designed for batch text processing, configuration file reading, and data import operations, making it essential for any file-based data retrieval and analysis tasks.", "file"),
    (12, "FirecrawlSearchTool", "A specialized web search tool that utilizes Firecrawl technology to perform deep, comprehensive searches across web pages and return highly relevant results. It supports customizable search parameters including content filtering, HTML inclusion, and result limits, making it ideal for targeted web research, content discovery, and information gathering from specific online sources.", "web"),
    (13, "FirecrawlCrawlWebsiteTool", "A powerful web crawling tool that uses Firecrawl to systematically navigate through websites and extract structured information. It can map site architectures, follow links based on configurable depth parameters, and collect comprehensive data from entire domains. Particularly useful for website analysis, content aggregation, and creating searchable indexes of web content.", "web"),
    (14, "FirecrawlScrapeWebsiteTool", "A precise web scraping tool leveraging Firecrawl technology to extract specific content from websites. It can be configured to focus on main content areas while filtering out navigation and promotional elements, with options to include or exclude HTML markup. Ideal for content extraction, data mining, and gathering text from articles, blogs, and other web publications.", "web"),
    (15, "GithubSearchTool", "A specialized RAG tool for searching within GitHub repositories, enabling deep code and documentation searches across various content types including code, repositories, pull requests, and issues. It leverages advanced embedding models for precise semantic matching and can be configured with specific repository URLs or used more broadly across GitHub. Essential for developers analyzing codebases, researching implementation approaches, or exploring open-source projects.", "code"),
    (16, "SerperDevTool", "A sophisticated search tool that integrates with the Serper.dev API to perform high-quality web searches and return structured results. It offers customizable search parameters including result count, geographic targeting (country and locale), and location specificity. This tool excels at retrieving current information from the web, making it essential for real-time research, market analysis, and gathering the latest data on any topic.", "development"),
    (17, "TXTSearchTool", "A focused RAG tool designed specifically for searching within plain text (.txt) files, making it ideal for working with logs, raw data exports, and unformatted documents. It employs semantic search capabilities to locate relevant information within plain text content, helping agents find specific passages, extract key information, and analyze patterns in unstructured text data.", "document"),
    (18, "JSONSearchTool", "A specialized RAG tool for searching within JSON files, optimized for structured data exploration and retrieval. It enables semantic searching across JSON content while maintaining awareness of data structure, making it perfect for API response analysis, configuration management, and working with structured data repositories. The tool understands JSON schemas and can efficiently locate specific data points within complex nested structures.", "data"),
    (19, "LlamaIndexTool", "A powerful integration tool that connects agents to the LlamaIndex ecosystem for advanced document retrieval and querying capabilities. It enables access to LlamaIndex's specialized data structures, retrieval methods, and processing capabilities, making it excellent for building sophisticated knowledge retrieval systems, creating searchable document collections, and implementing advanced RAG applications.", "ai"),
    (20, "MDXSearchTool", "A specialized RAG tool tailored for searching within Markdown (MDX) files, particularly useful for documentation, knowledge bases, and technical content written in Markdown format. It understands Markdown syntax while performing semantic searches, enabling precise retrieval of information from documentation repositories, GitHub wikis, and other Markdown-based content sources.", "document"),
    (21, "PDFSearchTool", "A comprehensive RAG tool aimed at searching within PDF documents, specialized for processing both text-based and scanned PDF files. It employs advanced embedding models for semantic search across PDF content, enabling precise information retrieval from research papers, reports, books, and other PDF publications. The tool can extract text, images, and structure from PDFs, making it essential for research, document analysis, and knowledge extraction tasks.", "document"),
    (22, "PGSearchTool", "A powerful RAG tool optimized for searching within PostgreSQL databases, enabling semantic search capabilities across structured database content. It allows natural language queries against PostgreSQL data, supporting complex data exploration tasks without requiring specialized SQL knowledge. Ideal for data analysis, business intelligence applications, and integrating database content into agent workflows.", "database"),
    (23, "Vision Tool", "An advanced visual processing tool that leverages the GPT-4 Vision model to analyze, understand, and generate insights from images. It can describe image content, identify objects, recognize text in images, analyze visual data, and provide detailed contextual understanding of visual information. Essential for applications involving image analysis, visual content understanding, and multimodal interactions.", "ai"),
    (24, "RagTool", "A versatile, general-purpose Retrieval-Augmented Generation tool capable of working with multiple data sources and types simultaneously. It provides a unified interface for semantic search across various content repositories, supporting dynamic switching between data sources and hybrid retrieval strategies. This tool excels at complex information retrieval tasks that span multiple document types, databases, and knowledge bases.", "ai"),
    (25, "ScrapeElementFromWebsiteTool", "A precision web scraping tool designed for extracting specific HTML elements from websites with high accuracy. It allows targeted data collection from precise website sections using CSS selectors, XPath expressions, or element identifiers. Particularly useful for extracting specific components like pricing tables, contact information, product details, or any structured data presented on web pages.", "web"),
    (26, "ScrapeWebsiteTool", "A comprehensive web scraping tool for extracting entire website content and converting it into structured, usable data. It handles various website complexities including JavaScript rendering, authentication, and cookie management to ensure complete content extraction. Ideal for content aggregation, data collection for analysis, competitive research, and building searchable archives of web content.", "web"),
    (27, "WebsiteSearchTool", "A sophisticated RAG tool for searching within website content, enabling deep semantic exploration of web information without requiring full scraping. It performs contextual searches across web pages, understanding content relationships and semantic meaning to retrieve the most relevant information. Perfect for research, content analysis, and extracting specific information from websites with large content volumes.", "web"),
    (28, "XMLSearchTool", "A specialized RAG tool designed for searching within XML files and data structures, optimized for exploring and retrieving information from structured XML formats. It understands XML schema structures while performing semantic searches, making it ideal for configuration file analysis, API response processing, and working with XML-based data exchanges such as SOAP services, RSS feeds, and SVG graphics.", "data"),
    (29, "YoutubeChannelSearchTool", "A specialized RAG tool for searching within YouTube channels, enabling content discovery and analysis across a channel's video library. It performs semantic searches across video titles, descriptions, transcripts, and metadata, making it perfect for content research, trend analysis, and information gathering from educational, news, or specialized YouTube channels.", "video"),
    (30, "YoutubeVideoSearchTool", "A precise RAG tool aimed at searching within specific YouTube videos, allowing deep content exploration of video materials. It works with video transcripts and metadata to enable semantic search within video content, making it ideal for educational research, finding specific information within lectures or presentations, and extracting insights from long-format video content without watching the entire video.", "video"),
    (31, "PerplexityTool", "A powerful search and question-answering tool that leverages the Perplexity AI platform to provide detailed, accurate answers to complex queries. It combines web search capabilities with advanced language processing to generate comprehensive responses with references and citations. Ideal for research tasks, fact-checking, gathering detailed information on specialized topics, and obtaining nuanced explanations of complex subjects.", "search"),
    (32, "GoogleSlidesTool", "A specialized tool for creating and manipulating Google Slides presentations using Google Apps Script. It enables the programmatic generation of professional presentations with full control over slide content, formatting, transitions, and visual elements. Particularly useful for automatic report generation, creating data-driven presentations, and producing visual content from textual information.", "presentation"),
    (34, "SpiderTool", "An advanced web crawling and content extraction tool designed to systematically navigate through websites, extract content, and return structured information. It features customizable parameters for crawl depth, response formatting, caching, and stealth mode operation, making it ideal for comprehensive web research, content aggregation, and building knowledge bases from web content.", "web"),
    (35, "GenieTool", "A sophisticated database querying tool that enables natural language access to database tables and content. It translates plain language questions into optimized database queries, allowing non-technical users to retrieve complex information from databases without SQL knowledge. Perfect for data analysis, business intelligence applications, and providing database access within conversational interfaces.", "database"),
    (36, "SendPulseEmailTool", "A comprehensive email communication tool that integrates with the SendPulse platform to enable automated email composition and delivery. It supports personalized messaging with customizable templates, sender information, and recipient management, making it essential for customer communications, marketing campaigns, notifications, and any workflow requiring reliable email delivery.", "email"),
    (37, "NixtlaTimeGPTTool", "An advanced time series forecasting tool powered by Nixtla's TimeGPT foundational model. It specializes in analyzing temporal data patterns and generating accurate predictions for future trends and values. Particularly valuable for financial forecasting, demand planning, resource allocation, predictive maintenance, and any application requiring precise time-based predictions.", "ai"),
    (38, "BrowserUseTool", "A versatile browser automation tool that enables programmatic control of web browsers for complex web interactions. It can navigate websites, fill forms, click buttons, and extract information while mimicking human browsing patterns. Essential for web testing, data collection from authenticated sites, process automation, and interacting with complex web applications.", "browser"),
    (39, "FileWriterTool", "A robust file creation and modification tool designed for cross-platform compatibility. It enables writing content to files with configurable parameters for directory paths, encoding formats, and overwrite behavior. Ideal for generating reports, creating data exports, saving processing results, and any workflow requiring persistent storage of information as files.", "file"),
    (40, "AIMindTool", "A sophisticated AI-powered reasoning tool that enhances problem-solving capabilities through advanced cognitive modeling. It employs structured thinking frameworks to break down complex problems, evaluate multiple approaches, and generate comprehensive solutions with explanations. Especially valuable for strategic planning, decision analysis, hypothesis testing, and solving multifaceted problems requiring nuanced thinking.", "ai"),
    (41, "ApifyActorsTool", "A powerful integration tool that connects with the Apify platform to leverage its ecosystem of web scraping and automation actors. It provides access to hundreds of specialized scrapers and automation workflows for various websites and data sources. Particularly useful for large-scale data collection, market monitoring, content aggregation, and automating interactions with websites that require specialized handling.", "web"),
    (42, "BraveSearchTool", "A privacy-focused search tool that utilizes the Brave search engine to find information across the web without tracking user data. It provides comprehensive search results while prioritizing user privacy and minimizing bias in results. Ideal for research, fact verification, content discovery, and general information gathering with enhanced privacy protections.", "search"),
    (43, "DatabricksQueryTool", "A specialized data analysis tool for executing queries against Databricks data warehouses and lakehouses. It enables running SQL queries and data analysis operations on massive datasets stored in Databricks environments, returning structured results for further processing. Essential for data science workflows, business intelligence, large-scale data analysis, and working with big data repositories.", "database"),
    (44, "HyperbrowserLoadTool", "An advanced browser interaction tool that uses Hyperbrowser technology to load and manipulate web content. It provides enhanced capabilities for rendering complex web applications, executing JavaScript, and interacting with dynamic web elements. Particularly valuable for working with modern single-page applications, JavaScript-heavy websites, and web platforms requiring complex interactions.", "browser"),
    (45, "LinkupSearchTool", "A specialized job search and career opportunity discovery tool that connects with the Linkup platform to find relevant employment listings. It can search across multiple industries, locations, and job types to identify career opportunities matching specific criteria. Ideal for career research, recruitment assistance, job market analysis, and providing accurate employment information.", "search"),
    (46, "MultiOnTool", "A versatile multi-purpose utility tool that provides a collection of commonly needed functionality across various domains. It combines multiple helper functions and utilities into a single interface, offering capabilities that span data processing, conversion operations, formatting, and general-purpose computing tasks. Valuable as a Swiss Army knife for agents needing access to diverse utility functions.", "utility"),
    (47, "MySQLSearchTool", "A powerful RAG tool optimized for searching within MySQL databases, enabling semantic exploration of structured data without requiring complex SQL knowledge. It translates natural language queries into optimized SQL, facilitating information retrieval from relational databases while maintaining data integrity and security. Essential for business intelligence applications, data analysis, and integrating database content into conversational interfaces.", "database"),
    (48, "NL2SQLTool", "An advanced natural language to SQL conversion tool that transforms plain language questions into precise SQL queries. It bridges the gap between human language and database query languages, allowing non-technical users to retrieve complex information from databases through conversational interfaces. Particularly valuable for democratizing data access, business intelligence applications, and embedding database querying in AI assistants.", "database"),
    (49, "PatronusEvalTool", "A sophisticated evaluation tool for assessing AI system outputs against defined criteria and standards. It provides objective analysis of response quality, accuracy, relevance, and other key performance metrics, helping identify strengths and weaknesses in AI-generated content. Essential for AI system development, quality assurance, performance monitoring, and continuous improvement of AI applications.", "evaluation"),
    (50, "PatronusLocalEvaluatorTool", "A specialized local evaluation tool for assessing AI outputs without requiring external API calls. It runs evaluation processes entirely on local infrastructure, enabling secure and private assessment of sensitive content. Particularly useful for evaluating AI systems handling confidential information, on-premises AI deployment, and environments with limited external connectivity.", "evaluation"),
    (51, "PatronusPredefinedCriteriaEvalTool", "A structured evaluation tool that assesses AI outputs against a set of predefined criteria and benchmarks. It enables consistent, repeatable evaluation of AI-generated content across multiple dimensions including accuracy, helpfulness, and safety. Valuable for standardized quality assessment, comparative analysis of AI systems, and ensuring AI outputs meet specific requirements or guidelines.", "evaluation"),
    (52, "QdrantVectorSearchTool", "A high-performance vector search tool that leverages the Qdrant vector database for similarity-based information retrieval. It excels at finding content based on semantic similarity rather than keyword matching, enabling nuanced understanding of relationships between concepts. Ideal for semantic search applications, recommendation systems, content discovery, and any use case requiring similarity-based matching.", "search"),
    (53, "ScrapegraphScrapeTool", "A graph-based web scraping tool that understands website structures as interconnected networks of information. It can navigate through complex website structures, following relationships between elements to extract comprehensive datasets. Particularly effective for extracting data from websites with complex hierarchical structures, interconnected information, and relationship-based content organization.", "web"),
    (54, "ScrapflyScrapeWebsiteTool", "A robust web scraping tool powered by Scrapfly technology, designed to handle complex anti-scraping measures and provide reliable data extraction. It leverages proxy rotation, browser fingerprinting, and request throttling to avoid detection while collecting web data. Essential for gathering information from websites with sophisticated protection measures, high-value data sources, and sites that actively block automated access.", "web"),
    (55, "SeleniumScrapingTool", "A powerful web automation and scraping tool built on the Selenium framework, capable of interacting with dynamic websites that require JavaScript rendering. It can navigate complex web applications, fill forms, click buttons, and extract data from elements that only appear after user interactions. Ideal for scraping single-page applications, websites with login requirements, and content hidden behind interactive elements.", "web"),
    (56, "SerpApiGoogleSearchTool", "A comprehensive Google search tool that leverages the SerpApi service to retrieve structured search results with high reliability. It provides access to various Google search features including web results, knowledge panels, featured snippets, and related questions. Perfect for research tasks, content discovery, competitive analysis, and gathering diverse information from the world's largest search engine.", "search"),
    (57, "SerpApiGoogleShoppingTool", "A specialized e-commerce search tool that uses SerpApi to access Google Shopping data in a structured format. It retrieves detailed product information including prices, retailers, ratings, and availability across multiple online stores. Excellent for price comparison, product research, market analysis, and gathering comprehensive product information for decision-making.", "search"),
    (58, "SerplyJobSearchTool", "A dedicated job search tool powered by Serply technology, designed to find employment opportunities across multiple job boards and career sites. It aggregates job listings with detailed information on positions, companies, requirements, and application processes. Valuable for career research, recruitment assistance, job market analysis, and providing personalized employment recommendations.", "search"),
    (59, "SerplyNewsSearchTool", "A news aggregation and search tool that uses Serply to collect and organize news articles from diverse sources. It provides access to current events, trending stories, and specialized news content with filtering options for relevance and recency. Essential for media monitoring, trend analysis, research on current events, and staying informed about specific topics or industries.", "search"),
    (60, "SerplyScholarSearchTool", "An academic research tool powered by Serply that specializes in finding scholarly articles, research papers, and academic publications. It searches across scientific journals, conference proceedings, and academic databases to retrieve relevant scholarly content. Indispensable for literature reviews, academic research, staying current with scientific developments, and exploring specialized academic topics.", "search"),
    (61, "SerplyWebSearchTool", "A versatile web search tool built on Serply technology that performs comprehensive searches across the internet with customizable parameters. It delivers structured search results with metadata and relevance scores, enabling precise information retrieval. Perfect for general research, fact verification, content discovery, and gathering diverse perspectives on any topic from across the web.", "search"),
    (62, "SerplyWebpageToMarkdownTool", "A specialized content transformation tool that converts web pages into clean, well-formatted Markdown text. It extracts the main content from web pages while removing advertisements, navigation elements, and other distractions, preserving the essential information in a structured format. Ideal for content repurposing, knowledge base creation, and preparing web content for further processing or analysis.", "web"),
    (63, "SnowflakeSearchTool", "A powerful search tool optimized for exploring and querying data within Snowflake data warehouses. It enables semantic search capabilities across massive datasets stored in Snowflake environments, combining the power of natural language understanding with enterprise-grade data storage. Essential for business intelligence, data analysis workflows, and integrating cloud data warehouse content into AI applications.", "database"),
    (64, "WeaviateVectorSearchTool", "An advanced vector search tool that integrates with the Weaviate vector database to provide semantic search capabilities across various data types. It excels at finding information based on meaning rather than exact keyword matches, supporting multimodal search across text, images, and other data formats. Particularly valuable for knowledge graphs, recommendation systems, content discovery, and AI applications requiring nuanced understanding of information relationships.", "search"),
    (65, "ZefixScrapingTool", "A specialized scraping tool designed specifically for extracting Swiss company information from the Zefix.ch official business registry. It can retrieve detailed corporate data including registration details, company structure, addresses, and identification numbers through company name or UID searches. Essential for business verification, due diligence research, compliance checks, and gathering official corporate information on Swiss entities.", "web"),
    (66, "CROScrapingTool", "A specialized scraping tool designed for extracting Irish company information from the Companies Registration Office (CRO) at core.cro.ie. It automates searching by company name or number and retrieves official company details for business verification, compliance, and research.", "web"),
    (67, "DatabricksCustomTool", "An enhanced database tool for executing SQL queries against Databricks with full CRUD (Create, Read, Update, Delete) capabilities. It securely connects to Databricks workspaces and allows running SQL operations with proper authentication and configuration from the DatabricksService. Supports all SQL operations including SELECT, INSERT, UPDATE, DELETE, and CREATE statements, with operation-specific result formatting and comprehensive error handling.", "database"),
    (68, "PythonPPTXTool", "A powerful tool for creating Microsoft PowerPoint presentations using the python-pptx library. It converts raw text content into professionally formatted slides with proper styling, titles, and content organization. The tool supports creating presentations from scratch or using templates, customizing styling, and saving to specified locations. Ideal for automating presentation creation, report generation, and converting textual information into visual slide formats.", "presentation"),
    (69, "MCPTool", "An advanced adapter for Model Context Protocol (MCP) servers that enables access to thousands of specialized tools from the MCP ecosystem. This tool establishes and manages connections with MCP servers through SSE (Server-Sent Events), providing seamless integration with community-built tool collections. Perfect for extending agent capabilities with domain-specific tools without requiring custom development or direct integration work.", "integration"),
]

def get_tool_configs():
    """Return the default configurations for each tool."""
    return {
        "1": {
            "result_as_answer": False
        },  # BrowserbaseLoadTool
        "2": {
            "result_as_answer": False
        },  # CodeDocsSearchTool
        "3": {
            "timeout": 60,
            "working_directory": "./code_interpreter_workspace",
            "result_as_answer": False
        },  # CodeInterpreterTool
        "4": {
            "composio_api_key": "your-composio-api-key-here",
            "result_as_answer": False
        },  # ComposioTool
        "5": {
            "result_as_answer": False
        },  # CSVSearchTool
        "6": {
            "model": "dall-e-3",
            "size": "1024x1024",
            "quality": "standard",
            "n": 1,
            "result_as_answer": False
        },  # DallETool
        "7": {
            "result_as_answer": False
        },  # DirectorySearchTool
        "8": {
            "result_as_answer": False
        },  # DOCXSearchTool
        "9": {
            "directory": "./",
            "result_as_answer": False
        },  # DirectoryReadTool
        "10": {
            "result_as_answer": False
        },  # EXASearchTool
        "11": {
            "file_path": "",
            "result_as_answer": False
        },  # FileReadTool
        "12": {
            "api_key": "",
            "page_options": {
                "onlyMainContent": True,
                "includeHtml": False,
                "fetchPageContent": True
            },
            "search_options": {
                "limit": 10
            },
            "result_as_answer": False
        },  # FirecrawlSearchTool
        "13": {
            "api_key": "",
            "page_options": {
                "onlyMainContent": True,
                "includeHtml": False,
                "fetchPageContent": True
            },
            "result_as_answer": False
        },  # FirecrawlCrawlWebsiteTool
        "14": {
            "api_key": "",
            "page_options": {
                "onlyMainContent": True,
                "includeHtml": False,
                "fetchPageContent": True
            },
            "result_as_answer": False
        },  # FirecrawlScrapeWebsiteTool
        "15": {
            "content_types": ["code", "repo", "pr", "issue"],
            "config": {
                "llm": {
                    "model": "gpt-4",
                    "temperature": 0.0
                },
                "embedder": {
                    "model": "text-embedding-ada-002"
                }
            },
            "result_as_answer": False
        },  # GithubSearchTool
        "16": {
            "n_results": 10,
            "search_url": "https://google.serper.dev/search",
            "country": "us",
            "locale": "en",
            "location": "",
            "result_as_answer": False
        },  # SerperDevTool
        "17": {
            "result_as_answer": False
        },  # TXTSearchTool
        "18": {
            "result_as_answer": False
        },  # JSONSearchTool
        "19": {
            "result_as_answer": False
        },  # LlamaIndexTool
        "20": {
            "result_as_answer": False
        },  # MDXSearchTool
        "21": {
            "config": {
                "llm": {
                    "model": "gpt-4",
                    "temperature": 0.0
                },
                "embedder": {
                    "model": "text-embedding-ada-002"
                }
            },
            "result_as_answer": False
        },  # PDFSearchTool
        "22": {
            "result_as_answer": False
        },  # PGSearchTool
        "23": {
            "model": "gpt-4-vision-preview",
            "result_as_answer": False
        },  # VisionTool
        "24": {
            "result_as_answer": False
        },  # RagTool
        "25": {
            "result_as_answer": False
        },  # ScrapeElementFromWebsiteTool
        "26": {
            "result_as_answer": False
        },  # ScrapeWebsiteTool
        "27": {
            "config": {
                "llm": {
                    "model": "gpt-4",
                    "temperature": 0.0
                },
                "embedder": {
                    "model": "text-embedding-ada-002"
                }
            },
            "result_as_answer": False
        },  # WebsiteSearchTool
        "29": {
            "config": {
                "llm": {
                    "model": "gpt-4",
                    "temperature": 0.0
                },
                "embedder": {
                    "model": "text-embedding-ada-002"
                }
            },
            "result_as_answer": False
        },  # YoutubeChannelSearchTool
        "30": {
            "config": {
                "llm": {
                    "model": "gpt-4",
                    "temperature": 0.0
                },
                "embedder": {
                    "model": "text-embedding-ada-002"
                }
            },
            "result_as_answer": False
        },  # YoutubeVideoSearchTool
        "31": {
            "perplexity_api_key": "your-perplexity-api-key-here",
            "result_as_answer": False
        },  # PerplexityTool
        "32": {
            "credentials_path": "path-to-your-google-credentials-json",
            "result_as_answer": False
        },  # GoogleSlidesTool
        "34": {
            "params": {"return_format": "markdown"},
            "request": "smart",
            "limit": 10,
            "depth": 3,
            "cache": True,
            "stealth": True,
            "metadata": True,
            "result_as_answer": False
        },  # SpiderTool
        "35": {
            "spaceId": ["01efdd2cd03211d0ab74f620f0023b77"],
            "DATABRICKS_HOST": ["https://e2-demo-field-eng.cloud.databricks.com/"],
            "result_as_answer": False
        },  # GenieTool
        "36": {
            "sendpulse_api_id": "your-sendpulse-api-id",
            "sendpulse_api_secret": "your-sendpulse-api-secret",
            "default_from_name": "CrewAI Agent",
            "default_from_email": "agent@example.com",
            "default_to_name": "User",
            "default_to_email": "user@example.com",
            "result_as_answer": False
        },  # SendPulseEmailTool
        "37": {
            "nixtla_api_key": "your-nixtla-api-key-here",
            "result_as_answer": False
        },  # NixtlaTimeGPTTool
        "38": {
            "browser_use_api_url": "your-browser-use-api-url-here",
            "result_as_answer": False
        },  # BrowserUseTool
        "39": {
            "default_directory": "./file_outputs",
            "overwrite": True,
            "encoding": "utf-8",
            "result_as_answer": False
        },  # FileWriterTool
        "40": {
            "result_as_answer": False
        },  # AIMindTool
        "41": {
            "result_as_answer": False
        },  # ApifyActorsTool
        "42": {
            "result_as_answer": False
        },  # BraveSearchTool
        "43": {
            "result_as_answer": False
        },  # DatabricksQueryTool
        "44": {
            "result_as_answer": False
        },  # HyperbrowserLoadTool
        "45": {
            "result_as_answer": False,
            # IMPORTANT: depth must be either "standard" or "deep" - any other value will cause API validation errors
            "depth": "standard",
            # IMPORTANT: output_type must be one of: "sourcedAnswer", "searchResults", or "structured"
            "output_type": "searchResults"
        },  # LinkupSearchTool
        "46": {
        },  # MultiOnTool
        "47": {
            "result_as_answer": False
        },  # MySQLSearchTool
        "48": {
            "result_as_answer": False
        },  # NL2SQLTool
        "49": {
            "result_as_answer": False
        },  # PatronusEvalTool
        "50": {
            "result_as_answer": False
        },  # PatronusLocalEvaluatorTool
        "51": {
            "result_as_answer": False
        },  # PatronusPredefinedCriteriaEvalTool
        "52": {
            "result_as_answer": False
        },  # QdrantVectorSearchTool
        "53": {
            "result_as_answer": False
        },  # ScrapegraphScrapeTool
        "54": {
            "result_as_answer": False
        },  # ScrapflyScrapeWebsiteTool
        "55": {
            "result_as_answer": False
        },  # SeleniumScrapingTool
        "56": {
            "result_as_answer": False
        },  # SerpApiGoogleSearchTool
        "57": {
            "result_as_answer": False
        },  # SerpApiGoogleShoppingTool
        "58": {
            "result_as_answer": False
        },  # SerplyJobSearchTool
        "59": {
            "result_as_answer": False
        },  # SerplyNewsSearchTool
        "60": {
            "result_as_answer": False
        },  # SerplyScholarSearchTool
        "61": {
            "result_as_answer": False
        },  # SerplyWebSearchTool
        "62": {
            "result_as_answer": False
        },  # SerplyWebpageToMarkdownTool
        "63": {
            "result_as_answer": False
        },  # SnowflakeSearchTool
        "64": {
            "result_as_answer": False
        },  # WeaviateVectorSearchTool
        "65": {
            "result_as_answer": False,
            "headless": False,  # Force browser visibility
            "wait_time": 5  # Default wait time in seconds
        },   # ZefixScrapingTool
        "66": {
            "result_as_answer": False,
            "headless": False,  # Force browser visibility
            "wait_time": 5  # Default wait time in seconds
        },   # CROScrapingTool
        "67": {
            "result_as_answer": False,
            "catalog": None,  # Will use the configured default from DatabricksService
            "schema": None,   # Will use the configured default from DatabricksService
            "warehouse_id": None,  # Will use the configured default from DatabricksService
        },   # DatabricksCustomTool
        "68": {
            "result_as_answer": False,
            "output_dir": "./presentations",
            "template_path": None
        },   # PythonPPTXTool
        "69": {
            "result_as_answer": False,
            "server_type": "sse",  # Type of MCP server: "sse" or "stdio"
            "server_url": "http://localhost:8001/sse",  # For SSE server type
            "command": "uvx",  # For STDIO server type - command to run the MCP server
            "args": ["--quiet", "pubmedmcp@0.1.3"],  # For STDIO server type - arguments for the command
            "env": {}  # For STDIO server type - additional environment variables
        }   # MCPTool
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
    
    for tool_id, title, description, icon in tools_data:
        try:
            async with async_session_factory() as session:
                if tool_id not in existing_ids:
                    # Add new tool
                    tool = Tool(
                        id=tool_id,
                        title=title,
                        description=description,
                        icon=icon,
                        config=get_tool_configs().get(str(tool_id), {}),
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
    
    for tool_id, title, description, icon in tools_data:
        try:
            with SessionLocal() as session:
                if tool_id not in existing_ids:
                    # Add new tool
                    tool = Tool(
                        id=tool_id,
                        title=title,
                        description=description,
                        icon=icon,
                        config=get_tool_configs().get(str(tool_id), {}),
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