"""
Seed the schemas table with default schema definitions.
"""
import json
import logging
from datetime import datetime
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.session import async_session_factory, SessionLocal
from src.models.schema import Schema

# Configure logging
logger = logging.getLogger(__name__)

# Sample schemas definition
SAMPLE_SCHEMAS = [
    {
        "name": "ResearchReport",
        "description": "A comprehensive research report with findings, sources, and recommendations",
        "schema_type": "data_model",
        "schema_definition": {
            "type": "object",
            "properties": {
                "title": {
                    "type": "string",
                    "description": "The title of the research report"
                },
                "executive_summary": {
                    "type": "string",
                    "description": "A brief summary of the key findings and recommendations"
                },
                "findings": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "section": {
                                "type": "string",
                                "description": "The section title for this finding"
                            },
                            "content": {
                                "type": "string",
                                "description": "The detailed content of the finding"
                            },
                            "importance": {
                                "type": "string",
                                "enum": ["low", "medium", "high", "critical"],
                                "description": "The importance level of this finding"
                            }
                        },
                        "required": ["section", "content"]
                    },
                    "description": "List of key findings from the research"
                },
                "sources": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "title": {
                                "type": "string",
                                "description": "The title of the source"
                            },
                            "url": {
                                "type": "string",
                                "description": "URL or reference location"
                            },
                            "credibility": {
                                "type": "integer",
                                "minimum": 1,
                                "maximum": 10,
                                "description": "Source credibility rating from 1-10"
                            }
                        },
                        "required": ["title"]
                    },
                    "description": "List of sources used in the research"
                },
                "recommendations": {
                    "type": "array",
                    "items": {
                        "type": "string"
                    },
                    "description": "List of actionable recommendations based on the findings"
                }
            },
            "required": ["title", "executive_summary", "findings", "recommendations"]
        },
        "field_descriptions": {
            "title": "The main title of the research report",
            "executive_summary": "A concise summary of the key points, findings, and recommendations",
            "findings": "Detailed discoveries and insights from the research",
            "sources": "References and citations that support the findings",
            "recommendations": "Actionable suggestions based on the research findings"
        },
        "keywords": ["research", "report", "analysis", "findings", "recommendations"],
        "tools": ["research_assistant", "data_analyzer"],
        "example_data": {
            "title": "Market Analysis: Emerging AI Trends in 2023",
            "executive_summary": "This report analyzes the current trends in AI technology adoption across industries, highlighting key opportunities and challenges for businesses.",
            "findings": [
                {
                    "section": "Large Language Models",
                    "content": "Large language models have become increasingly accessible to small and medium-sized businesses, with specialized models showing superior performance in domain-specific tasks.",
                    "importance": "high"
                },
                {
                    "section": "Regulatory Landscape",
                    "content": "New AI regulations in Europe and North America are creating compliance challenges but also opportunities for companies offering compliance solutions.",
                    "importance": "critical"
                }
            ],
            "sources": [
                {
                    "title": "State of AI Report 2023",
                    "url": "https://example.com/ai-report-2023",
                    "credibility": 9
                },
                {
                    "title": "Industry Survey: AI Adoption Rates",
                    "url": "https://example.com/ai-survey",
                    "credibility": 7
                }
            ],
            "recommendations": [
                "Invest in domain-specific AI training to outperform generic solutions",
                "Develop an AI compliance framework aligned with emerging regulations",
                "Focus on AI explainability to build customer trust"
            ]
        }
    },
    {
        "name": "ProductRequirements",
        "description": "Detailed product requirements document for development teams",
        "schema_type": "data_model",
        "schema_definition": {
            "type": "object",
            "properties": {
                "product_name": {
                    "type": "string",
                    "description": "Name of the product"
                },
                "version": {
                    "type": "string",
                    "description": "Version number"
                },
                "objective": {
                    "type": "string",
                    "description": "Primary objective of the product"
                },
                "stakeholders": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "role": {
                                "type": "string",
                                "description": "Stakeholder role"
                            },
                            "requirements": {
                                "type": "array",
                                "items": {
                                    "type": "string"
                                },
                                "description": "Specific requirements from this stakeholder"
                            }
                        }
                    },
                    "description": "Key stakeholders and their requirements"
                },
                "features": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "id": {
                                "type": "string",
                                "description": "Unique feature identifier"
                            },
                            "name": {
                                "type": "string",
                                "description": "Feature name"
                            },
                            "description": {
                                "type": "string",
                                "description": "Detailed feature description"
                            },
                            "priority": {
                                "type": "string",
                                "enum": ["low", "medium", "high", "critical"],
                                "description": "Feature priority"
                            },
                            "acceptance_criteria": {
                                "type": "array",
                                "items": {
                                    "type": "string"
                                },
                                "description": "Criteria for accepting the feature as complete"
                            }
                        },
                        "required": ["id", "name", "description", "priority"]
                    },
                    "description": "List of product features"
                },
                "timeline": {
                    "type": "object",
                    "properties": {
                        "start_date": {
                            "type": "string",
                            "format": "date",
                            "description": "Project start date"
                        },
                        "milestones": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "name": {
                                        "type": "string",
                                        "description": "Milestone name"
                                    },
                                    "date": {
                                        "type": "string",
                                        "format": "date",
                                        "description": "Milestone target date"
                                    },
                                    "deliverables": {
                                        "type": "array",
                                        "items": {
                                            "type": "string"
                                        },
                                        "description": "Expected deliverables for this milestone"
                                    }
                                }
                            },
                            "description": "Project milestones"
                        },
                        "release_date": {
                            "type": "string",
                            "format": "date",
                            "description": "Target release date"
                        }
                    },
                    "description": "Project timeline information"
                }
            },
            "required": ["product_name", "objective", "features"]
        },
        "keywords": ["product", "requirements", "specifications", "features", "development"],
        "tools": ["product_manager", "requirements_analyzer"],
        "example_data": {
            "product_name": "SmartAssist AI",
            "version": "1.0",
            "objective": "Create an AI assistant that helps users manage their daily tasks and increase productivity",
            "stakeholders": [
                {
                    "role": "End Users",
                    "requirements": [
                        "Simple, intuitive interface",
                        "Fast response times (<2 seconds)",
                        "Privacy-focused design"
                    ]
                },
                {
                    "role": "Business Team",
                    "requirements": [
                        "Analytics dashboard",
                        "Premium feature upsell opportunities",
                        "Enterprise integration options"
                    ]
                }
            ],
            "features": [
                {
                    "id": "F001",
                    "name": "Task Management",
                    "description": "Allow users to create, edit, and organize tasks with due dates and priorities",
                    "priority": "critical",
                    "acceptance_criteria": [
                        "Users can create tasks with title, description, and due date",
                        "Tasks can be marked as complete",
                        "Tasks can be categorized and filtered"
                    ]
                },
                {
                    "id": "F002",
                    "name": "AI Suggestions",
                    "description": "Provide intelligent suggestions for task prioritization and scheduling",
                    "priority": "high",
                    "acceptance_criteria": [
                        "System analyzes user patterns",
                        "Suggestions are contextually relevant",
                        "Users can accept or reject suggestions"
                    ]
                }
            ],
            "timeline": {
                "start_date": "2023-09-01",
                "milestones": [
                    {
                        "name": "Design Complete",
                        "date": "2023-10-15",
                        "deliverables": [
                            "UI/UX mockups",
                            "Technical architecture document"
                        ]
                    },
                    {
                        "name": "Beta Launch",
                        "date": "2023-12-01",
                        "deliverables": [
                            "Functional beta with core features",
                            "Initial user testing feedback"
                        ]
                    }
                ],
                "release_date": "2024-02-01"
            }
        }
    },
    {
        "name": "WebScrapingConfig",
        "description": "Configuration schema for web scraping tasks",
        "schema_type": "tool_config",
        "schema_definition": {
            "type": "object",
            "properties": {
                "target_url": {
                    "type": "string",
                    "description": "The URL to scrape"
                },
                "elements_to_extract": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "name": {
                                "type": "string",
                                "description": "Name for the extracted data"
                            },
                            "selector": {
                                "type": "string",
                                "description": "CSS or XPath selector for the element"
                            },
                            "attribute": {
                                "type": "string",
                                "description": "Attribute to extract (default is text content)"
                            },
                            "multiple": {
                                "type": "boolean",
                                "description": "Whether to extract multiple elements"
                            }
                        },
                        "required": ["name", "selector"]
                    },
                    "description": "Elements to extract from the page"
                },
                "pagination": {
                    "type": "object",
                    "properties": {
                        "enabled": {
                            "type": "boolean",
                            "description": "Whether to follow pagination"
                        },
                        "next_page_selector": {
                            "type": "string",
                            "description": "Selector for the next page button/link"
                        },
                        "max_pages": {
                            "type": "integer",
                            "description": "Maximum number of pages to scrape"
                        }
                    },
                    "description": "Pagination configuration"
                },
                "headers": {
                    "type": "object",
                    "additionalProperties": {
                        "type": "string"
                    },
                    "description": "HTTP headers to use in requests"
                },
                "delay": {
                    "type": "number",
                    "description": "Delay between requests in seconds"
                }
            },
            "required": ["target_url", "elements_to_extract"]
        },
        "keywords": ["web scraping", "data extraction", "crawler", "web data"],
        "tools": ["web_scraper", "data_collector"],
        "example_data": {
            "target_url": "https://example.com/products",
            "elements_to_extract": [
                {
                    "name": "product_titles",
                    "selector": ".product-title h2",
                    "attribute": "text",
                    "multiple": True
                },
                {
                    "name": "product_prices",
                    "selector": ".product-price .amount",
                    "attribute": "text",
                    "multiple": True
                },
                {
                    "name": "product_images",
                    "selector": ".product-image img",
                    "attribute": "src",
                    "multiple": True
                }
            ],
            "pagination": {
                "enabled": True,
                "next_page_selector": ".pagination .next",
                "max_pages": 5
            },
            "headers": {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            },
            "delay": 2.5
        }
    },
    # Add Pydantic Output Models for CrewAI tools
    {
        "name": "BaseToolOutput",
        "description": "Base class for all tool outputs",
        "schema_type": "output_model",
        "schema_definition": {
            "title": "BaseToolOutput",
            "type": "object",
            "description": "Base class for all tool outputs",
            "properties": {}
        },
        "keywords": ["tool", "output", "base"],
        "tools": ["all"]
    },
    {
        "name": "EmailRecipient",
        "description": "Model for email recipient",
        "schema_type": "output_model",
        "schema_definition": {
            "title": "EmailRecipient",
            "type": "object",
            "description": "Model for email recipient",
            "properties": {
                "name": {"type": "string"},
                "email": {"type": "string"}
            },
            "required": ["name", "email"]
        },
        "keywords": ["email", "recipient"],
        "tools": ["email"]
    },
    {
        "name": "EmailSender",
        "description": "Model for email sender",
        "schema_type": "output_model",
        "schema_definition": {
            "title": "EmailSender",
            "type": "object",
            "description": "Model for email sender",
            "properties": {
                "name": {"type": "string"},
                "email": {"type": "string"}
            },
            "required": ["name", "email"]
        },
        "keywords": ["email", "sender"],
        "tools": ["email"]
    },
    {
        "name": "EmailContent",
        "description": "Model for email content",
        "schema_type": "output_model",
        "schema_definition": {
            "title": "EmailContent",
            "type": "object",
            "description": "Model for email content",
            "properties": {
                "subject": {"type": "string"},
                "html": {"type": "string"},
                "text": {"type": "string", "nullable": True},
                "from_": {"$ref": "#/definitions/EmailSender"},
                "to": {
                    "type": "array",
                    "items": {"$ref": "#/definitions/EmailRecipient"}
                }
            },
            "required": ["subject", "html", "from_", "to"]
        },
        "keywords": ["email", "content"],
        "tools": ["email"]
    },
    {
        "name": "SerperResult",
        "description": "Model for individual Serper search result",
        "schema_type": "output_model",
        "schema_definition": {
            "title": "SerperResult",
            "type": "object",
            "description": "Model for individual Serper search result",
            "properties": {
                "Title": {"type": "string"},
                "Link": {"type": "string"},
                "Snippet": {"type": "string"}
            },
            "required": ["Title", "Link", "Snippet"]
        },
        "keywords": ["serper", "search", "result"],
        "tools": ["serper"]
    },
    {
        "name": "SerperDevToolOutput",
        "description": "Output model for SerperDev search tool",
        "schema_type": "output_model",
        "schema_definition": {
            "title": "SerperDevToolOutput",
            "type": "object",
            "description": "Output model for SerperDev search tool",
            "properties": {
                "results": {
                    "type": "array",
                    "items": {"$ref": "#/definitions/SerperResult"}
                }
            },
            "required": ["results"]
        },
        "keywords": ["serper", "search", "tool", "output"],
        "tools": ["serper", "search"]
    },
    {
        "name": "URLData",
        "description": "Model for URL data with associated metadata",
        "schema_type": "output_model",
        "schema_definition": {
            "title": "URLData",
            "type": "object",
            "description": "Model for URL data with associated metadata",
            "properties": {
                "Title": {"type": "string"},
                "Link": {"type": "string"},
                "Snippet": {"type": "string"}
            },
            "required": ["Title", "Link", "Snippet"]
        },
        "keywords": ["url", "data"],
        "tools": ["url", "web"]
    },
    {
        "name": "MultiURLToolOutput",
        "description": "Output model for handling multiple URLs with their metadata",
        "schema_type": "output_model",
        "schema_definition": {
            "title": "MultiURLToolOutput",
            "type": "object",
            "description": "Output model for handling multiple URLs with their metadata",
            "properties": {
                "results": {
                    "type": "array",
                    "items": {
                        "$ref": "#/definitions/URLData"
                    }
                },
                "total_count": {
                    "type": "integer",
                    "nullable": True
                },
                "source": {
                    "type": "string",
                    "nullable": True
                },
                "timestamp": {
                    "type": "string",
                    "nullable": True
                },
                "metadata": {
                    "type": "object",
                    "nullable": True,
                    "additionalProperties": True
                }
            },
            "required": [
                "results"
            ],
            "definitions": {
                "URLData": {
                    "type": "object",
                    "description": "Model for URL data with associated metadata",
                    "properties": {
                        "Title": {
                            "type": "string"
                        },
                        "Link": {
                            "type": "string",
                            "description": "URL must not be empty"
                        },
                        "Snippet": {
                            "type": "string"
                        }
                    },
                    "required": [
                        "Title",
                        "Link",
                        "Snippet"
                    ]
                }
            }
        },
        "keywords": ["url", "web", "search", "results", "multiple"],
        "tools": ["web_search"]
    },
    {
        "name": "WebBrowserToolOutput",
        "description": "Output model for Web Browser tool",
        "schema_type": "output_model",
        "schema_definition": {
            "title": "WebBrowserToolOutput",
            "type": "object",
            "description": "Output model for Web Browser tool",
            "properties": {
                "url": {"type": "string"},
                "content": {"type": "string"},
                "title": {"type": "string", "nullable": True}
            },
            "required": ["url", "content"]
        },
        "keywords": ["web", "browser", "tool", "output"],
        "tools": ["browser", "web"]
    },
    {
        "name": "FileToolOutput",
        "description": "Output model for File tool",
        "schema_type": "output_model",
        "schema_definition": {
            "title": "FileToolOutput",
            "type": "object",
            "description": "Output model for File tool",
            "properties": {
                "path": {"type": "string"},
                "content": {"type": "string"},
                "metadata": {"type": "object", "nullable": True}
            },
            "required": ["path", "content"]
        },
        "keywords": ["file", "tool", "output"],
        "tools": ["file"]
    },
    {
        "name": "DatabaseToolOutput",
        "description": "Output model for Database tool",
        "schema_type": "output_model",
        "schema_definition": {
            "title": "DatabaseToolOutput",
            "type": "object",
            "description": "Output model for Database tool",
            "properties": {
                "query_result": {"type": "object"},
                "affected_rows": {"type": "integer", "nullable": True},
                "metadata": {"type": "object", "nullable": True}
            },
            "required": ["query_result"]
        },
        "keywords": ["database", "sql", "tool", "output"],
        "tools": ["database", "sql"]
    },
    {
        "name": "APIToolOutput",
        "description": "Output model for API tool",
        "schema_type": "output_model",
        "schema_definition": {
            "title": "APIToolOutput",
            "type": "object",
            "description": "Output model for API tool",
            "properties": {
                "response": {"type": "object"},
                "status_code": {"type": "integer", "nullable": True},
                "headers": {"type": "object", "nullable": True}
            },
            "required": ["response"]
        },
        "keywords": ["api", "http", "tool", "output"],
        "tools": ["api", "http"]
    },
    {
        "name": "CustomToolOutput",
        "description": "Output model for Custom tool",
        "schema_type": "output_model",
        "schema_definition": {
            "title": "CustomToolOutput",
            "type": "object",
            "description": "Output model for Custom tool",
            "properties": {
                "result": {"type": "object"},
                "metadata": {"type": "object", "nullable": True}
            },
            "required": ["result"]
        },
        "keywords": ["custom", "tool", "output"],
        "tools": ["custom"]
    },
    {
        "name": "GoogleSlidesToolOutput",
        "description": "Output model for Google Slides tool",
        "schema_type": "output_model",
        "schema_definition": {
            "title": "GoogleSlidesToolOutput",
            "type": "object",
            "description": "Output model for Google Slides tool",
            "properties": {
                "slide_id": {"type": "string"},
                "content": {"type": "string"},
                "presentation_id": {"type": "string", "nullable": True}
            },
            "required": ["slide_id", "content"]
        },
        "keywords": ["google", "slides", "tool", "output"],
        "tools": ["google", "slides"]
    },
    {
        "name": "ArxivPaper",
        "description": "Schema for an arXiv paper",
        "schema_type": "output_model",
        "schema_definition": {
            "title": "ArxivPaper",
            "type": "object",
            "description": "Schema for an arXiv paper",
            "properties": {
                "id": {"type": "string"},
                "title": {"type": "string"},
                "abstract": {"type": "string"},
                "authors": {"type": "array", "items": {"type": "string"}},
                "categories": {"type": "array", "items": {"type": "string"}},
                "published_date": {"type": "string"},
                "pdf_url": {"type": "string", "nullable": True}
            },
            "required": ["id", "title", "abstract", "authors", "categories", "published_date"]
        },
        "keywords": ["arxiv", "research", "paper"],
        "tools": ["arxiv"]
    },
    {
        "name": "ArxivSearchResult",
        "description": "Schema for arXiv search results",
        "schema_type": "output_model",
        "schema_definition": {
            "title": "ArxivSearchResult",
            "type": "object",
            "description": "Schema for arXiv search results",
            "properties": {
                "papers": {"type": "array", "items": {"$ref": "#/definitions/ArxivPaper"}},
                "total_results": {"type": "integer"}
            },
            "required": ["papers"]
        },
        "keywords": ["arxiv", "search", "results"],
        "tools": ["arxiv"]
    },
    {
        "name": "GenieOutput",
        "description": "Schema for Databricks Genie Conversational API response",
        "schema_type": "output_model",
        "schema_definition": {
            "title": "GenieOutput",
            "type": "object",
            "description": "Schema for Databricks Genie Conversational API response",
            "properties": {
                "conversation": {
                    "type": "object",
                    "description": "Information about the conversation",
                    "properties": {
                        "created_timestamp": {"type": "integer"},
                        "id": {"type": "string"},
                        "last_updated_timestamp": {"type": "integer"},
                        "space_id": {"type": "string"},
                        "title": {"type": "string"},
                        "user_id": {"type": "integer"}
                    },
                    "required": ["created_timestamp", "id", "last_updated_timestamp", "space_id", "title", "user_id"]
                },
                "message": {
                    "type": "object",
                    "description": "Information about the message",
                    "properties": {
                        "attachments": {
                            "type": "array", 
                            "items": {
                                "type": "object",
                                "properties": {
                                    "text": {"type": "string"},
                                    "query": {"type": "string"},
                                    "attachment_id": {"type": "string"}
                                }
                            },
                            "nullable": True
                        },
                        "content": {"type": "string"},
                        "conversation_id": {"type": "string"},
                        "created_timestamp": {"type": "integer"},
                        "error": {"type": "object", "nullable": True},
                        "id": {"type": "string"},
                        "last_updated_timestamp": {"type": "integer"},
                        "query_result": {"type": "object", "nullable": True},
                        "space_id": {"type": "string"},
                        "status": {"type": "string", "enum": ["IN_PROGRESS", "COMPLETED", "FAILED", "CANCELLED"]},
                        "user_id": {"type": "integer"}
                    },
                    "required": ["content", "conversation_id", "created_timestamp", "id", "last_updated_timestamp", "space_id", "status", "user_id"]
                }
            },
            "required": ["conversation", "message"]
        },
        "keywords": ["genie", "databricks", "conversation", "api", "response"],
        "tools": ["genie", "databricks"],
        "example_data": {
            "conversation": {
                "created_timestamp": 1719769718,
                "id": "6a64adad2e664ee58de08488f986af3e",
                "last_updated_timestamp": 1719769718,
                "space_id": "3c409c00b54a44c79f79da06b82460e2",
                "title": "Give me top sales for last month",
                "user_id": 12345
            },
            "message": {
                "attachments": None,
                "content": "Give me top sales for last month",
                "conversation_id": "6a64adad2e664ee58de08488f986af3e",
                "created_timestamp": 1719769718,
                "error": None,
                "id": "e1ef34712a29169db030324fd0e1df5f",
                "last_updated_timestamp": 1719769718,
                "query_result": None,
                "space_id": "3c409c00b54a44c79f79da06b82460e2",
                "status": "IN_PROGRESS",
                "user_id": 12345
            }
        }
    },
    {
        "name": "PythonPPTX",
        "description": "Schema for Python PPTX presentation content structure",
        "schema_type": "data_model",
        "schema_definition": {
            "title": "PythonPPTXContent",
            "type": "object",
            "description": "Content structure for creating PowerPoint presentations",
            "properties": {
                "title": {"type": "string", "description": "The title of the presentation"},
                "author": {"type": "string", "description": "The author of the presentation"},
                "description": {"type": "string", "description": "Overview description of the presentation content"},
                "headline": {
                    "type": "object",
                    "properties": {
                        "title": {"type": "string", "description": "The headline title"},
                        "subtitle": {"type": "string", "description": "The headline subtitle"},
                        "date": {"type": "string", "format": "date-time", "description": "The headline date"}
                    },
                    "description": "Headline information for the presentation"
                },
                "company": {"type": "string", "description": "The company or organization associated with the presentation"},
                "keywords": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Keywords related to the presentation content"
                },
                "slides": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "title": {"type": "string", "description": "The title of the slide"},
                            "content": {"type": "string", "description": "Simple text content for the slide"},
                            "bullet_points": {
                                "type": "array",
                                "items": {
                                    "oneOf": [
                                        {"type": "string"},
                                        {
                                            "type": "object",
                                            "properties": {
                                                "text": {"type": "string", "description": "The text of the bullet point"},
                                                "bold": {"type": "boolean", "description": "Whether the text should be bold"},
                                                "italic": {"type": "boolean", "description": "Whether the text should be italic"},
                                                "level": {"type": "integer", "description": "The indentation level of the bullet point"}
                                            },
                                            "required": ["text"]
                                        }
                                    ]
                                },
                                "description": "Bullet points for the slide"
                            },
                            "notes": {"type": "string", "description": "Speaker notes for the slide"},
                            "chart_data": {
                                "type": "object",
                                "properties": {
                                    "title": {"type": "string", "description": "The title of the chart"},
                                    "chart_type": {"type": "string", "enum": ["BAR", "LINE", "PIE"], "description": "The type of chart"},
                                    "categories": {
                                        "type": "array",
                                        "items": {"type": "string"},
                                        "description": "Categories for the chart"
                                    },
                                    "series": {
                                        "type": "object",
                                        "additionalProperties": {
                                            "type": "array",
                                            "items": {"type": "number"}
                                        },
                                        "description": "Data series for the chart"
                                    }
                                },
                                "description": "Chart data for the slide"
                            },
                            "table_data": {
                                "type": "object",
                                "properties": {
                                    "headers": {
                                        "type": "array",
                                        "items": {"type": "string"},
                                        "description": "Header row for the table"
                                    },
                                    "rows": {
                                        "type": "array",
                                        "items": {
                                            "type": "array",
                                            "items": {"type": "string"}
                                        },
                                        "description": "Data rows for the table"
                                    }
                                },
                                "description": "Table data for the slide"
                            }
                        },
                        "required": ["title"]
                    },
                    "description": "Array of slides in the presentation"
                },
                "include_footer": {"type": "boolean", "description": "Whether to include a footer on all slides"},
                "revision": {"type": "integer", "description": "The revision number of the presentation"}
            },
            "required": ["title", "slides"]
        },
        "keywords": ["presentation", "powerpoint", "pptx", "slides", "content"],
        "tools": ["python_pptx"]
    }
]

async def seed_async():
    """Seed schemas into the database using async session."""
    logger.info("Seeding schemas table (async)...")
    
    # Get existing schema names to avoid duplicates (outside the loop to reduce DB queries)
    async with async_session_factory() as session:
        result = await session.execute(select(Schema.name))
        existing_names = {row[0] for row in result.scalars().all()}
    
    # Insert new schemas
    schemas_added = 0
    schemas_updated = 0
    schemas_skipped = 0
    schemas_error = 0
    
    # Process each schema individually with its own session to avoid transaction problems
    for schema_data in SAMPLE_SCHEMAS:
        try:
            # Create a fresh session for each schema to avoid transaction conflicts
            async with async_session_factory() as session:
                if schema_data["name"] not in existing_names:
                    # Check again to be extra sure - this helps with race conditions
                    check_result = await session.execute(
                        select(Schema).filter(Schema.name == schema_data["name"])
                    )
                    existing_schema = check_result.scalars().first()
                    
                    if existing_schema:
                        # If it exists now (race condition), update it instead
                        existing_schema.description = schema_data["description"]
                        existing_schema.schema_type = schema_data["schema_type"]
                        existing_schema.schema_definition = schema_data["schema_definition"]
                        existing_schema.field_descriptions = schema_data.get("field_descriptions", {})
                        existing_schema.keywords = schema_data.get("keywords", [])
                        existing_schema.tools = schema_data.get("tools", [])
                        existing_schema.example_data = schema_data.get("example_data", {})
                        existing_schema.updated_at = datetime.now().replace(tzinfo=None)
                        logger.debug(f"Updating existing schema: {schema_data['name']}")
                        schemas_updated += 1
                    else:
                        # Add new schema
                        schema = Schema(
                            name=schema_data["name"],
                            description=schema_data["description"],
                            schema_type=schema_data["schema_type"],
                            schema_definition=schema_data["schema_definition"],
                            field_descriptions=schema_data.get("field_descriptions", {}),
                            keywords=schema_data.get("keywords", []),
                            tools=schema_data.get("tools", []),
                            example_data=schema_data.get("example_data", {}),
                            created_at=datetime.now().replace(tzinfo=None),
                            updated_at=datetime.now().replace(tzinfo=None)
                        )
                        session.add(schema)
                        logger.debug(f"Adding new schema: {schema_data['name']}")
                        schemas_added += 1
                else:
                    # Update existing schema
                    result = await session.execute(
                        select(Schema).filter(Schema.name == schema_data["name"])
                    )
                    existing_schema = result.scalars().first()
                    
                    if existing_schema:
                        # Update fields
                        existing_schema.description = schema_data["description"]
                        existing_schema.schema_type = schema_data["schema_type"]
                        existing_schema.schema_definition = schema_data["schema_definition"]
                        existing_schema.field_descriptions = schema_data.get("field_descriptions", {})
                        existing_schema.keywords = schema_data.get("keywords", [])
                        existing_schema.tools = schema_data.get("tools", [])
                        existing_schema.example_data = schema_data.get("example_data", {})
                        existing_schema.updated_at = datetime.now().replace(tzinfo=None)
                        logger.debug(f"Updating existing schema: {schema_data['name']}")
                        schemas_updated += 1
                
                # Commit the session for this schema
                try:
                    await session.commit()
                except Exception as e:
                    await session.rollback()
                    if "UNIQUE constraint failed" in str(e):
                        logger.warning(f"Schema {schema_data['name']} already exists, skipping insert")
                        schemas_skipped += 1
                    else:
                        logger.error(f"Failed to commit schema {schema_data['name']}: {str(e)}")
                        schemas_error += 1
        except Exception as e:
            logger.error(f"Error processing schema {schema_data['name']}: {str(e)}")
            schemas_error += 1
    
    logger.info(f"Schemas seeding summary: Added {schemas_added}, Updated {schemas_updated}, Skipped {schemas_skipped}, Errors {schemas_error}")

def seed_sync():
    """Seed schemas into the database using sync session."""
    logger.info("Seeding schemas table (sync)...")
    
    # Get existing schema names to avoid duplicates (outside the loop to reduce DB queries)
    with SessionLocal() as session:
        result = session.execute(select(Schema.name))
        existing_names = {row[0] for row in result.scalars().all()}
    
    # Insert new schemas
    schemas_added = 0
    schemas_updated = 0
    schemas_skipped = 0
    schemas_error = 0
    
    # Process each schema individually with its own session to avoid transaction problems
    for schema_data in SAMPLE_SCHEMAS:
        try:
            # Create a fresh session for each schema to avoid transaction conflicts
            with SessionLocal() as session:
                if schema_data["name"] not in existing_names:
                    # Check again to be extra sure - this helps with race conditions
                    check_result = session.execute(
                        select(Schema).filter(Schema.name == schema_data["name"])
                    )
                    existing_schema = check_result.scalars().first()
                    
                    if existing_schema:
                        # If it exists now (race condition), update it instead
                        existing_schema.description = schema_data["description"]
                        existing_schema.schema_type = schema_data["schema_type"]
                        existing_schema.schema_definition = schema_data["schema_definition"]
                        existing_schema.field_descriptions = schema_data.get("field_descriptions", {})
                        existing_schema.keywords = schema_data.get("keywords", [])
                        existing_schema.tools = schema_data.get("tools", [])
                        existing_schema.example_data = schema_data.get("example_data", {})
                        existing_schema.updated_at = datetime.now().replace(tzinfo=None)
                        logger.debug(f"Updating existing schema: {schema_data['name']}")
                        schemas_updated += 1
                    else:
                        # Add new schema
                        schema = Schema(
                            name=schema_data["name"],
                            description=schema_data["description"],
                            schema_type=schema_data["schema_type"],
                            schema_definition=schema_data["schema_definition"],
                            field_descriptions=schema_data.get("field_descriptions", {}),
                            keywords=schema_data.get("keywords", []),
                            tools=schema_data.get("tools", []),
                            example_data=schema_data.get("example_data", {}),
                            created_at=datetime.now().replace(tzinfo=None),
                            updated_at=datetime.now().replace(tzinfo=None)
                        )
                        session.add(schema)
                        logger.debug(f"Adding new schema: {schema_data['name']}")
                        schemas_added += 1
                else:
                    # Update existing schema
                    result = session.execute(
                        select(Schema).filter(Schema.name == schema_data["name"])
                    )
                    existing_schema = result.scalars().first()
                    
                    if existing_schema:
                        # Update fields
                        existing_schema.description = schema_data["description"]
                        existing_schema.schema_type = schema_data["schema_type"]
                        existing_schema.schema_definition = schema_data["schema_definition"]
                        existing_schema.field_descriptions = schema_data.get("field_descriptions", {})
                        existing_schema.keywords = schema_data.get("keywords", [])
                        existing_schema.tools = schema_data.get("tools", [])
                        existing_schema.example_data = schema_data.get("example_data", {})
                        existing_schema.updated_at = datetime.now().replace(tzinfo=None)
                        logger.debug(f"Updating existing schema: {schema_data['name']}")
                        schemas_updated += 1
                
                # Commit the session for this schema
                try:
                    session.commit()
                except Exception as e:
                    session.rollback()
                    if "UNIQUE constraint failed" in str(e):
                        logger.warning(f"Schema {schema_data['name']} already exists, skipping insert")
                        schemas_skipped += 1
                    else:
                        logger.error(f"Failed to commit schema {schema_data['name']}: {str(e)}")
                        schemas_error += 1
        except Exception as e:
            logger.error(f"Error processing schema {schema_data['name']}: {str(e)}")
            schemas_error += 1
    
    logger.info(f"Schemas seeding summary: Added {schemas_added}, Updated {schemas_updated}, Skipped {schemas_skipped}, Errors {schemas_error}")

# Main entry point for seeding - can be called directly or by seed_runner
async def seed():
    """Main entry point for seeding schemas."""
    logger.info("Starting schema seeding process...")
    try:
        await seed_async()
        logger.info("Schema seeding completed successfully")
    except Exception as e:
        logger.error(f"Error seeding schemas: {str(e)}")
        import traceback
        logger.error(f"Schema seeding traceback: {traceback.format_exc()}")
        # Don't re-raise - allow other seeds to run

# For backwards compatibility or direct command-line usage
if __name__ == "__main__":
    import asyncio
    asyncio.run(seed()) 
    asyncio.run(seed()) 