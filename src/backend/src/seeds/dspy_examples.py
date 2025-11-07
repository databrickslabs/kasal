"""
Seeder for DSPy training examples.

Hydrates initial curated/synthetic examples for:
- intent_detection
- agent_generation
- task_generation
- crew_generation

Follows the project seeding pattern: Router → Service → Repository → DB
Uses the repository directly with async_session_factory.
"""

import logging
from typing import List, Dict, Any

from sqlalchemy import select, func

from src.db.session import async_session_factory
from src.repositories.dspy_config_repository import DSPyConfigRepository
from src.schemas.dspy_schemas import OptimizationType, ExampleSourceType
from src.models.group import Group

logger = logging.getLogger(__name__)


def _intent_detection_examples() -> List[Dict[str, Any]]:
    return [
        {
            "input_data": {
                "message": "Create a task to summarize the quarterly revenue report",
                "semantic_hints": "generate_task, analytics"
            },
            "output_data": {
                "intent": "generate_task",
                "confidence": 0.92,
                "extracted_info": {"topic": "revenue", "period": "quarterly"},
                "reasoning": "User asks to create a task for summarization"
            },
            "quality_score": 0.9,
            "source_type": ExampleSourceType.SYNTHETIC.value,
        },
        {
            "input_data": {
                "message": "Build an agent that monitors GitHub issues and labels bugs",
                "semantic_hints": "generate_agent, github"
            },
            "output_data": {
                "intent": "generate_agent",
                "confidence": 0.91,
                "extracted_info": {"domain": "github", "action": "monitor"},
                "reasoning": "Clearly an agent to monitor GitHub issues"
            },
            "quality_score": 0.9,
            "source_type": ExampleSourceType.SYNTHETIC.value,
        },
        {
            "input_data": {
                "message": "Design a crew to analyze sales data and generate a dashboard",
                "semantic_hints": "generate_crew, analytics"
            },
            "output_data": {
                "intent": "generate_crew",
                "confidence": 0.9,
                "extracted_info": {"goal": "dashboard", "data": "sales"},
                "reasoning": "Team required to analyze and present sales"
            },
            "quality_score": 0.88,
            "source_type": ExampleSourceType.SYNTHETIC.value,
        },
        {
            "input_data": {
                "message": "Run the weekly metrics workflow now",
                "semantic_hints": "execute_crew"
            },
            "output_data": {
                "intent": "execute_crew",
                "confidence": 0.89,
                "extracted_info": {"schedule": "weekly"},
                "reasoning": "Execute an existing workflow"
            },
            "quality_score": 0.87,
            "source_type": ExampleSourceType.SYNTHETIC.value,
        },
        {
            "input_data": {
                "message": "Configure the reporting crew to use the Databricks endpoint",
                "semantic_hints": "configure_crew, databricks"
            },
            "output_data": {
                "intent": "configure_crew",
                "confidence": 0.88,
                "extracted_info": {"target": "Databricks"},
                "reasoning": "Configuration update for a crew"
            },
            "quality_score": 0.86,
            "source_type": ExampleSourceType.SYNTHETIC.value,
        },
        {
            "input_data": {
                "message": "I'm not sure what to do",
                "semantic_hints": "unknown"
            },
            "output_data": {
                "intent": "unknown",
                "confidence": 0.6,
                "extracted_info": {},
                "reasoning": "No clear intent present"
            },
            "quality_score": 0.8,
            "source_type": ExampleSourceType.SYNTHETIC.value,
        },
        {
            "input_data": {
                "message": "Generate an agent for web scraping product prices",
                "semantic_hints": "generate_agent, scraping"
            },
            "output_data": {
                "intent": "generate_agent",
                "confidence": 0.91,
                "extracted_info": {"use_case": "price scraping"},
                "reasoning": "Agent generation request"
            },
            "quality_score": 0.9,
            "source_type": ExampleSourceType.SYNTHETIC.value,
        },
        {
            "input_data": {
                "message": "Plan tasks for cleaning and deduplicating the dataset",
                "semantic_hints": "generate_task, data_cleaning"
            },
            "output_data": {
                "intent": "generate_task",
                "confidence": 0.9,
                "extracted_info": {"topic": "data cleaning"},
                "reasoning": "Create tasks for data prep"
            },
            "quality_score": 0.88,
            "source_type": ExampleSourceType.SYNTHETIC.value,
        },
        {
            "input_data": {
                "message": "Assemble a team to research competition and write a summary",
                "semantic_hints": "generate_crew, research"
            },
            "output_data": {
                "intent": "generate_crew",
                "confidence": 0.9,
                "extracted_info": {"goal": "competitive research"},
                "reasoning": "Crew generation to research and summarize"
            },
            "quality_score": 0.89,
            "source_type": ExampleSourceType.SYNTHETIC.value,
        },
        {
            "input_data": {
                "message": "Start the analytics workflow for this week",
                "semantic_hints": "execute_crew, analytics"
            },
            "output_data": {
                "intent": "execute_crew",
                "confidence": 0.9,
                "extracted_info": {"timeframe": "this week"},
                "reasoning": "Run the existing workflow"
            },
            "quality_score": 0.88,
            "source_type": ExampleSourceType.SYNTHETIC.value,
        },
    ]


def _agent_generation_examples() -> List[Dict[str, Any]]:
    return [
        {
            "input_data": {
                "prompt": "Create a research agent that reads PDFs and extracts key insights",
                "tools_available": ["PDFSearchTool", "PerplexityTool"]
            },
            "output_data": {
                "agent_name": "PDF Insight Extractor",
                "role": "Analyze PDFs and extract insights",
                "backstory": "Expert in document analysis",
                "goal": "Summarize reports",
                "tools": ["PDFSearchTool", "PerplexityTool"],
                "reasoning": "Tools match the task"
            },
            "quality_score": 0.9,
            "source_type": ExampleSourceType.SYNTHETIC.value,
        },
        {
            "input_data": {
                "prompt": "Build an agent to monitor GitHub PRs and tag reviewers",
                "tools_available": ["GithubSearchTool"]
            },
            "output_data": {
                "agent_name": "PR Watcher",
                "role": "Monitor pull requests",
                "backstory": "Automation specialist",
                "goal": "Improve code review workflows",
                "tools": ["GithubSearchTool"],
                "reasoning": "Needs GitHub access"
            },
            "quality_score": 0.88,
            "source_type": ExampleSourceType.SYNTHETIC.value,
        },
        {
            "input_data": {
                "prompt": "Agent that queries Databricks SQL and returns charts",
                "tools_available": ["DatabricksQueryTool"]
            },
            "output_data": {
                "agent_name": "Lakehouse Analyst",
                "role": "Query and visualize data",
                "backstory": "Data specialist",
                "goal": "Answer BI questions",
                "tools": ["DatabricksQueryTool"],
                "reasoning": "Direct warehouse access"
            },
            "quality_score": 0.9,
            "source_type": ExampleSourceType.SYNTHETIC.value,
        },
        {
            "input_data": {
                "prompt": "Web research agent that answers with sources",
                "tools_available": ["PerplexityTool"]
            },
            "output_data": {
                "agent_name": "Sourced Researcher",
                "role": "Research web and cite sources",
                "backstory": "Search expert",
                "goal": "Answer factual questions",
                "tools": ["PerplexityTool"],
                "reasoning": "Search + cite"
            },
            "quality_score": 0.87,
            "source_type": ExampleSourceType.SYNTHETIC.value,
        },
        {
            "input_data": {
                "prompt": "Code assistant that writes Python snippets",
                "tools_available": ["CodeInterpreterTool"]
            },
            "output_data": {
                "agent_name": "PyHelper",
                "role": "Write & run Python",
                "backstory": "Dev assistant",
                "goal": "Produce working code",
                "tools": ["CodeInterpreterTool"],
                "reasoning": "Executes code"
            },
            "quality_score": 0.88,
            "source_type": ExampleSourceType.SYNTHETIC.value,
        },
        {
            "input_data": {
                "prompt": "Marketing copywriter agent for product pages",
                "tools_available": []
            },
            "output_data": {
                "agent_name": "CopyCraft",
                "role": "Write marketing copy",
                "backstory": "Brand voice expert",
                "goal": "Convert visitors",
                "tools": [],
                "reasoning": "Language focused"
            },
            "quality_score": 0.86,
            "source_type": ExampleSourceType.SYNTHETIC.value,
        },
        {
            "input_data": {"prompt": "Agent to scrape sites and compile CSV", "tools_available": ["ScrapeWebsiteTool", "CSVSearchTool"]},
            "output_data": {"agent_name": "Site Scraper", "role": "Scrape & compile", "backstory": "Automation", "goal": "CSV output", "tools": ["ScrapeWebsiteTool", "CSVSearchTool"], "reasoning": "Scrape then structure"},
            "quality_score": 0.87,
            "source_type": ExampleSourceType.SYNTHETIC.value,
        },
        {
            "input_data": {"prompt": "Agent to search YouTube transcripts for topics", "tools_available": ["YoutubeVideoSearchTool"]},
            "output_data": {"agent_name": "Video Scout", "role": "Find clips", "backstory": "Media", "goal": "Topic snippets", "tools": ["YoutubeVideoSearchTool"], "reasoning": "Search videos"},
            "quality_score": 0.86,
            "source_type": ExampleSourceType.SYNTHETIC.value,
        },
        {
            "input_data": {"prompt": "Agent that queries Snowflake for finance KPIs", "tools_available": ["SnowflakeSearchTool"]},
            "output_data": {"agent_name": "Finance KPI Bot", "role": "BI answers", "backstory": "Analyst", "goal": "KPI queries", "tools": ["SnowflakeSearchTool"], "reasoning": "Warehouse access"},
            "quality_score": 0.88,
            "source_type": ExampleSourceType.SYNTHETIC.value,
        },
    ]


def _task_generation_examples() -> List[Dict[str, Any]]:
    return [
        {
            "input_data": {
                "prompt": "Create a task to extract tables from a PDF report",
                "agent_context": "PDF Insight Extractor"
            },
            "output_data": {
                "task_name": "Extract Tables",
                "description": "Find and extract tabular data from PDF",
                "expected_output": "CSV with tables",
                "tools": ["PDFSearchTool"],
                "reasoning": "Tabular extraction"
            },
            "quality_score": 0.9,
            "source_type": ExampleSourceType.SYNTHETIC.value,
        },
        {
            "input_data": {"prompt": "Write a task to compute weekly revenue KPI", "agent_context": "Lakehouse Analyst"},
            "output_data": {"task_name": "Compute Weekly Revenue", "description": "SQL against sales table", "expected_output": "Number & chart", "tools": ["DatabricksQueryTool"], "reasoning": "Warehouse query"},
            "quality_score": 0.89,
            "source_type": ExampleSourceType.SYNTHETIC.value,
        },
        {
            "input_data": {"prompt": "Task to search web for top 5 competitors", "agent_context": "Sourced Researcher"},
            "output_data": {"task_name": "Competitor Search", "description": "Find competitors and citations", "expected_output": "List with links", "tools": ["PerplexityTool"], "reasoning": "Web search"},
            "quality_score": 0.87,
            "source_type": ExampleSourceType.SYNTHETIC.value,
        },
        {
            "input_data": {"prompt": "Task to clean and deduplicate customer emails", "agent_context": "PyHelper"},
            "output_data": {"task_name": "Clean Emails", "description": "Normalize and dedupe emails", "expected_output": "Clean CSV", "tools": ["CodeInterpreterTool"], "reasoning": "Python transformations"},
            "quality_score": 0.88,
            "source_type": ExampleSourceType.SYNTHETIC.value,
        },
        {
            "input_data": {"prompt": "Create a task to scrape product prices", "agent_context": "Site Scraper"},
            "output_data": {"task_name": "Scrape Prices", "description": "Extract prices from pages", "expected_output": "CSV rows", "tools": ["ScrapeWebsiteTool"], "reasoning": "Scraping"},
            "quality_score": 0.86,
            "source_type": ExampleSourceType.SYNTHETIC.value,
        },
        {
            "input_data": {"prompt": "Create a task to crawl site map", "agent_context": "Site Scraper"},
            "output_data": {"task_name": "Crawl Sitemap", "description": "Traverse links and capture", "expected_output": "JSON dump", "tools": ["ScrapeWebsiteTool"], "reasoning": "Crawl"},
            "quality_score": 0.85,
            "source_type": ExampleSourceType.SYNTHETIC.value,
        },
        {
            "input_data": {"prompt": "Create a task to summarize video transcript", "agent_context": "Video Scout"},
            "output_data": {"task_name": "Summarize Transcript", "description": "Summarize key points", "expected_output": "Bulleted list", "tools": ["YoutubeVideoSearchTool"], "reasoning": "Transcript processing"},
            "quality_score": 0.86,
            "source_type": ExampleSourceType.SYNTHETIC.value,
        },
        {
            "input_data": {"prompt": "Create a task to compute churn rate", "agent_context": "Lakehouse Analyst"},
            "output_data": {"task_name": "Compute Churn", "description": "SQL on user events", "expected_output": "Churn metric", "tools": ["DatabricksQueryTool"], "reasoning": "BI metric"},
            "quality_score": 0.88,
            "source_type": ExampleSourceType.SYNTHETIC.value,
        },
        {
            "input_data": {"prompt": "Create a task to convert JSON to CSV", "agent_context": "PyHelper"},
            "output_data": {"task_name": "JSON to CSV", "description": "Transform records", "expected_output": "CSV file", "tools": ["CodeInterpreterTool"], "reasoning": "Data transform"},
            "quality_score": 0.87,
            "source_type": ExampleSourceType.SYNTHETIC.value,
        },
    ]


def _crew_generation_examples() -> List[Dict[str, Any]]:
    return [
        {
            "input_data": {
                "prompt": "Generate a crew to analyze quarterly sales and publish a dashboard",
                "tools_available": ["DatabricksQueryTool", "PerplexityTool"]
            },
            "output_data": {
                "crew_name": "Sales Intelligence Crew",
                "agents": [
                    {"name": "Lakehouse Analyst", "tools": ["DatabricksQueryTool"]},
                    {"name": "Sourced Researcher", "tools": ["PerplexityTool"]}
                ],
                "tasks": [
                    {"name": "Fetch Sales Data"},
                    {"name": "Generate Metrics"},
                    {"name": "Draft Dashboard"}
                ],
                "workflow": {"edges": [[0,1],[1,2]]},
                "reasoning": "Analysis then presentation"
            },
            "quality_score": 0.9,
            "source_type": ExampleSourceType.SYNTHETIC.value,
        },
        {
            "input_data": {"prompt": "Crew for scraping competitors and summarizing", "tools_available": ["ScrapeWebsiteTool", "PerplexityTool"]},
            "output_data": {"crew_name": "Competitive Research Crew", "agents": [{"name": "Site Scraper"}, {"name": "Sourced Researcher"}], "tasks": [{"name": "Scrape Sites"}, {"name": "Summarize Findings"}], "workflow": {"edges": [[0,1]]}, "reasoning": "Scrape then summarize"},
            "quality_score": 0.88,
            "source_type": ExampleSourceType.SYNTHETIC.value,
        },
        {
            "input_data": {"prompt": "Crew to compile weekly KPIs and email report", "tools_available": ["DatabricksQueryTool"]},
            "output_data": {"crew_name": "KPI Reporter", "agents": [{"name": "Lakehouse Analyst"}], "tasks": [{"name": "Compute KPIs"}, {"name": "Email Report"}], "workflow": {"edges": [[0,1]]}, "reasoning": "Compute then send"},
            "quality_score": 0.88,
            "source_type": ExampleSourceType.SYNTHETIC.value,
        },
        {
            "input_data": {"prompt": "Crew to process support tickets and tag issues", "tools_available": []},
            "output_data": {"crew_name": "Support Triage", "agents": [{"name": "PR Watcher"}], "tasks": [{"name": "Classify Tickets"}], "workflow": {"edges": []}, "reasoning": "Simple triage"},
            "quality_score": 0.86,
            "source_type": ExampleSourceType.SYNTHETIC.value,
        },
        {
            "input_data": {"prompt": "Crew to crawl a site and build a CSV", "tools_available": ["ScrapeWebsiteTool", "CSVSearchTool"]},
            "output_data": {"crew_name": "Site ETL", "agents": [{"name": "Site Scraper"}], "tasks": [{"name": "Crawl"}, {"name": "Assemble CSV"}], "workflow": {"edges": [[0,1]]}, "reasoning": "ETL flow"},
            "quality_score": 0.86,
            "source_type": ExampleSourceType.SYNTHETIC.value,
        },
        {
            "input_data": {"prompt": "Crew to analyze churn and recommend actions", "tools_available": ["DatabricksQueryTool"]},
            "output_data": {"crew_name": "Churn Analyst", "agents": [{"name": "Lakehouse Analyst"}], "tasks": [{"name": "Compute Churn"}, {"name": "Recommend Actions"}], "workflow": {"edges": [[0,1]]}, "reasoning": "Metric then recs"},
            "quality_score": 0.87,
            "source_type": ExampleSourceType.SYNTHETIC.value,
        },
        {
            "input_data": {"prompt": "Crew to summarize video series", "tools_available": ["YoutubeVideoSearchTool"]},
            "output_data": {"crew_name": "Video Summarizer", "agents": [{"name": "Video Scout"}], "tasks": [{"name": "Find Clips"}, {"name": "Summarize"}], "workflow": {"edges": [[0,1]]}, "reasoning": "Clip then summarize"},
            "quality_score": 0.86,
            "source_type": ExampleSourceType.SYNTHETIC.value,
        },
        {
            "input_data": {"prompt": "Crew to perform literature review and write notes", "tools_available": ["PerplexityTool"]},
            "output_data": {"crew_name": "Literature Review", "agents": [{"name": "Sourced Researcher"}], "tasks": [{"name": "Collect Sources"}, {"name": "Write Notes"}], "workflow": {"edges": [[0,1]]}, "reasoning": "Collect then note"},
            "quality_score": 0.87,
            "source_type": ExampleSourceType.SYNTHETIC.value,
        },
        {
            "input_data": {"prompt": "Crew to track open PRs and alert reviewers", "tools_available": ["GithubSearchTool"]},
            "output_data": {"crew_name": "Review Radar", "agents": [{"name": "PR Watcher"}], "tasks": [{"name": "List PRs"}, {"name": "Notify Reviewers"}], "workflow": {"edges": [[0,1]]}, "reasoning": "Monitor then alert"},
            "quality_score": 0.86,
            "source_type": ExampleSourceType.SYNTHETIC.value,
        },
    ]


async def seed() -> None:
    """Seed DSPy training examples for all four optimization types.
    - Top up per-group to ensure at least 10 examples per type per group
    - Does not overwrite existing examples; only inserts missing count
    """
    logger.info("Seeding DSPy training examples...")

    TARGET_PER_TYPE = 10

    async with async_session_factory() as session:
        repo = DSPyConfigRepository(session)

        # Fetch all group IDs to seed per-tenant examples
        result = await session.execute(select(Group.id))
        group_ids = list(result.scalars().all())
        if not group_ids:
            logger.info("No groups found; seeding global (group_id=None) only")
            group_ids = [None]

        async def _top_up_for_group(typ: OptimizationType, builder, gid: str | None) -> int:
            # Count existing examples for this group/type
            from src.models.dspy_config import DSPyTrainingExample  # local import to avoid circulars
            q = select(func.count()).select_from(DSPyTrainingExample).where(
                DSPyTrainingExample.optimization_type == typ.value
            )
            if gid:
                q = q.where(DSPyTrainingExample.group_id == gid)
            else:
                q = q.where(DSPyTrainingExample.group_id.is_(None))

            count = (await session.execute(q)).scalar_one()
            if count >= TARGET_PER_TYPE:
                logger.info(f"{typ.value} ({gid or 'global'}): already has {count} examples; skipping")
                return 0

            needed = TARGET_PER_TYPE - count
            examples = builder()[:needed]
            created = await repo.create_training_examples(
                examples=examples,
                optimization_type=typ,
                group_id=gid,
            )
            logger.info(f"{typ.value} ({gid or 'global'}): topped up {len(created)} (had {count}, target {TARGET_PER_TYPE})")
            return len(created)

        total = 0
        for gid in group_ids:
            total += await _top_up_for_group(OptimizationType.INTENT_DETECTION, _intent_detection_examples, gid)
            total += await _top_up_for_group(OptimizationType.AGENT_GENERATION, _agent_generation_examples, gid)
            total += await _top_up_for_group(OptimizationType.TASK_GENERATION, _task_generation_examples, gid)
            total += await _top_up_for_group(OptimizationType.CREW_GENERATION, _crew_generation_examples, gid)

    logger.info(f"DSPy examples seeding complete. Total inserted: {total}")

