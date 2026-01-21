"""
Seed the schemas table with the top 10 most useful CrewAI output schemas.

Based on common production use cases: content creation, research, support,
email automation, report generation, and decision-making workflows.
"""
import logging
from datetime import datetime
from sqlalchemy import select

from src.db.session import async_session_factory
from src.models.schema import Schema

logger = logging.getLogger(__name__)

# Top 10 business-friendly schemas for CrewAI tasks
SAMPLE_SCHEMAS = [
    {
        "name": "Article",
        "description": "Content creation - blogs, articles, social posts, documentation",
        "schema_type": "schema",
        "schema_definition": {
            "type": "object",
            "properties": {
                "title": {"type": "string"},
                "content": {"type": "string"},
                "summary": {"type": "string"},
                "tags": {"type": "array", "items": {"type": "string"}}
            },
            "required": ["title", "content"]
        }
    },
    {
        "name": "Summary",
        "description": "Summarization - distill content into key points and takeaways",
        "schema_type": "schema",
        "schema_definition": {
            "type": "object",
            "properties": {
                "title": {"type": "string"},
                "key_points": {"type": "array", "items": {"type": "string"}},
                "conclusion": {"type": "string"}
            },
            "required": ["key_points"]
        }
    },
    {
        "name": "Analysis",
        "description": "Research & analysis - findings, insights, and next steps",
        "schema_type": "schema",
        "schema_definition": {
            "type": "object",
            "properties": {
                "subject": {"type": "string"},
                "findings": {"type": "array", "items": {"type": "string"}},
                "insights": {"type": "array", "items": {"type": "string"}},
                "next_steps": {"type": "array", "items": {"type": "string"}}
            },
            "required": ["findings"]
        }
    },
    {
        "name": "SearchResults",
        "description": "Research output - search findings with sources and references",
        "schema_type": "schema",
        "schema_definition": {
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "results": {"type": "array", "items": {"type": "string"}},
                "sources": {"type": "array", "items": {"type": "string"}}
            },
            "required": ["results"]
        }
    },
    {
        "name": "Recommendation",
        "description": "Decision-making - recommendations with reasoning and confidence",
        "schema_type": "schema",
        "schema_definition": {
            "type": "object",
            "properties": {
                "recommendation": {"type": "string"},
                "reasoning": {"type": "string"},
                "alternatives": {"type": "array", "items": {"type": "string"}},
                "confidence": {"type": "string"}
            },
            "required": ["recommendation", "reasoning"]
        }
    },
    {
        "name": "ActionItems",
        "description": "Planning - tasks, to-dos, action items with priority",
        "schema_type": "schema",
        "schema_definition": {
            "type": "object",
            "properties": {
                "items": {"type": "array", "items": {"type": "string"}},
                "priority": {"type": "string"},
                "owner": {"type": "string"},
                "deadline": {"type": "string"}
            },
            "required": ["items"]
        }
    },
    {
        "name": "Email",
        "description": "Email automation - subject, body, and tone for outreach",
        "schema_type": "schema",
        "schema_definition": {
            "type": "object",
            "properties": {
                "subject": {"type": "string"},
                "body": {"type": "string"},
                "tone": {"type": "string"},
                "call_to_action": {"type": "string"}
            },
            "required": ["subject", "body"]
        }
    },
    {
        "name": "Report",
        "description": "Comprehensive report - sections, conclusions, recommendations",
        "schema_type": "schema",
        "schema_definition": {
            "type": "object",
            "properties": {
                "title": {"type": "string"},
                "executive_summary": {"type": "string"},
                "sections": {"type": "array", "items": {"type": "string"}},
                "conclusion": {"type": "string"},
                "recommendations": {"type": "array", "items": {"type": "string"}}
            },
            "required": ["title", "sections"]
        }
    },
    {
        "name": "QA",
        "description": "Question-answer - for support, FAQs, and knowledge queries",
        "schema_type": "schema",
        "schema_definition": {
            "type": "object",
            "properties": {
                "question": {"type": "string"},
                "answer": {"type": "string"},
                "sources": {"type": "array", "items": {"type": "string"}},
                "confidence": {"type": "string"}
            },
            "required": ["question", "answer"]
        }
    },
    {
        "name": "Evaluation",
        "description": "Scoring & review - assessments, lead scoring, ratings",
        "schema_type": "schema",
        "schema_definition": {
            "type": "object",
            "properties": {
                "subject": {"type": "string"},
                "score": {"type": "integer"},
                "criteria": {"type": "array", "items": {"type": "string"}},
                "pros": {"type": "array", "items": {"type": "string"}},
                "cons": {"type": "array", "items": {"type": "string"}},
                "verdict": {"type": "string"}
            },
            "required": ["subject", "verdict"]
        }
    }
]


async def seed_async():
    """Seed schemas into the database."""
    logger.info("Seeding schemas...")

    schemas_added = 0
    schemas_updated = 0

    for schema_data in SAMPLE_SCHEMAS:
        try:
            async with async_session_factory() as session:
                result = await session.execute(
                    select(Schema).filter(Schema.name == schema_data["name"])
                )
                existing = result.scalars().first()

                if existing:
                    existing.description = schema_data["description"]
                    existing.schema_type = schema_data["schema_type"]
                    existing.schema_definition = schema_data["schema_definition"]
                    existing.updated_at = datetime.now().replace(tzinfo=None)
                    schemas_updated += 1
                else:
                    schema = Schema(
                        name=schema_data["name"],
                        description=schema_data["description"],
                        schema_type=schema_data["schema_type"],
                        schema_definition=schema_data["schema_definition"],
                        created_at=datetime.now().replace(tzinfo=None),
                        updated_at=datetime.now().replace(tzinfo=None)
                    )
                    session.add(schema)
                    schemas_added += 1

                await session.commit()
        except Exception as e:
            logger.error(f"Error processing schema {schema_data['name']}: {e}")

    logger.info(f"Schemas: {schemas_added} added, {schemas_updated} updated")


async def seed():
    """Main entry point for seeding schemas."""
    try:
        await seed_async()
    except Exception as e:
        logger.error(f"Schema seeding error: {e}")


if __name__ == "__main__":
    import asyncio
    asyncio.run(seed())
