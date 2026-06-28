"""
Seed the schemas table with CrewAI output schemas for flow routing.

Every schema here describes the structured output of an ENTIRE crew/workload run
(one object per kickoff, assigned to the final task via output_pydantic) — not
per-row or content detail. Fields are deliberately routing-friendly: categorical
strings, booleans, and numbers that a flow Router condition can branch on
(e.g. status == "failed", rows_inserted > 0, risk_level == "high").

Content-generation schemas (article/email/report bodies, free-text summaries) were
intentionally left out: their fields are prose you cannot route on. Add those per
task as needed; this seed set targets decision/outcome workloads.
"""
import logging
from datetime import datetime
from sqlalchemy import select

from src.db.session import async_session_factory
from src.models.schema import Schema

logger = logging.getLogger(__name__)

# Schema names that earlier seed versions created but that don't describe a routable
# whole-crew outcome. Removed from the DB on seed so the picker stays focused.
OBSOLETE_SCHEMA_NAMES = [
    "Article", "Summary", "Analysis", "SearchResults", "Recommendation",
    "ActionItems", "Email", "Report", "QA",
]

# Whole-crew workload outcomes with routing-friendly fields.
SAMPLE_SCHEMAS = [
    # ── Action / automation outcomes ────────────────────────────────────────────
    {
        "name": "OperationResult",
        "description": "Action workloads - outcome of an operation (DB writes, API calls, jobs)",
        "schema_type": "schema",
        "schema_definition": {
            "type": "object",
            "properties": {
                "success": {"type": "boolean"},
                "status": {"type": "string"},
                "rows_affected": {"type": "integer"},
                "records_processed": {"type": "integer"},
                "error_count": {"type": "integer"},
                "message": {"type": "string"}
            },
            "required": ["status", "success"]
        }
    },
    {
        "name": "DataLoadResult",
        "description": "ETL / data loading - row counts for an insert/update workload",
        "schema_type": "schema",
        "schema_definition": {
            "type": "object",
            "properties": {
                "table": {"type": "string"},
                "rows_inserted": {"type": "integer"},
                "rows_updated": {"type": "integer"},
                "rows_failed": {"type": "integer"},
                "status": {"type": "string"},
                "success": {"type": "boolean"}
            },
            "required": ["status", "rows_inserted"]
        }
    },

    # ── Classification / triage outcomes ────────────────────────────────────────
    {
        "name": "SupportTicketTriage",
        "description": "Customer support - classify & route an incoming ticket",
        "schema_type": "schema",
        "schema_definition": {
            "type": "object",
            "properties": {
                "category": {"type": "string"},
                "priority": {"type": "string"},
                "sentiment": {"type": "string"},
                "requires_human": {"type": "boolean"},
                "suggested_team": {"type": "string"}
            },
            "required": ["category", "priority"]
        }
    },
    {
        "name": "SentimentAnalysis",
        "description": "CX & social - overall sentiment of the analyzed input",
        "schema_type": "schema",
        "schema_definition": {
            "type": "object",
            "properties": {
                "sentiment": {"type": "string"},
                "score": {"type": "number"}
            },
            "required": ["sentiment"]
        }
    },
    {
        "name": "IntentClassification",
        "description": "Conversational - classify user intent for routing",
        "schema_type": "schema",
        "schema_definition": {
            "type": "object",
            "properties": {
                "intent": {"type": "string"},
                "confidence": {"type": "number"},
                "fallback": {"type": "boolean"}
            },
            "required": ["intent"]
        }
    },
    {
        "name": "CustomerFeedback",
        "description": "CX - categorize feedback with sentiment and NPS",
        "schema_type": "schema",
        "schema_definition": {
            "type": "object",
            "properties": {
                "category": {"type": "string"},
                "sentiment": {"type": "string"},
                "nps_score": {"type": "integer"},
                "action_required": {"type": "boolean"}
            },
            "required": ["sentiment"]
        }
    },

    # ── Research / retrieval outcomes ───────────────────────────────────────────
    {
        "name": "WebSearchResult",
        "description": "Web search - outcome of an online search (counts, top hit, relevance)",
        "schema_type": "schema",
        "schema_definition": {
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "results_found": {"type": "integer"},
                "has_results": {"type": "boolean"},
                "answer_found": {"type": "boolean"},
                "top_result_url": {"type": "string"},
                "top_result_title": {"type": "string"},
                "relevance_score": {"type": "number"}
            },
            "required": ["query", "results_found", "has_results"]
        }
    },

    # ── Decision / approval outcomes ────────────────────────────────────────────
    {
        "name": "ApprovalDecision",
        "description": "Operations - approve / reject / route for human review",
        "schema_type": "schema",
        "schema_definition": {
            "type": "object",
            "properties": {
                "decision": {"type": "string"},
                "confidence": {"type": "number"}
            },
            "required": ["decision"]
        }
    },
    {
        "name": "LeadQualification",
        "description": "Sales - qualify and score an inbound lead",
        "schema_type": "schema",
        "schema_definition": {
            "type": "object",
            "properties": {
                "qualified": {"type": "boolean"},
                "score": {"type": "integer"},
                "tier": {"type": "string"},
                "estimated_value": {"type": "number"}
            },
            "required": ["qualified", "score"]
        }
    },
    {
        "name": "ResumeScreening",
        "description": "HR/recruiting - screen a candidate against a role",
        "schema_type": "schema",
        "schema_definition": {
            "type": "object",
            "properties": {
                "candidate_name": {"type": "string"},
                "match_score": {"type": "integer"},
                "recommended": {"type": "boolean"},
                "decision": {"type": "string"}
            },
            "required": ["match_score", "decision"]
        }
    },
    {
        "name": "Evaluation",
        "description": "Scoring & review - overall score and verdict for the subject",
        "schema_type": "schema",
        "schema_definition": {
            "type": "object",
            "properties": {
                "subject": {"type": "string"},
                "score": {"type": "integer"},
                "verdict": {"type": "string"}
            },
            "required": ["verdict"]
        }
    },

    # ── Risk / compliance / finance outcomes ────────────────────────────────────
    {
        "name": "RiskAssessment",
        "description": "Risk/compliance - score and classify risk for escalation",
        "schema_type": "schema",
        "schema_definition": {
            "type": "object",
            "properties": {
                "risk_level": {"type": "string"},
                "risk_score": {"type": "integer"},
                "requires_escalation": {"type": "boolean"}
            },
            "required": ["risk_level"]
        }
    },
    {
        "name": "ContentModeration",
        "description": "Trust & safety - flag content and decide an action",
        "schema_type": "schema",
        "schema_definition": {
            "type": "object",
            "properties": {
                "flagged": {"type": "boolean"},
                "category": {"type": "string"},
                "severity": {"type": "string"},
                "action": {"type": "string"}
            },
            "required": ["flagged", "action"]
        }
    },
    {
        "name": "FraudCheck",
        "description": "Security - flag suspected fraud and recommend an action",
        "schema_type": "schema",
        "schema_definition": {
            "type": "object",
            "properties": {
                "is_fraud": {"type": "boolean"},
                "risk_score": {"type": "integer"},
                "recommended_action": {"type": "string"}
            },
            "required": ["is_fraud", "recommended_action"]
        }
    },
    {
        "name": "ExpenseApproval",
        "description": "Finance - check an expense against policy and approve",
        "schema_type": "schema",
        "schema_definition": {
            "type": "object",
            "properties": {
                "amount": {"type": "number"},
                "category": {"type": "string"},
                "policy_compliant": {"type": "boolean"},
                "approval_status": {"type": "string"}
            },
            "required": ["amount", "approval_status"]
        }
    },
    {
        "name": "InvoiceData",
        "description": "Finance/AP - extracted invoice header for downstream routing",
        "schema_type": "schema",
        "schema_definition": {
            "type": "object",
            "properties": {
                "vendor": {"type": "string"},
                "invoice_number": {"type": "string"},
                "total_amount": {"type": "number"},
                "currency": {"type": "string"},
                "status": {"type": "string"}
            },
            "required": ["vendor", "total_amount"]
        }
    }
]


async def seed_async():
    """Seed schemas into the database (upsert by name) and prune obsolete ones."""
    logger.info("Seeding schemas...")

    schemas_added = 0
    schemas_updated = 0
    schemas_removed = 0

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

    # Prune schemas from earlier seed versions that no longer fit the routing model.
    for name in OBSOLETE_SCHEMA_NAMES:
        try:
            async with async_session_factory() as session:
                result = await session.execute(
                    select(Schema).filter(Schema.name == name)
                )
                existing = result.scalars().first()
                if existing:
                    await session.delete(existing)
                    await session.commit()
                    schemas_removed += 1
        except Exception as e:
            logger.error(f"Error removing obsolete schema {name}: {e}")

    logger.info(
        f"Schemas: {schemas_added} added, {schemas_updated} updated, {schemas_removed} removed"
    )


async def seed():
    """Main entry point for seeding schemas."""
    try:
        await seed_async()
    except Exception as e:
        logger.error(f"Schema seeding error: {e}")


if __name__ == "__main__":
    import asyncio
    asyncio.run(seed())
