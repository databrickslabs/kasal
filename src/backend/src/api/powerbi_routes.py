"""
Power BI API Routes

API endpoints for Power BI DAX query generation.
"""

import logging
from typing import Any, Dict

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.dependencies import get_db, get_group_context
from src.utils.user_context import GroupContext
from src.schemas.powerbi import (
    DAXGenerationRequest,
    DAXGenerationResponse,
    QuestionSuggestionRequest,
    QuestionSuggestionResponse,
)
from src.services.dax_generator_service import DAXGeneratorService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/powerbi", tags=["Power BI"])


@router.post("/generate-dax", response_model=DAXGenerationResponse)
async def generate_dax(
    request: DAXGenerationRequest,
    db: AsyncSession = Depends(get_db),
    group_context: GroupContext = Depends(get_group_context)
) -> Any:
    """
    Generate a DAX query from a natural language question.

    This endpoint takes a natural language question and Power BI dataset metadata,
    then generates an executable DAX query using LLMs.

    Args:
        request: DAX generation request with question and metadata
        db: Database session
        group_context: Group context for multi-tenancy

    Returns:
        Generated DAX query with explanation and confidence score

    Example:
        ```json
        {
            "question": "What is the total NSR per product?",
            "metadata": {
                "tables": [
                    {
                        "name": "Products",
                        "columns": [
                            {"name": "ProductID", "data_type": "int"},
                            {"name": "ProductName", "data_type": "string"},
                            {"name": "NSR", "data_type": "decimal"}
                        ]
                    }
                ]
            },
            "model_name": "databricks-meta-llama-3-1-405b-instruct",
            "temperature": 0.1
        }
        ```
    """
    try:
        logger.info(f"Generating DAX for question: {request.question}")

        # Initialize service
        service = DAXGeneratorService(db)

        # Generate DAX query
        if request.sample_data:
            result = await service.generate_dax_with_samples(
                question=request.question,
                metadata=request.metadata,
                sample_data=request.sample_data,
                model_name=request.model_name,
                temperature=request.temperature
            )
        else:
            result = await service.generate_dax_from_question(
                question=request.question,
                metadata=request.metadata,
                model_name=request.model_name,
                temperature=request.temperature
            )

        logger.info(f"Successfully generated DAX query (confidence: {result['confidence']:.0%})")

        return DAXGenerationResponse(**result)

    except Exception as e:
        logger.error(f"Error generating DAX: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate DAX query: {str(e)}"
        )


@router.post("/suggest-questions", response_model=QuestionSuggestionResponse)
async def suggest_questions(
    request: QuestionSuggestionRequest,
    db: AsyncSession = Depends(get_db),
    group_context: GroupContext = Depends(get_group_context)
) -> Any:
    """
    Suggest relevant questions based on Power BI dataset metadata.

    This endpoint analyzes the dataset structure and suggests interesting
    questions that users might want to ask.

    Args:
        request: Question suggestion request with metadata
        db: Database session
        group_context: Group context for multi-tenancy

    Returns:
        List of suggested questions

    Example:
        ```json
        {
            "metadata": {
                "tables": [...]
            },
            "num_suggestions": 5,
            "model_name": "databricks-meta-llama-3-1-405b-instruct"
        }
        ```
    """
    try:
        logger.info(f"Suggesting {request.num_suggestions} questions")

        # Initialize service
        service = DAXGeneratorService(db)

        # Get suggested questions
        questions = await service.suggest_questions(
            metadata=request.metadata,
            model_name=request.model_name,
            num_suggestions=request.num_suggestions
        )

        logger.info(f"Generated {len(questions)} question suggestions")

        return QuestionSuggestionResponse(questions=questions)

    except Exception as e:
        logger.error(f"Error suggesting questions: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to suggest questions: {str(e)}"
        )


@router.get("/health")
async def health_check() -> Dict[str, str]:
    """
    Health check endpoint for Power BI DAX generator.

    Returns:
        Health status
    """
    return {"status": "healthy", "service": "powerbi-dax-generator"}
