"""
DAX Generator Service

This service generates DAX queries from natural language questions using LLMs.
It's designed to work with Power BI semantic models via XMLA endpoints.

The generated DAX can then be executed in Databricks notebooks using the
Power BI integration.
"""

import json
import logging
from typing import Any, Dict, List, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from src.utils.powerbi_connector import PowerBIMetadataExtractor, clean_dax_query

logger = logging.getLogger(__name__)


class DAXGeneratorService:
    """
    Service for generating DAX queries from natural language questions.

    This service uses LLMs to translate user questions into DAX queries
    based on Power BI dataset metadata.
    """

    def __init__(self, session: AsyncSession):
        """
        Initialize the DAX Generator Service.

        Args:
            session: SQLAlchemy async session for database operations
        """
        self.session = session
        self.metadata_extractor = PowerBIMetadataExtractor()

    @classmethod
    async def from_unit_of_work(cls, uow):
        """
        Create service instance from Unit of Work.

        Args:
            uow: Unit of Work instance

        Returns:
            DAXGeneratorService instance
        """
        return cls(uow.session)

    async def generate_dax_from_question(
        self,
        question: str,
        metadata: Dict[str, Any],
        model_name: str = "databricks-meta-llama-3-1-405b-instruct",
        temperature: float = 0.1
    ) -> Dict[str, Any]:
        """
        Generate a DAX query from a natural language question.

        Args:
            question: Natural language question about the data
            metadata: Power BI dataset metadata (tables, columns, relationships)
            model_name: LLM model to use for generation
            temperature: Temperature for LLM generation (lower = more deterministic)

        Returns:
            Dictionary containing:
                - dax_query: Generated DAX query string
                - explanation: Explanation of what the query does
                - confidence: Confidence score (0-1)
        """
        try:
            # Set metadata for extraction
            self.metadata_extractor.set_metadata(metadata)

            # Format metadata for LLM
            metadata_str = self.metadata_extractor.format_metadata_for_llm()

            # Build prompt for DAX generation
            prompt = self._build_dax_generation_prompt(question, metadata_str)

            # Call LLM to generate DAX
            response = await self._call_llm(prompt, model_name, temperature)

            # Parse and clean the response
            dax_result = self._parse_dax_response(response)

            logger.info(f"Successfully generated DAX query for question: {question[:100]}")
            return dax_result

        except Exception as e:
            logger.error(f"Error generating DAX query: {str(e)}", exc_info=True)
            raise

    async def generate_dax_with_samples(
        self,
        question: str,
        metadata: Dict[str, Any],
        sample_data: Optional[Dict[str, List[Dict[str, Any]]]] = None,
        model_name: str = "databricks-meta-llama-3-1-405b-instruct",
        temperature: float = 0.1
    ) -> Dict[str, Any]:
        """
        Generate DAX query with sample data context for better accuracy.

        Args:
            question: Natural language question
            metadata: Power BI dataset metadata
            sample_data: Optional sample data from tables for context
            model_name: LLM model to use
            temperature: Temperature for generation

        Returns:
            Dictionary with DAX query, explanation, and confidence
        """
        try:
            # Set metadata
            self.metadata_extractor.set_metadata(metadata)

            # Format metadata and samples
            metadata_str = self.metadata_extractor.format_metadata_for_llm()

            sample_str = ""
            if sample_data:
                sample_str = "\n\nSample Data for Reference:\n"
                for table_name, samples in sample_data.items():
                    sample_str += f"\n{table_name} (first {len(samples)} rows):\n"
                    sample_str += json.dumps(samples, indent=2)

            # Build enhanced prompt
            prompt = self._build_dax_generation_prompt(
                question, metadata_str, sample_str
            )

            # Generate DAX
            response = await self._call_llm(prompt, model_name, temperature)

            # Parse response
            dax_result = self._parse_dax_response(response)

            logger.info(f"Generated DAX with samples for: {question[:100]}")
            return dax_result

        except Exception as e:
            logger.error(f"Error generating DAX with samples: {str(e)}", exc_info=True)
            raise

    async def suggest_questions(
        self,
        metadata: Dict[str, Any],
        model_name: str = "databricks-meta-llama-3-1-405b-instruct",
        num_suggestions: int = 5
    ) -> List[str]:
        """
        Suggest relevant questions based on dataset metadata.

        Args:
            metadata: Power BI dataset metadata
            model_name: LLM model to use
            num_suggestions: Number of questions to suggest

        Returns:
            List of suggested questions
        """
        try:
            self.metadata_extractor.set_metadata(metadata)
            metadata_str = self.metadata_extractor.format_metadata_for_llm()

            prompt = f"""Based on the following Power BI dataset structure, suggest {num_suggestions} interesting questions a user might ask:

{metadata_str}

Generate {num_suggestions} diverse questions that would showcase different aspects of the data.
Return only the questions as a JSON array, nothing else.

Example format:
["Question 1", "Question 2", "Question 3"]
"""

            response = await self._call_llm(prompt, model_name, temperature=0.7)

            # Parse JSON response
            try:
                questions = json.loads(response.strip())
                if isinstance(questions, list):
                    return questions[:num_suggestions]
            except json.JSONDecodeError:
                logger.warning("Failed to parse suggested questions as JSON")

            # Fallback suggestions
            return [
                "What are the total sales?",
                "Show me the top 10 products by revenue",
                "What is the revenue trend over time?",
                "Which region has the highest sales?",
                "What are the key performance metrics?"
            ][:num_suggestions]

        except Exception as e:
            logger.error(f"Error suggesting questions: {str(e)}", exc_info=True)
            return [
                "What are the total sales?",
                "Show me the top 10 products",
                "What is the trend over time?"
            ][:num_suggestions]

    def _build_dax_generation_prompt(
        self,
        question: str,
        metadata_str: str,
        sample_data_str: str = ""
    ) -> str:
        """
        Build the prompt for DAX query generation.

        Args:
            question: User's natural language question
            metadata_str: Formatted metadata string
            sample_data_str: Optional formatted sample data

        Returns:
            Complete prompt string
        """
        prompt = f"""You are a Power BI DAX expert. Generate a DAX query to answer the following question.

Available dataset structure:
{metadata_str}
{sample_data_str}

User question: {question}

IMPORTANT RULES:
1. Generate only the DAX query without any explanation or markdown
2. Do NOT use any HTML or XML tags in the query
3. Do NOT use angle brackets < or > except for DAX operators
4. Use only valid DAX syntax
5. Reference only columns and measures that exist in the schema
6. The query should be executable as-is
7. Use proper DAX functions like EVALUATE, SUMMARIZE, FILTER, CALCULATE, etc.
8. Start the query with EVALUATE

Example format:
EVALUATE SUMMARIZE(Sales, Product[Category], "Total Revenue", SUM(Sales[Amount]))

Now generate the DAX query for the user's question:"""

        return prompt

    async def _call_llm(
        self,
        prompt: str,
        model_name: str,
        temperature: float
    ) -> str:
        """
        Call LLM to generate response.

        Args:
            prompt: Prompt for the LLM
            model_name: Model identifier
            temperature: Generation temperature

        Returns:
            LLM response string
        """
        try:
            # Import here to avoid circular dependencies
            from src.engines.crewai.llm_manager import LLMManager

            # Initialize LLM manager
            llm_manager = LLMManager()

            # Get LLM instance
            llm = llm_manager.get_llm(model_name, temperature=temperature)

            # Generate response
            response = await llm.ainvoke(prompt)

            # Extract content from response
            if hasattr(response, 'content'):
                return response.content
            elif isinstance(response, str):
                return response
            else:
                return str(response)

        except Exception as e:
            logger.error(f"Error calling LLM: {str(e)}", exc_info=True)
            raise

    def _parse_dax_response(self, response: str) -> Dict[str, Any]:
        """
        Parse and clean the LLM response to extract DAX query.

        Args:
            response: Raw LLM response

        Returns:
            Dictionary with cleaned query and metadata
        """
        try:
            # Clean the response
            cleaned = clean_dax_query(response)

            # Remove markdown code blocks if present
            if "```" in cleaned:
                # Extract content between code blocks
                parts = cleaned.split("```")
                for part in parts:
                    if "EVALUATE" in part.upper():
                        cleaned = part.strip()
                        # Remove language identifier if present
                        if cleaned.startswith("dax\n") or cleaned.startswith("DAX\n"):
                            cleaned = cleaned[4:].strip()
                        break

            # Ensure query starts with EVALUATE
            if not cleaned.strip().upper().startswith("EVALUATE"):
                # Try to find EVALUATE in the response
                lines = cleaned.split("\n")
                for i, line in enumerate(lines):
                    if "EVALUATE" in line.upper():
                        cleaned = "\n".join(lines[i:])
                        break

            # Calculate a simple confidence score based on query characteristics
            confidence = 0.8  # Default confidence

            # Increase confidence if query has proper DAX structure
            if "EVALUATE" in cleaned.upper():
                confidence += 0.1
            if any(func in cleaned.upper() for func in ["SUMMARIZE", "CALCULATE", "FILTER", "TOPN"]):
                confidence += 0.1

            # Cap at 1.0
            confidence = min(1.0, confidence)

            return {
                "dax_query": cleaned.strip(),
                "explanation": "Generated DAX query from natural language question",
                "confidence": confidence,
                "raw_response": response
            }

        except Exception as e:
            logger.error(f"Error parsing DAX response: {str(e)}", exc_info=True)
            return {
                "dax_query": response.strip(),
                "explanation": "Raw response (parsing failed)",
                "confidence": 0.5,
                "raw_response": response
            }
