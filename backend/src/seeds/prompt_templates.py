"""
Seed the prompt_templates table with default template definitions.
"""
import logging
from datetime import datetime
from sqlalchemy import select

from src.db.session import async_session_factory, SessionLocal
from src.models.template import PromptTemplate

# Configure logging
logger = logging.getLogger(__name__)

# Define template contents
GENERATE_AGENT_TEMPLATE = """You are an expert at creating AI agents. Based on the user's description, generate a complete agent setup.

CRITICAL OUTPUT INSTRUCTIONS:
1. Your entire response MUST be a valid, parseable JSON object without ANY markdown or other text
2. Do NOT include ```json, ```, or any other markdown syntax
3. Do NOT include any explanations, comments, or text outside the JSON
4. Structure your response EXACTLY as shown in the example below
5. Ensure all JSON keys and string values use double quotes ("") not single quotes ('')
6. Do NOT add trailing commas in arrays or objects
7. Make sure all opened braces and brackets are properly closed
8. Make sure all property names are properly quoted

Format your response as a JSON object with the following structure:
{
    "name": "descriptive name",
    "role": "specific role title",
    "goal": "clear objective",
    "backstory": "relevant experience and expertise",
    "tools": [],
    "advanced_config": {
        "llm": "databricks-llama-4-maverick",
        "function_calling_llm": null,
        "max_iter": 25,
        "max_rpm": 1,
        "verbose": false,
        "allow_delegation": false,
        "cache": true,
        "allow_code_execution": false,
        "code_execution_mode": "safe",
        "max_retry_limit": 2,
        "use_system_prompt": true,
        "respect_context_window": true
    }
}

Keep your response concise and make sure to:
1. Give the agent a descriptive name
2. Define a clear and specific role
3. Set a concrete goal aligned with the role
4. Write a backstory that explains their expertise (1-2 sentences)
5. Only include tools specifically listed below
6. Keep the advanced configuration with default values

REMINDER: Your output must be PURE, VALID JSON with no additional text. Double-check your response to ensure it is properly formatted JSON."""

GENERATE_CONNECTIONS_TEMPLATE = """Analyze the provided agents and tasks, then create an optimal connection plan with:
1. Task-to-agent assignments based on agent capabilities and task requirements
2. Task dependencies based on information flow and logical sequence
3. Reasoning for each assignment and dependency

Consider the following:
- Match tasks to agents based on role, skills, and tools
- Ensure agents have the right capabilities for their assigned tasks
- Set dependencies to ensure outputs from one task flow to dependent tasks
- Each task should wait for prerequisite tasks that provide necessary inputs

CRITICAL OUTPUT INSTRUCTIONS:
1. Return ONLY raw JSON without any markdown formatting or code block markers
2. Do not include ```json, ``` or any other markdown syntax
3. The response must be a single JSON object that can be directly parsed

Expected JSON structure:
{
    "assignments": [
        {
            "agent_name": "agent name",
            "tasks": [
                {
                    "task_name": "task name",
                    "reasoning": "brief explanation of why this task fits this agent"
                }
            ]
        }
    ],
    "dependencies": [
        {
            "task_name": "task name",
            "required_before": ["task names that must be completed first"],
            "reasoning": "explain why these tasks must be completed first and how their output is used"
        }
    ]
}

Only include tasks in the dependencies array if they actually have prerequisites.
Think carefully about the workflow and how information flows between tasks."""

GENERATE_JOB_NAME_TEMPLATE = """Generate a concise, descriptive name (2-4 words) for an AI job run based on the agents and tasks involved.
Focus on the specific domain, region, and purpose of the job.
The name should reflect the main activity (e.g., 'Lebanese News Monitor' for a Lebanese journalist monitoring news).
Prioritize including:
1. The region or topic (e.g., Lebanese, Beirut)
2. The main activity (e.g., News Analysis, Press Review)
Only return the name, no explanations or additional text.
Avoid generic terms like 'Agent', 'Task', 'Initiative', or 'Collaboration'."""

GENERATE_TASK_TEMPLATE = """You are an expert in designing structured AI task configurations. Your objective is to generate a fully specified task setup suitable for automated systems.
Please provide your response strictly as a valid and well-formatted JSON object using the following schema:
json
{
  "name": "A concise, descriptive name for the task",
  "description": "A detailed explanation of what the task involves, including context, objectives, and requirements",
  "expected_output": "A clear specification of the deliverables, including format, structure, and any constraints",
  "tools": [],
  "advanced_config": {
    "async_execution": false,
    "context": [],
    "output_json": null,
    "output_pydantic": null,
    "output_file": null,
    "human_input": false,
    "retry_on_fail": true,
    "max_retries": 3,
    "timeout": null,
    "priority": 1,
    "dependencies": [],
    "callback": null,
    "error_handling": "default",
    "output_parser": null,
    "cache_response": true,
    "cache_ttl": 3600,
    "markdown": false
  }
}
Please follow these strict guidelines when generating your output:
1. Ensure all fields are present and populated correctly.
2. name must be a short, meaningful string summarizing the task.
3. description should clearly outline what needs to be done, including background or context if necessary.
4. expected_output must describe the output's format, data type, and content expectations.
5. The tools field must be an empty array.
6. Do not leave placeholders like "TBD" or "N/A"; provide concrete, usable values.
7. All boolean and null values must use correct JSON syntax.
8. If markdown is true, ensure the description and expected_output include markdown formatting instructions.
9. Do not include any explanation or commentary—only return the JSON object."""

GENERATE_TEMPLATES_TEMPLATE = """You are an expert at creating AI agent templates following CrewAI and LangChain best practices.
Given an agent's role, goal, and backstory, generate three templates:
1. System Template: Defines the agent's core identity and capabilities
2. Prompt Template: Structures how tasks are presented to the agent
3. Response Template: Guides how the agent should format its responses

Follow these principles:
- System Template should establish expertise, boundaries, and ethical guidelines
- Prompt Template should include placeholders for task-specific information
- Response Template should enforce structured, actionable outputs
- Use {variables} for dynamic content
- Keep templates concise but comprehensive
- Ensure templates work together cohesively

IMPORTANT: Return a JSON object with exactly these field names:
{
    "system_template": "your system template here",
    "prompt_template": "your prompt template here",
    "response_template": "your response template here"
}"""

GENERATE_CREW_TEMPLATE = """You are an expert at creating AI crews. Based on the user's goal, generate a complete crew setup with appropriate agents and tasks.
Each agent should be specialized and have a clear purpose. Each task should be assigned to a specific agent and have clear dependencies.

CRITICAL OUTPUT INSTRUCTIONS:
1. Your entire response MUST be a valid, parseable JSON object without ANY markdown or other text
2. Do NOT include ```json, ```, or any other markdown syntax
3. Do NOT include any explanations, comments, or text outside the JSON
4. Structure your response EXACTLY as shown in the example below
5. Ensure all JSON keys and string values use double quotes ("") not single quotes ('')
6. Do NOT add trailing commas in arrays or objects
7. Make sure all opened braces and brackets are properly closed
8. Make sure all property names are properly quoted
9. Make sure that the context of the task can also include the name of the previous task that will be needed in order to accomplish the task. 

The response must be a single JSON object with two arrays: 'agents' and 'tasks'.

For agents include:
{
    "agents": [
        {
            "name": "descriptive name",
            "role": "specific role title",
            "goal": "clear objective",
            "backstory": "relevant experience and expertise",
            "tools": [],
            "llm": "{
    "dispatcher": {
        "intent": "generate_agent",
        "confidence": 0.95,
        "extracted_info": {
            "agent_type": "flight finder"
        },
        "suggested_prompt": "Create an agent specialized in finding flights"
    },
    "generation_result": {
        "name": "Flight Finder",
        "role": "Flight Search Specialist",
        "goal": "To find the best flights based on user's preferences",
        "backstory": "This agent has access to comprehensive flight databases and can compare flight options based on different criteria such as price, duration, airline, and more.",
        "tools": [],
        "advanced_config": {
            "llm": "databricks-llama-4-maverick",
            "function_calling_llm": null,
            "max_iter": 25,
            "max_rpm": 1,
            "verbose": false,
            "allow_delegation": false,
            "cache": true,
            "allow_code_execution": false,
            "code_execution_mode": "safe",
            "max_retry_limit": 2,
            "use_system_prompt": true,
            "respect_context_window": true,
            "max_execution_time": null,
            "system_template": null,
            "prompt_template": null,
            "response_template": null
        }
    },
    "service_called": "generate_agent"
}",
            "function_calling_llm": null,
            "max_iter": 25,
            "max_rpm": 1,
            "max_execution_time": null,
            "verbose": false,
            "allow_delegation": false,
            "cache": true,
            "system_template": null,
            "prompt_template": null,
            "response_template": null,
            "allow_code_execution": false,
            "code_execution_mode": "safe",
            "max_retry_limit": 2,
            "use_system_prompt": true,
            "respect_context_window": true
        }
    ],
    "tasks": [
        {
            "name": "descriptive name",
            "description": "detailed description",
            "expected_output": "specific deliverable format",
            "agent": null,
            "tools": [],
            "async_execution": false,
            "context": [],
            "config": {},
            "output_json": null,
            "output_pydantic": null,
            "output_file": null,
            "output": null,
            "callback": null,
            "human_input": false,
            "converter_cls": null
        }
    ]
}

Ensure:
1. Each agent has a clear role and purpose
2. Each task is well-defined with clear outputs
3. Tasks are properly sequenced and dependencies are clear
4. All fields have sensible default values
5. An agent might have one or more tasks assigned to it
6. CRITICAL: ONLY use tools that are explicitly listed in the provided tools array. Do not suggest or use any additional tools that are not in the provided list
7. Return the name of the tool exactly as it is in the tools array
8. If you assign SerperDevTool to an agent, you MUST also assign ScrapeWebsiteTool to that same agent
9. IMPORTANT: Each agent should be assigned a MAXIMUM of 3 tasks. If more tasks are needed, distribute them among multiple agents with appropriate roles

REMINDER: Your output must be PURE, VALID JSON with no additional text. Double-check your response to ensure it is properly formatted JSON."""

# Define template data
DEFAULT_TEMPLATES = [
    {
        "name": "generate_agent",
        "description": "Template for generating an AI agent based on user description",
        "template": GENERATE_AGENT_TEMPLATE,
        "is_active": True
    },
    {
        "name": "generate_connections",
        "description": "Template for generating connections between agents and tasks",
        "template": GENERATE_CONNECTIONS_TEMPLATE,
        "is_active": True
    },
    {
        "name": "generate_job_name",
        "description": "Template for generating a job name based on agents and tasks",
        "template": GENERATE_JOB_NAME_TEMPLATE,
        "is_active": True
    },
    {
        "name": "generate_task",
        "description": "Template for generating a task configuration",
        "template": GENERATE_TASK_TEMPLATE,
        "is_active": True
    },
    {
        "name": "generate_templates",
        "description": "Template for generating system, prompt, and response templates",
        "template": GENERATE_TEMPLATES_TEMPLATE,
        "is_active": True
    },
    {
        "name": "generate_crew",
        "description": "Template for generating a complete crew with agents and tasks",
        "template": GENERATE_CREW_TEMPLATE,
        "is_active": True
    }
]

async def seed_async():
    """Seed prompt templates into the database using async session."""
    logger.info("Seeding prompt_templates table (async)...")
    
    # Get existing template names to avoid duplicates (outside the loop to reduce DB queries)
    async with async_session_factory() as session:
        result = await session.execute(select(PromptTemplate.name))
        existing_names = {row[0] for row in result.scalars().all()}
    
    # Insert new templates
    templates_added = 0
    templates_updated = 0
    templates_skipped = 0
    templates_error = 0
    
    # Process each template individually with its own session to avoid transaction problems
    for template_data in DEFAULT_TEMPLATES:
        try:
            # Create a fresh session for each template to avoid transaction conflicts
            async with async_session_factory() as session:
                if template_data["name"] not in existing_names:
                    # Check again to be extra sure - this helps with race conditions
                    check_result = await session.execute(
                        select(PromptTemplate).filter(PromptTemplate.name == template_data["name"])
                    )
                    existing_template = check_result.scalars().first()
                    
                    if existing_template:
                        # If it exists now (race condition), update it instead
                        existing_template.description = template_data["description"]
                        existing_template.template = template_data["template"]
                        existing_template.is_active = template_data["is_active"]
                        existing_template.updated_at = datetime.now().replace(tzinfo=None)
                        logger.debug(f"Updating existing template: {template_data['name']}")
                        templates_updated += 1
                    else:
                        # Add new template
                        template = PromptTemplate(
                            name=template_data["name"],
                            description=template_data["description"],
                            template=template_data["template"],
                            is_active=template_data["is_active"],
                            created_at=datetime.now().replace(tzinfo=None),
                            updated_at=datetime.now().replace(tzinfo=None)
                        )
                        session.add(template)
                        logger.debug(f"Adding new template: {template_data['name']}")
                        templates_added += 1
                else:
                    # Update existing template
                    result = await session.execute(
                        select(PromptTemplate).filter(PromptTemplate.name == template_data["name"])
                    )
                    existing_template = result.scalars().first()
                    
                    if existing_template:
                        existing_template.description = template_data["description"]
                        existing_template.template = template_data["template"]
                        existing_template.is_active = template_data["is_active"]
                        existing_template.updated_at = datetime.now().replace(tzinfo=None)
                        logger.debug(f"Updating existing template: {template_data['name']}")
                        templates_updated += 1
                
                # Commit the session for this template
                try:
                    await session.commit()
                except Exception as e:
                    await session.rollback()
                    if "UNIQUE constraint failed" in str(e):
                        logger.warning(f"Template {template_data['name']} already exists, skipping insert")
                        templates_skipped += 1
                    else:
                        logger.error(f"Failed to commit template {template_data['name']}: {str(e)}")
                        templates_error += 1
        except Exception as e:
            logger.error(f"Error processing template {template_data['name']}: {str(e)}")
            templates_error += 1
    
    logger.info(f"Prompt templates seeding summary: Added {templates_added}, Updated {templates_updated}, Skipped {templates_skipped}, Errors {templates_error}")

def seed_sync():
    """Seed prompt templates into the database using sync session."""
    logger.info("Seeding prompt_templates table (sync)...")
    
    # Get existing template names to avoid duplicates (outside the loop to reduce DB queries)
    with SessionLocal() as session:
        result = session.execute(select(PromptTemplate.name))
        existing_names = {row[0] for row in result.scalars().all()}
    
    # Insert new templates
    templates_added = 0
    templates_updated = 0
    templates_skipped = 0
    templates_error = 0
    
    # Process each template individually with its own session to avoid transaction problems
    for template_data in DEFAULT_TEMPLATES:
        try:
            # Create a fresh session for each template to avoid transaction conflicts
            with SessionLocal() as session:
                if template_data["name"] not in existing_names:
                    # Check again to be extra sure - this helps with race conditions
                    check_result = session.execute(
                        select(PromptTemplate).filter(PromptTemplate.name == template_data["name"])
                    )
                    existing_template = check_result.scalars().first()
                    
                    if existing_template:
                        # If it exists now (race condition), update it instead
                        existing_template.description = template_data["description"]
                        existing_template.template = template_data["template"]
                        existing_template.is_active = template_data["is_active"]
                        existing_template.updated_at = datetime.now().replace(tzinfo=None)
                        logger.debug(f"Updating existing template: {template_data['name']}")
                        templates_updated += 1
                    else:
                        # Add new template
                        template = PromptTemplate(
                            name=template_data["name"],
                            description=template_data["description"],
                            template=template_data["template"],
                            is_active=template_data["is_active"],
                            created_at=datetime.now().replace(tzinfo=None),
                            updated_at=datetime.now().replace(tzinfo=None)
                        )
                        session.add(template)
                        logger.debug(f"Adding new template: {template_data['name']}")
                        templates_added += 1
                else:
                    # Update existing template
                    result = session.execute(
                        select(PromptTemplate).filter(PromptTemplate.name == template_data["name"])
                    )
                    existing_template = result.scalars().first()
                    
                    if existing_template:
                        existing_template.description = template_data["description"]
                        existing_template.template = template_data["template"]
                        existing_template.is_active = template_data["is_active"]
                        existing_template.updated_at = datetime.now().replace(tzinfo=None)
                        logger.debug(f"Updating existing template: {template_data['name']}")
                        templates_updated += 1
                
                # Commit the session for this template
                try:
                    session.commit()
                except Exception as e:
                    session.rollback()
                    if "UNIQUE constraint failed" in str(e):
                        logger.warning(f"Template {template_data['name']} already exists, skipping insert")
                        templates_skipped += 1
                    else:
                        logger.error(f"Failed to commit template {template_data['name']}: {str(e)}")
                        templates_error += 1
        except Exception as e:
            logger.error(f"Error processing template {template_data['name']}: {str(e)}")
            templates_error += 1
    
    logger.info(f"Prompt templates seeding summary: Added {templates_added}, Updated {templates_updated}, Skipped {templates_skipped}, Errors {templates_error}")

# Main entry point for seeding - can be called directly or by seed_runner
async def seed():
    """Main entry point for seeding prompt templates."""
    logger.info("Starting prompt templates seeding process...")
    try:
        await seed_async()
        logger.info("Prompt templates seeding completed successfully")
    except Exception as e:
        logger.error(f"Error seeding prompt templates: {str(e)}")
        import traceback
        logger.error(f"Prompt templates seeding traceback: {traceback.format_exc()}")
        # Don't re-raise - allow other seeds to run

# For backwards compatibility or direct command-line usage
if __name__ == "__main__":
    import asyncio
    asyncio.run(seed()) 