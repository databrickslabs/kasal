"""
Seed the prompt_templates table with default template definitions.
"""
import logging
from datetime import datetime
from sqlalchemy import select

from src.db.session import async_session_factory
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
    "advanced_config": {
        "llm": "databricks-llama-4-maverick",
        "function_calling_llm": null,
        "max_iter": 25,
        "max_rpm": 10,
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
5. Keep the advanced configuration with default values

IMPORTANT: Do NOT include a "tools" field in your response. Tools are assigned at the task level, not the agent level.

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
            "depends_on": ["task names that must be completed first"],
            "reasoning": "explain why these tasks must be completed first and how their output is used"
        }
    ]
}

Only include tasks in the dependencies array if they actually have prerequisites.
Think carefully about the workflow and how information flows between tasks."""

GENERATE_JOB_NAME_TEMPLATE = """Generate a concise, descriptive name (2-4 words) for an AI job run based on the agents and tasks involved.
Focus on the specific domain, region, and purpose of the job.
The name should reflect the main activity (e.g., 'Swiss News Monitor' for a Swiss journalist monitoring news).
Prioritize including:
1. The region or topic (e.g., Switzerland, Zurich)
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
  },
  "llm_guardrail": {"description": "Validation criteria based on expected_output", "llm_model": "databricks-claude-sonnet-4-5"}
}
Please follow these strict guidelines when generating your output:
1. Ensure all fields are present and populated correctly.
2. name must be a short, meaningful string summarizing the task.
3. description should clearly outline what needs to be done, including background or context if necessary.
4. expected_output must describe the output's format, data type, and content expectations.
5. TOOL SELECTION — Read the Tool Catalog below carefully before assigning tools. Only assign tools listed in "Available tools" (provided at the end of this prompt). If no available tools are listed, leave tools as an empty array. NEVER invent tool names.
6. Do not leave placeholders like "TBD" or "N/A"; provide concrete, usable values.
7. All boolean and null values must use correct JSON syntax.
8. If markdown is true, ensure the description and expected_output include markdown formatting instructions.
9. Do not include any explanation or commentary—only return the JSON object.
10. CRITICAL: Do NOT include an "output_file" field in your response. Task outputs should be returned directly as the task result, NOT written to files. The output_file feature is reserved for explicit user requests only.

TOOL CATALOG — Reference this when deciding which tools to assign:

GENERAL RULES:
- Assign at most 1-2 tools per task. Fewer is better.
- ALWAYS prefer internal/organizational data tools over web search when the task involves the user's OWN data (campaigns, metrics, reports, KPIs, employees, products, etc.).
- Use web search tools ONLY when the task explicitly needs external/public information (industry trends, competitor research, general knowledge).
- Tasks that write, compose, synthesize, summarize, or review typically need NO tools — the LLM handles these natively.

1. GenieTool — INTERNAL DATA QUERIES
   USE: When the task needs to query the organization's own data — databases, tables, metrics, KPIs, campaign performance, sales figures, employee data, product catalogs, etc.
   DO NOT USE: For general internet research or external information gathering.
   PRIORITY: HIGH — If the task is about analyzing "our", "my", or company-specific data, this is almost always the right tool.

2. SerperDevTool — WEB SEARCH
   USE: When the task needs to search the public internet for current news, trends, general information, or external competitor data.
   DO NOT USE: For internal data analysis, writing tasks, or when GenieTool would be more appropriate. Not needed if PerplexityTool is already assigned.

3. ScrapeWebsiteTool — WEB CONTENT EXTRACTION
   USE: When the task needs to extract full content from a specific website or URL (e.g., scraping a competitor's product page, extracting article content).
   DO NOT USE: For general search (use SerperDevTool), internal data queries, or writing tasks.

4. PerplexityTool — AI-POWERED RESEARCH
   USE: When the task needs in-depth research with citations and references on external topics, fact-checking, or detailed explanations of complex subjects.
   DO NOT USE: For internal data analysis (use GenieTool), simple web searches (use SerperDevTool), or writing/synthesis tasks.

5. DatabricksKnowledgeSearchTool — DOCUMENT SEARCH (RAG)
   USE: When the task needs to search through uploaded knowledge documents (PDFs, Word docs, text files) stored in the organization's knowledge base.
   DO NOT USE: For structured database queries (use GenieTool), web search, or when no knowledge documents have been uploaded.

6. DatabricksJobsTool — JOB ORCHESTRATION
   USE: When the task needs to list, run, monitor, or create Databricks jobs and data pipelines.
   DO NOT USE: For data analysis (use GenieTool), web search, or content generation tasks.

7. AgentBricksTool — AI AGENT ENDPOINTS
   USE: When the task needs to call a pre-built Databricks AI agent endpoint for specialized processing.
   DO NOT USE: For standard data queries (use GenieTool), web search, or general-purpose tasks.

8. PowerBIAnalysisTool — BUSINESS INTELLIGENCE
   USE: When the task needs to run complex Power BI analytics, DAX queries, year-over-year analysis, or heavy computational BI workloads.
   DO NOT USE: For simple data queries (use GenieTool), web search, or non-BI tasks.

9. MCPTool — MCP SERVER ACCESS
   USE: When the task needs access to specialized tools from the MCP ecosystem not covered by other tools above.
   DO NOT USE: As a first choice — prefer specific tools above. Use only when no other tool fits.

LLM GUARDRAIL GUIDELINES:
The llm_guardrail field enables AI-powered output validation.
IMPORTANT: ALWAYS generate a task-specific guardrail. The description MUST align with the task's expected_output.

Structure: {"description": "task-specific validation criteria", "llm_model": "databricks-claude-sonnet-4-5"}

How to write the guardrail description:
1. Analyze the task's expected_output field
2. Create validation criteria that verify the output meets those expectations
3. Be specific about format, content requirements, and quality standards

The guardrail description should answer: "What makes this task's output valid and complete?"

Examples based on task type:
- Research: {"description": "Output must include at least 3 credible sources, distinguish facts from opinions, and provide specific data points.", "llm_model": "databricks-claude-sonnet-4-5"}
- Writing: {"description": "Must be professional tone, well-structured, free of jargon, and suitable for the intended audience.", "llm_model": "databricks-claude-sonnet-4-5"}
- Analysis: {"description": "Must contain clear methodology, data-backed findings, and actionable recommendations.", "llm_model": "databricks-claude-sonnet-4-5"}
- Email: {"description": "Must have proper email structure, clear message body, and professional tone.", "llm_model": "databricks-claude-sonnet-4-5"}

If the user's goal involves creating a presentation, follow these MANDATORY guidelines:

STRUCTURE & CDN REQUIREMENTS:
- Generate a single HTML file using reveal.js 5.1.0 from jsDelivr CDN. NEVER embed minified JavaScript inline.
- In <head>, include: reset.css, reveal.css, and a theme (white, black, league, or moon).
- Structure: <div class="reveal"><div class="slides"><section>...</section></div></div>
- Before </body>: load reveal.js and call Reveal.initialize()

CRITICAL CONTENT LIMITS TO PREVENT OVERFLOW (STRICTLY ENFORCE):
- Maximum 4-5 bullet points per slide (NEVER exceed 5)
- Maximum 12 words per bullet point (brevity is mandatory)
- NO nested bullet lists - flat structure only
- Headings must be under 6 words
- If a slide includes an image, limit text to 2-3 bullet points only
- One main concept per slide

REQUIRED CSS BLOCK (MUST include in every presentation inside <style> in <head>):
.reveal .slides section { overflow: hidden; }
.reveal h1 { font-size: 2.2em; margin-bottom: 0.5em; }
.reveal h2 { font-size: 1.5em; margin-bottom: 0.4em; }
.reveal ul, .reveal ol { font-size: 0.85em; max-height: 60vh; overflow: hidden; margin-left: 1em; }
.reveal li { margin: 0.4em 0; line-height: 1.3; }
.reveal img { max-height: 45vh; max-width: 85%; display: block; margin: 0 auto; }
.reveal p { font-size: 0.9em; max-height: 50vh; overflow: hidden; }

REQUIRED INITIALIZATION (use exactly this configuration):
Reveal.initialize({ width: 960, height: 700, margin: 0.1, center: true, hash: true, slideNumber: true, transition: 'slide' });

SLIDE ORGANIZATION:
- Title slide: h1 (max 6 words), optional subtitle as p
- Overview slide: h2 + maximum 4 bullet points
- Content slides: h2 + maximum 5 bullet points (12 words each max)
- Conclusion slide: h2 + 3-4 key takeaways
- Use data-background-color for visual variety between slides

CRITICAL: For presentation tasks, the task "description" and "expected_output" fields MUST include ALL technical requirements. Here is an EXAMPLE:
{
    "name": "Create News Presentation",
    "description": "Research news and create an HTML presentation. Generate a single HTML file using reveal.js 5.1.0 from jsDelivr CDN. Include CDN links: https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/reset.css, reveal.css, and theme/white.css in the head. Structure content with max 4-5 bullet points per slide, 12 words max per bullet. Include required CSS: .reveal .slides section { overflow: hidden; } .reveal ul { max-height: 60vh; overflow: hidden; font-size: 0.85em; }. Initialize with: Reveal.initialize({ width: 960, height: 700, margin: 0.1, center: true, hash: true, slideNumber: true }).",
    "expected_output": "A complete HTML file with reveal.js 5.1.0 CDN links in head, required CSS block preventing content overflow, Reveal.initialize() configuration with margin: 0.1, slides containing max 5 bullet points each (12 words max per bullet), title slide, overview, content slides, and conclusion.",
    "llm_guardrail": {"description": "HTML must use reveal.js 5.1.0 CDN (not inline), include overflow prevention CSS, and have no more than 5 bullets per slide with 12 words max each.", "llm_model": "databricks-claude-sonnet-4-5"}
}"""

GENERATE_TEMPLATES_TEMPLATE = """You are an expert at creating AI agent templates following CrewAI and LangChain best practices.
Given an agent's role, goal, and backstory, generate three templates that work together cohesively:

1. System Template: Defines the agent's core identity using {role}, {goal}, and {backstory} parameters
2. Prompt Template: Structures how tasks are presented, including placeholders like {input} and {context}
3. Response Template: Guides response formatting with structured sections for consistency

TEMPLATE REQUIREMENTS:
- System Template MUST incorporate {role}, {goal}, and {backstory} parameters to establish agent identity
- Prompt Template should use {input}, {context}, and other task-specific placeholders including {variables}
- Response Template should enforce structured outputs (e.g., THOUGHTS, ACTION, RESULT sections)
- Include proper placeholder syntax with curly braces for dynamic content including {variables}
- Ensure templates establish expertise boundaries and ethical guidelines
- Make templates model-agnostic and production-ready

EXAMPLE PARAMETER USAGE:
- System: "You are a {role}. {backstory} Your goal is: {goal}"
- Prompt: "Task: {input}\nContext: {context}\nPlease complete this task..."
- Response: "THOUGHTS: [analysis]\nACTION: [what you will do]\nRESULT: [final output]"

CRITICAL OUTPUT INSTRUCTIONS:
1. Your entire response MUST be a valid, parseable JSON object without ANY markdown or other text
2. Do NOT include ```json, ```, or any other markdown syntax
3. Do NOT include any explanations, comments, or text outside the JSON
4. Structure your response EXACTLY as shown in the example below
5. Ensure all JSON keys and string values use double quotes ("") not single quotes ('')
6. Do NOT add trailing commas in arrays or objects
7. Make sure all opened braces and brackets are properly closed
8. Make sure all property names are properly quoted
9. Use proper escape sequences for quotes within template strings

Return a JSON object with exactly these field names:
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
            "llm": "databricks-llama-4-maverick",
            "function_calling_llm": null,
            "max_iter": 25,
            "max_rpm": 10,
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
            "output": null,
            "callback": null,
            "human_input": false,
            "converter_cls": null,
            "llm_guardrail": {"description": "Validation criteria that aligns with expected_output", "llm_model": "databricks-claude-sonnet-4-5"}
        }
    ]
}

TASK LIMIT RULES:
1. CRITICAL: Generate a MAXIMUM of 6 total tasks for the entire crew
2. Each agent should be assigned 1-3 tasks (never more than 3 tasks per agent)
3. If the user's request seems to need more than 6 tasks, focus on the most important core tasks
4. Combine related sub-tasks into single comprehensive tasks when necessary
5. For simple requests, use fewer tasks (2-4 tasks is often sufficient)

Ensure:
1. Each agent has a clear role and purpose
2. Each task is well-defined with clear outputs
3. Tasks are properly sequenced and dependencies are clear
4. All fields have sensible default values
5. An agent might have one or more tasks assigned to it
6. CRITICAL: ONLY use tools that are explicitly listed in the provided tools array. Do not suggest or use any additional tools that are not in the provided list
7. Return the name of the tool exactly as it is in the tools array
8. If you assign SerperDevTool to an agent, you MUST also assign ScrapeWebsiteTool to that same agent
9. The total number of tasks MUST NOT exceed 6 tasks
10. CRITICAL: Do NOT include an "output_file" field in any task. Task outputs should be returned directly as the task result, NOT written to files. The output_file feature is reserved for explicit user requests only.

LLM GUARDRAIL CONFIGURATION:
The llm_guardrail field enables AI-powered output validation for tasks.
IMPORTANT: Generate a task-specific guardrail for EVERY task. The description MUST align with what the task is supposed to produce.

Structure: {"description": "task-specific validation criteria", "llm_model": "databricks-claude-sonnet-4-5"}

How to write the guardrail description for each task:
1. Read the task's expected_output field
2. Create validation criteria that verify the output meets those expectations
3. Be specific about format, content requirements, and quality standards

Examples based on task type:
- Email task: {"description": "Must contain proper email structure with subject context, clear message body, and professional closing. Verify recipient information is referenced correctly.", "llm_model": "databricks-claude-sonnet-4-5"}
- Research task: {"description": "Output must include at least 3 credible sources, distinguish facts from opinions, provide specific data points, and avoid unverified claims.", "llm_model": "databricks-claude-sonnet-4-5"}
- Analysis task: {"description": "Must contain clear methodology, data-backed findings, actionable recommendations, and logical conclusions aligned with the analysis objective.", "llm_model": "databricks-claude-sonnet-4-5"}
- Content/Writing task: {"description": "Must be professional tone, well-structured with clear sections, free of jargon, and suitable for the intended audience.", "llm_model": "databricks-claude-sonnet-4-5"}
- Data processing task: {"description": "Output must be in the specified format, contain all required fields, and have no missing or malformed data.", "llm_model": "databricks-claude-sonnet-4-5"}

The guardrail description should answer: "What makes this task's output valid and complete?"

If the user's goal involves creating a presentation, follow these MANDATORY guidelines:

STRUCTURE & CDN REQUIREMENTS:
- Generate a single HTML file using reveal.js 5.1.0 from jsDelivr CDN. NEVER embed minified JavaScript inline.
- In <head>, include: reset.css, reveal.css, and a theme (white, black, league, or moon).
- Structure: <div class="reveal"><div class="slides"><section>...</section></div></div>
- Before </body>: load reveal.js and call Reveal.initialize()

CRITICAL CONTENT LIMITS TO PREVENT OVERFLOW (STRICTLY ENFORCE):
- Maximum 4-5 bullet points per slide (NEVER exceed 5)
- Maximum 12 words per bullet point (brevity is mandatory)
- NO nested bullet lists - flat structure only
- Headings must be under 6 words
- If a slide includes an image, limit text to 2-3 bullet points only
- One main concept per slide

REQUIRED CSS BLOCK (MUST include in every presentation inside <style> in <head>):
.reveal .slides section { overflow: hidden; }
.reveal h1 { font-size: 2.2em; margin-bottom: 0.5em; }
.reveal h2 { font-size: 1.5em; margin-bottom: 0.4em; }
.reveal ul, .reveal ol { font-size: 0.85em; max-height: 60vh; overflow: hidden; margin-left: 1em; }
.reveal li { margin: 0.4em 0; line-height: 1.3; }
.reveal img { max-height: 45vh; max-width: 85%; display: block; margin: 0 auto; }
.reveal p { font-size: 0.9em; max-height: 50vh; overflow: hidden; }

REQUIRED INITIALIZATION (use exactly this configuration):
Reveal.initialize({ width: 960, height: 700, margin: 0.1, center: true, hash: true, slideNumber: true, transition: 'slide' });

SLIDE ORGANIZATION:
- Title slide: h1 (max 6 words), optional subtitle as p
- Overview slide: h2 + maximum 4 bullet points
- Content slides: h2 + maximum 5 bullet points (12 words each max)
- Conclusion slide: h2 + 3-4 key takeaways
- Use data-background-color for visual variety between slides

CRITICAL: For presentation tasks, the task "description" and "expected_output" fields MUST include ALL technical requirements. Here is an EXAMPLE of a correctly formatted presentation task:
{
    "name": "Create News Presentation",
    "description": "Research news and create an HTML presentation. Generate a single HTML file using reveal.js 5.1.0 from jsDelivr CDN. Include CDN links: https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/reset.css, reveal.css, and theme/white.css in the head. Structure content with max 4-5 bullet points per slide, 12 words max per bullet. Include required CSS: .reveal .slides section { overflow: hidden; } .reveal ul { max-height: 60vh; overflow: hidden; font-size: 0.85em; }. Initialize with: Reveal.initialize({ width: 960, height: 700, margin: 0.1, center: true, hash: true, slideNumber: true }).",
    "expected_output": "A complete HTML file with reveal.js 5.1.0 CDN links in head, required CSS block preventing content overflow, Reveal.initialize() configuration with margin: 0.1, slides containing max 5 bullet points each (12 words max per bullet), title slide, overview, content slides, and conclusion.",
    "agent": "Presentation Creator",
    "llm_guardrail": {"description": "HTML must use reveal.js 5.1.0 CDN (not inline), include overflow prevention CSS, and have no more than 5 bullets per slide with 12 words max each.", "llm_model": "databricks-claude-sonnet-4-5"}
}

REMINDER: Your output must be PURE, VALID JSON with no additional text. Double-check your response to ensure it is properly formatted JSON and contains NO MORE THAN 6 TASKS."""

GENERATE_CREW_PLAN_TEMPLATE = """You are an expert AI crew planner. Given a user's goal, generate a lightweight crew outline with the right number of agents and tasks.

CRITICAL: Return ONLY a JSON object. Do NOT include descriptions, goals, backstories, or tools — those will be generated separately.

CRITICAL OUTPUT INSTRUCTIONS:
1. Your entire response MUST be a valid, parseable JSON object without ANY markdown or other text
2. Do NOT include ```json, ```, or any other markdown syntax
3. Do NOT include any explanations, comments, or text outside the JSON

Return this exact structure:
{
    "complexity": "light|standard|complex",
    "process_type": "sequential|parallel",
    "agents": [
        {"name": "descriptive agent name", "role": "specific role title"}
    ],
    "tasks": [
        {"name": "descriptive task name", "assigned_agent": "exact agent name from above", "context": []}
    ]
}

COMPLEXITY TIERS — Choose the RIGHT size for the goal:

**light** (1-2 agents, 2-3 tasks): Simple, focused requests.
  Examples: "write a blog post", "summarize this document", "analyze this data"
  Pattern: 1 core agent does the work, optional 1 reviewer. 2-3 sequential tasks.

**standard** (2-3 agents, 3-4 tasks): Multi-step workflows with distinct phases.
  Examples: "research a topic then write a report", "analyze competitors and create strategy"
  Pattern: Each phase gets a specialized agent. 3-4 tasks forming a clear pipeline.

**complex** (3-4 agents, 4-6 tasks): Multi-domain work with parallel tracks or specialized roles.
  Examples: "build a market analysis with competitive intel, financial data, and strategic recommendations"
  Pattern: Multiple specialists working on different aspects, results synthesized. Some tasks may run in parallel.

HARD CAPS: NEVER exceed 4 agents or 6 tasks. More agents/tasks = slower execution. Prefer fewer, well-defined entities.

PROCESS TYPE RULES:

**sequential** (DEFAULT — use for most crews): Tasks form a pipeline where each builds on the previous.
  - The FIRST task has an empty context array: "context": []
  - EVERY subsequent task MUST list the previous task name in its context array
  - Example: Task 1 context=[], Task 2 context=["Task 1 name"], Task 3 context=["Task 2 name"]
  - This ensures each task receives the output of the previous task

**parallel**: Use ONLY when tasks are genuinely independent and can run simultaneously.
  - Independent tasks have empty context arrays
  - A final synthesis/merge task should list ALL parallel tasks in its context array
  - Example: Task A context=[], Task B context=[], Task C (synthesis) context=["Task A name", "Task B name"]
  - Use this when the goal has independent sub-problems that converge

AGENT ASSIGNMENT RULES:
1. Each agent should own 1-2 tasks (never more than 3)
2. Avoid assigning unrelated tasks to the same agent
3. An agent's tasks should align with their role

TASK DEPENDENCY RULES:
1. Each task's "context" array contains names of tasks whose output it needs
2. For sequential: chain tasks linearly (each depends on the previous)
3. For parallel: only add genuine data dependencies, use empty context for independent tasks
4. A task CANNOT depend on itself or on tasks that come after it
5. Keep names concise but descriptive (2-5 words)

REMINDER: Your output must be PURE, VALID JSON with no additional text."""

DETECT_INTENT_TEMPLATE = """You are an intelligent intent detection system for a CrewAI workflow designer.

Analyze the user's message and determine their intent from these categories:

1. **generate_task**: User wants to create a SINGLE task or action. Look for:
   - Single action words: find, search, analyze, create, write, calculate, etc.
   - Simple task descriptions: "find the best flight", "analyze this data", "write a report"
   - Instructions for one specific action: "get information about X", "compare Y and Z"
   - Casual requests that imply a single task: "an order find...", "I need to...", "help me..."
   - Commands or directives for one action: "find me", "get the", "calculate", "determine"
   - IMPORTANT: If the message contains "create a plan" or "plan that", it is NOT generate_task

2. **generate_agent**: User wants to create a single agent with specific capabilities:
   - Explicit mentions of "agent", "assistant", "bot"
   - Role-based requests: "create a financial analyst", "I need a data scientist"
   - Capability-focused: "something that can analyze data and write reports"

3. **generate_crew**: User wants to create multiple agents and/or tasks working together:
   - Multiple roles mentioned: "team of agents", "research and writing team"
   - Complex workflows: "research then write then review"
   - Collaborative language: "agents working together", "workflow with multiple steps"
   - Multiple related tasks that need coordination
   - Planning language: "create a plan", "build a plan", "design a plan", "plan that", "plan to"
   - Strategic terms: "roadmap", "blueprint", "framework", "architecture", "strategy"
   - Complex multi-step operations: "get all news", "analyze multiple sources", "comprehensive collection"

4. **execute_crew**: User wants to execute/run an existing crew:
   - Execution commands: "execute crew", "run crew", "start crew", "ec"
   - Action words with crew context: "execute", "run", "start", "launch", "begin"
   - Short commands: "ec" (shorthand for execute crew)

6. **configure_crew**: User wants to configure workflow settings (LLM, max RPM, tools):
   - Configuration requests: "configure crew", "setup llm", "change model", "select tools"
   - Settings modifications: "update max rpm", "set llm model", "modify tools"
   - Preference adjustments: "choose different model", "adjust settings", "pick tools"
   - Direct mentions: "llm", "maxr", "max rpm", "tools", "config", "settings"

7. **unknown**: Unclear or ambiguous messages that don't fit the above categories.

**CRITICAL RULES**:
1. Many task requests are phrased conversationally. Look for ACTION WORDS and GOALS rather than formal task language.
2. If the message describes multiple agents or complex workflows, it's generate_crew.

Return a JSON object with:
{
    "intent": "generate_task" | "generate_agent" | "generate_crew" | "execute_crew" | "configure_crew" | "unknown",
    "confidence": 0.0-1.0,
    "extracted_info": {
        "action_words": ["list", "of", "detected", "action", "words"],
        "entities": ["extracted", "entities", "or", "objects"],
        "goal": "what the user wants to accomplish",
        "config_type": "llm|maxr|tools|general" // Only for configure_crew intent
    },
    "suggested_prompt": "Enhanced version optimized for the specific service"
}

Examples:
- "Create an agent that can analyze data" -> generate_agent
- "I need a task to summarize documents" -> generate_task
- "an order find the best flight between zurich and montreal" -> generate_task
- "find me the cheapest hotel in paris" -> generate_task
- "get information about the weather tomorrow" -> generate_task
- "analyze this sales data and create a report" -> generate_task
- "Build a team of agents to handle customer support" -> generate_crew
- "Create a research agent and a writer agent with tasks for each" -> generate_crew
- "Create a plan that will get all the news from switzerland" -> generate_crew
- "Plan to collect and analyze customer feedback" -> generate_crew
- "Build a plan for market analysis" -> generate_crew
- "Create a plan with multiple agents" -> generate_crew
- "execute crew" -> execute_crew
- "run crew" -> execute_crew
- "ec" -> execute_crew
- "configure crew" -> configure_crew
- "setup llm" -> configure_crew
- "change model" -> configure_crew
- "select tools" -> configure_crew
- "update max rpm" -> configure_crew
- "adjust settings" -> configure_crew
"""

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
    },
    {
        "name": "detect_intent",
        "description": "Template for detecting user intent in natural language messages",
        "template": DETECT_INTENT_TEMPLATE,
        "is_active": True
    },
    {
        "name": "generate_crew_plan",
        "description": "Template for generating a lightweight crew plan outline with agent names/roles and task names/assignments",
        "template": GENERATE_CREW_PLAN_TEMPLATE,
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
            await session.rollback()
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
            session.rollback()
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