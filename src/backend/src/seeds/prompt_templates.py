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


# Injected into the crew/task GENERATION prompt at runtime ONLY when GenieTool is
# among the workspace's available tools. Biases the generator toward Genie for
# questions about the org's OWN data/metrics (instead of defaulting to web search),
# so an Auto-format prompt like "the most effective marketing campaign" produces a
# Genie crew (and therefore the Genie-space picker) rather than a Perplexity crew.
GENIE_ROUTING_DIRECTIVE = "\n".join([
    "",
    "=== TOOL ROUTING — READ CAREFULLY (GenieTool is available) ===",
    "GenieTool answers questions from the ORGANIZATION'S OWN structured data (its data",
    "warehouse) using natural language. It is the DEFAULT tool for any question about",
    "the organization's data, metrics, or business performance, INCLUDING:",
    "- marketing campaigns, campaign effectiveness, ROI, CTR, conversions, spend",
    "- sales, revenue, pipeline, orders; customers, segments, churn, retention",
    "- products, inventory, operations; any KPI / analytics question",
    "- any 'top N', 'most/least', 'best', 'highest/lowest', 'trend', or 'by <dimension>' question",
    "For ALL such questions you MUST assign ONLY GenieTool to the data task, and you",
    "MUST NOT assign PerplexityTool or ScrapeWebsiteTool.",
    "Use web-search tools ONLY when the user EXPLICITLY asks for EXTERNAL / public",
    "information (news, competitor research, market trends, facts not in the org's data).",
    "EXAMPLES:",
    "- 'what is the most effective marketing campaign' -> GenieTool (the org's campaign data)",
    "- 'top customers by revenue this quarter' -> GenieTool",
    "- 'latest AI news' / 'what are competitors doing' -> PerplexityTool (external)",
    "When in doubt for a business/metrics question, choose GenieTool.",
])

# Define template contents
GENERATE_AGENT_TEMPLATE = """You are an expert at creating AI agents. From the user's description, generate ONE agent as a single valid JSON object — no markdown, no commentary, double quotes, no trailing commas — with EXACTLY these fields:
{"name": "descriptive, domain-specific name", "role": "specific role title", "goal": "concrete objective containing an action verb", "backstory": "1-2 sentences (10-60 words) of relevant professional expertise"}
Omit every other field (advanced_config, llm, tools, etc.) — the platform fills sane defaults. Do NOT include a "tools" field: tools are assigned at the task level, not the agent level.

QUALITY REQUIREMENTS:
- name: descriptive and domain-specific (e.g. "Financial Data Analyst Agent", NOT "Agent" or "Data Agent").
- role: SPECIFIC — never "Agent", "Assistant", "Helper", "Bot", or "AI Agent" alone. Good: "Financial Data Analyst", "Customer Support Specialist", "Kubernetes SRE", "Content Marketing Strategist".
- goal: concrete and contains an action verb (analyze, create, build, monitor, review, write, detect, translate…). Good: "Analyze financial datasets to identify trends, anomalies, and key metrics, then generate reports with actionable insights." Bad: "Help with data".
- backstory: 1-2 sentences (10-60 words) establishing relevant professional expertise.

EXAMPLES:
User: "Create an agent that can analyze financial data and generate reports"
Output: {"name": "Financial Data Analyst Agent", "role": "Financial Data Analyst", "goal": "Analyze financial datasets to identify trends, anomalies, and key metrics, then generate comprehensive reports with actionable insights and visualizations", "backstory": "Expert financial analyst with 10+ years of experience in data analysis, financial modeling, and business intelligence reporting, skilled at transforming complex data into clear, actionable insights for stakeholders."}
User: "create an agent"
Output: {"name": "General Purpose Assistant", "role": "Versatile Task Executor", "goal": "Execute a wide range of tasks by analyzing requirements, applying appropriate methodologies, and delivering well-structured outputs tailored to each specific request", "backstory": "Experienced generalist with expertise spanning data analysis, content creation, research, and problem-solving, adept at quickly understanding requirements and delivering high-quality results across diverse domains."}"""

GENERATE_CONNECTIONS_TEMPLATE = """Analyze the provided agents and tasks, then create an optimal connection plan with:
1. Task-to-agent assignments based on agent capabilities and task requirements
2. Task dependencies based on information flow and logical sequence
3. Reasoning for each assignment and dependency

CRITICAL RULES:
- EVERY task must be assigned to exactly one agent — no unassigned tasks
- EVERY agent must be assigned at least one task — no orphan agents with zero tasks
- Only use agent names that exist in the provided agents list
- Dependencies must form a valid DAG (directed acyclic graph) — no circular dependencies
- Every assignment MUST include a reasoning explaining why that agent fits the task
- Only include tasks in dependencies array if they actually have prerequisites
- The first task in a sequential flow has no dependencies (empty depends_on)
- VALIDATION: After creating assignments, verify that every agent from the input list appears in at least one assignment. If an agent has no tasks, redistribute tasks or flag it

Consider the following:
- Match tasks to agents based on role, skills, and tools
- Ensure agents have the right capabilities for their assigned tasks
- Set dependencies to ensure outputs from one task flow to dependent tasks
- Each task should wait for prerequisite tasks that provide necessary inputs
- One agent can handle multiple related tasks if they share the same expertise

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

FEW-SHOT EXAMPLES (from GEPA optimization):

Example 1 — Single agent, single task (no dependencies):
Input: {"agents":[{"name":"News Analyst","role":"Researcher"}],"tasks":[{"name":"Gather News"}]}
Output: {"assignments": [{"agent_name": "News Analyst", "tasks": [{"task_name": "Gather News", "reasoning": "The News Analyst's role as a Researcher directly aligns with gathering news, which requires research skills to find, collect, and curate relevant news information from various sources."}]}], "dependencies": []}

Example 2 — Single agent, two sequential tasks:
Input: {"agents":[{"name":"Analyst","role":"Data Analyst"}],"tasks":[{"name":"Analyze Data"},{"name":"Write Report"}]}
Output: {"assignments": [{"agent_name": "Analyst", "tasks": [{"task_name": "Analyze Data", "reasoning": "The Analyst is a Data Analyst, making them the perfect fit for analyzing data. This is their core competency."}, {"task_name": "Write Report", "reasoning": "As the only available agent and a Data Analyst, the Analyst writes the report based on their analysis findings."}]}], "dependencies": [{"task_name": "Analyze Data", "depends_on": [], "reasoning": "This is the initial task with no prerequisites."}, {"task_name": "Write Report", "depends_on": ["Analyze Data"], "reasoning": "The report must be written after data analysis is complete, as it documents findings from the analysis."}]}

Example 3 — Two agents, two tasks (role-matched):
Input: {"agents":[{"name":"Researcher","role":"Web Researcher"},{"name":"Writer","role":"Content Writer"}],"tasks":[{"name":"Research Topic"},{"name":"Write Article"}]}
Output: {"assignments": [{"agent_name": "Researcher", "tasks": [{"task_name": "Research Topic", "reasoning": "The Researcher's Web Researcher role is ideally suited for gathering information on the topic from various web sources."}]}, {"agent_name": "Writer", "tasks": [{"task_name": "Write Article", "reasoning": "The Writer's Content Writer role makes them the natural choice for composing the article using the research findings."}]}], "dependencies": [{"task_name": "Research Topic", "depends_on": [], "reasoning": "Research must happen first to provide source material."}, {"task_name": "Write Article", "depends_on": ["Research Topic"], "reasoning": "The article requires research findings as input material."}]}

Only include tasks in the dependencies array if they actually have prerequisites.
Think carefully about the workflow and how information flows between tasks."""

GENERATE_JOB_NAME_TEMPLATE = """Generate a concise, descriptive name (2-4 words) for an AI job run based on the agents and tasks involved.
Focus on the specific domain, region, and purpose of the job.
The name should reflect the main activity (e.g., 'Swiss News Monitor' for a Swiss journalist monitoring news).
Prioritize including:
1. The region or topic (e.g., Switzerland, Zurich)
2. The main activity (e.g., News Analysis, Press Review)
Only return the name, no explanations or additional text.
Avoid generic terms like 'Agent', 'Task', 'Initiative', or 'Collaboration'.

FEW-SHOT EXAMPLES (from GEPA optimization):
- Swiss journalist monitoring news, task: gather Swiss news → Swiss News Monitor
- Financial analyst, tasks: analyze AAPL stock, recommend investments → AAPL Stock Analysis
- Support agent, task: categorize support tickets → Support Ticket Categorization
- Content writer, task: write ML blog posts → ML Blog Writing
- Recruiter, tasks: find ML candidates, score, outreach → ML Talent Search
- Marketing strategist, tasks: competitive analysis, campaign strategy → Marketing Campaign Strategy
- An agent that scrapes websites and builds dashboards → Web Dashboard Builder
- Oil price monitor with email notification → Oil Price Monitor"""

GENERATE_TASK_TEMPLATE = """You are an expert at designing AI task configurations. Generate ONE task as a single valid JSON object — no markdown, no commentary, double quotes, no trailing commas — with EXACTLY these fields:
{"name": "concise, descriptive name", "description": "what the task does — context, objectives, methodology (>= 20 words)", "expected_output": "specific deliverable — sections, structure, quality standards (>= 15 words)", "tools": [], "llm_guardrail": {"description": "validation criteria aligned with expected_output"}}
Omit every other field — the platform fills defaults (async_execution, retries, priority, dependencies, etc.). Do NOT set output_file, output_json, or output_pydantic.

QUALITY:
- description: >= 20 words, detailed (context, objectives, methodology). expected_output: >= 15 words, specific about content and structure. No placeholders like "TBD"/"N/A".
- llm_guardrail.description: write one for EVERY task, aligned with expected_output, answering "what makes this task's output valid and complete?" (e.g. "Must contain clear methodology, data-backed findings, and actionable recommendations.").

TOOLS — assign at most 1-2, ONLY from the "Available tools" list provided at the end of this prompt; never invent names; if none are listed use []:
- PREFER internal/organizational data tools (e.g. GenieTool) when the task uses the user's OWN data (campaigns, metrics, reports, KPIs, employees, products…).
- Use web tools ONLY for external/public info (industry trends, competitor research, general knowledge). If you assign SerperDevTool you MUST also assign ScrapeWebsiteTool.
- Research/data-gathering tasks ALWAYS get the relevant tools. Tasks that only write/compose/summarize/review, or that create a presentation/dashboard from already-gathered data, need []. A single task that must BOTH gather data AND compose the deliverable KEEPS its tools.

DELIVERABLE: describe the final output by CONTENT and STRUCTURE only (sections, slides with headings/points, KPI tiles, chart data, table rows, quiz questions) — never HTML, CSS, JavaScript, or downloadable files. Density: presentation slides carry 3-5 full-sentence points; dashboards present multiple KPIs with values/deltas plus charts and a data table. Research/gathering tasks that precede the final task keep normal text output.

EXAMPLE:
User: "analyze sales data and create a dashboard"
Output: {"name": "Sales Analysis Dashboard", "description": "Query and analyze sales performance data, then organize the findings into a metrics dashboard: KPI tiles for the headline numbers, plus charts for trends and a table of the underlying rows.", "expected_output": "A metrics dashboard: at least 4 KPI tiles with values and deltas, one or more charts for trends/breakdowns, and a data table of the key rows.", "tools": ["GenieTool"], "llm_guardrail": {"description": "Must present at least 4 KPI tiles with values, at least one chart, and a data table; reject if sparse."}}
(A pure writing/composition task would instead carry "tools": []; a public-web research task would carry ["SerperDevTool", "ScrapeWebsiteTool"].)"""

GENERATE_TEMPLATES_TEMPLATE = """You are an expert at creating AI agent templates following CrewAI and LangChain best practices.
Given an agent's role, goal, and backstory, generate three templates that work together cohesively:

1. System Template: Defines the agent's core identity using {role}, {goal}, and {backstory} parameters
2. Prompt Template: Structures how tasks are presented, including placeholders like {input} and {context}
3. Response Template: Guides response formatting with structured sections for consistency

TEMPLATE REQUIREMENTS (CRITICAL — all three must meet these):
- System Template MUST use ALL THREE parameters: {role}, {goal}, and {backstory} — each must appear literally
- Prompt Template MUST use {input} parameter — this is required. {context} is optional but recommended
- Response Template MUST have structured sections with clear labels (e.g., THOUGHTS, ACTION, RESULT or ANALYSIS, FINDINGS, RECOMMENDATIONS)
- Each template must be substantial — at least 2 sentences, not just a placeholder
- Include proper placeholder syntax with curly braces for dynamic content
- Ensure templates establish expertise boundaries and ethical guidelines
- Make templates model-agnostic and production-ready

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
}

FEW-SHOT EXAMPLES (from GEPA optimization — improved baseline from 80% to 100%):

Example 1 — Financial Analyst:
Input: "Role: Financial Analyst, Goal: Analyze stock performance, Backstory: 10 years equity research"
Output: {"system_template": "You are a {role}. Your goal is to {goal}. Background: {backstory}. You approach every analysis with rigor, data-driven insights, and attention to market dynamics. Provide thorough financial analysis with clear reasoning and evidence-based conclusions.", "prompt_template": "Context: {context}\\n\\nTask: {input}\\n\\nPlease analyze this thoroughly, considering relevant financial metrics, market conditions, and risk factors. Provide your professional assessment.", "response_template": "ANALYSIS:\\n[Detailed reasoning and methodology used in the analysis]\\n\\nFINDINGS:\\n[Key insights, metrics, and observations from the data]\\n\\nRECOMMENDATION:\\n[Actionable conclusions and suggested course of action based on the analysis]"}

Example 2 — Content Writer:
Input: "Role: Content Writer, Goal: Write engaging blog posts, Backstory: Published author and SEO expert"
Output: {"system_template": "You are a {role}. Your goal is to {goal}. Background: {backstory}. You combine creative writing with strategic SEO optimization. Every piece of content you create is engaging, well-researched, and optimized for both readers and search engines.", "prompt_template": "Context: {context}\\n\\nTask: {input}\\n\\nPlease create content that is engaging, well-structured, and optimized for the target audience. Consider SEO best practices, readability, and the overall narrative flow.", "response_template": "CONTENT STRATEGY:\\n[Brief outline of the approach, target audience, and key themes]\\n\\nDRAFT:\\n[The complete content piece with proper formatting, headings, and structure]\\n\\nSEO NOTES:\\n[Keyword recommendations, meta description suggestion, and optimization tips]"}

Example 3 — Minimal input (edge case):
Input: "Role: Helper, Goal: Help, Backstory: Helpful"
Output: {"system_template": "You are a {role}. Your goal is to {goal}. Background: {backstory}. You are a versatile problem-solver who adapts your approach to each unique situation, providing clear, practical, and well-organized assistance.", "prompt_template": "Context: {context}\\n\\nTask: {input}\\n\\nPlease complete this task thoroughly. Break down complex problems into manageable steps and provide clear, actionable results.", "response_template": "THOUGHTS:\\n[Analysis of the task requirements and approach]\\n\\nACTION:\\n[Steps taken and methodology applied]\\n\\nRESULT:\\n[Final output with clear formatting and actionable conclusions]"}"""

GENERATE_CREW_PLAN_TEMPLATE = """You are an expert at planning AI crews. Produce a PLAN OUTLINE only — the skeleton of agents and tasks; descriptions, goals, backstories, and tools are generated separately.

VERB-TO-TASK MAPPING (CRITICAL):
Count the distinct action verbs in the user's message. Each distinct verb typically maps to one task:
- 1 verb = 1 task ("summarize this document" → 1 task)
- 2 verbs = 2 tasks ("create a dashboard AND send an email" → 2 tasks)
- 3+ verbs = match the verb count up to the stated maximum
When verbs are closely related sub-steps of one action (e.g., "extract, transform, and load" = ETL), they MAY be combined into a single task. Use the minimum number of agents needed to cover the tasks.

OUTPUT — respond with ONLY this JSON shape (no markdown, no commentary):
{"complexity": "light|standard|complex", "process_type": "sequential|parallel", "agents": [{"name": "...", "role": "..."}], "tasks": [{"name": "...", "assigned_agent": "...", "context": []}]}

Rules:
1. Every task's assigned_agent must be the name of one of the agents.
2. A task's context lists the names of earlier tasks whose output it needs (empty list if none).
3. Names are short and descriptive; roles are one specialised sentence fragment.
4. Do NOT include descriptions, goals, backstories, or tools."""

GENERATE_CREW_TEMPLATE = """You are an expert at creating AI crews. From the user's goal, generate specialized agents and well-defined tasks. Each task is assigned to one agent and may depend on earlier tasks.

VERB-TO-TASK MAPPING: count the distinct action verbs in the user's message; each verb typically maps to one task (closely-related sub-steps like "extract, transform, load" may combine into one). Examples: "write a blog post" -> 1 task; "research competitors and write a summary" -> 2 tasks; "gather news, summarize findings, create a presentation" -> 3 tasks.

LIMITS: at most 3 agents and 6 tasks unless the user explicitly asks for more (hard cap 10 agents / 10 tasks). Use the minimum agents needed; the number of agents must NEVER exceed the number of tasks; every agent MUST be assigned at least one task (no orphan agents).

TOOLS:
- ONLY use tools from the provided tools list, and return tool names EXACTLY as listed. Do not invent tools.
- Research / data-gathering tasks ALWAYS get the relevant tools (they fetch data); if you assign SerperDevTool you MUST also assign ScrapeWebsiteTool.
- The final task that composes the deliverable gets tools: [] ONLY IF earlier tasks already gathered the data; a single task that must BOTH gather and compose KEEPS its tools.

OUTPUT: respond with ONLY a valid JSON object — no markdown, no commentary, double quotes, no trailing commas — with exactly these fields and no others:
{
  "agents": [
    {"name": "descriptive name", "role": "specific role title", "goal": "clear objective", "backstory": "relevant experience and expertise", "tools": []}
  ],
  "tasks": [
    {"name": "descriptive name", "description": "detailed description", "expected_output": "specific deliverable", "assigned_agent": "<one of the agent names>", "context": ["<names of earlier tasks whose output this needs>"], "tools": [], "llm_guardrail": {"description": "validation criteria aligned with expected_output"}}
  ]
}
Omit every other field — the platform fills sane defaults (llm, max_iter, cache, async_execution, etc.). Do NOT set output_file, output_json, or output_pydantic. Write a task-specific llm_guardrail.description for EVERY task answering "what makes this task's output valid and complete?" (e.g. a research task: "includes >=3 credible sources, separates facts from opinions, gives specific data points").

OUTPUT FORMAT: describe the final deliverable by CONTENT and STRUCTURE only (slides with headings and points, KPI tiles, chart data, table rows, quiz questions) — format-neutral. Never mention HTML, CSS, JavaScript, or downloadable files. Aim for substantive density: presentation slides carry 3-5 full-sentence points; dashboards present multiple KPIs with values/deltas plus charts and a data table. Research / gathering tasks that come BEFORE the final task keep normal text output (reports, summaries, data)."""

DETECT_INTENT_TEMPLATE = """You are an intent detection system for a CrewAI workflow designer.

DEFAULT: the intent is "generate_crew" at confidence 0.95. A crew can hold a single agent with a single task, so it is the safe, flexible choice. Only choose another intent on EXPLICIT evidence — and a message with 2+ distinct action verbs is ALWAYS generate_crew.

Choose a non-default intent ONLY when:
1. generate_agent — the message explicitly creates ONE agent and uses "agent"/"bot"/"assistant"/"chatbot" as the entity created, with no other action verb (e.g. "create an agent that analyzes data"). Role words like expert/analyst/specialist are NOT agents.
2. generate_task — the message explicitly creates a task ("create a task", "add a task"); the word "task" must be the entity created.
3. execute_crew — "execute", "run", "start", "launch", or "ec".
4. configure_crew — change LLM/model, max rpm, tools, or settings ("configure", "change model", "select tools", "update max rpm").
5. catalog/flow ops — list, load, save, schedule, or delete plans/flows/crews.
Everything else (research, analysis, reporting, retrieval, comparison, multi-step or goal-oriented requests) is generate_crew.

ACTION VERBS: scan the WHOLE message and list every distinct action verb in "action_words" — each verb usually becomes a separate task. E.g. "create a dashboard and send an email" -> ["create","send"]; "analyze feedback, identify trends, write recommendations" -> ["analyze","identify","write"].

Return ONLY this JSON object (double quotes, no markdown, no trailing commas):
{
    "intent": "generate_task" | "generate_agent" | "generate_crew" | "execute_crew" | "configure_crew" | "unknown",
    "confidence": 0.0-1.0,
    "extracted_info": {
        "action_words": ["all", "distinct", "verbs"],
        "entities": ["objects", "or", "entities"],
        "goal": "what the user wants to accomplish",
        "config_type": "llm|maxr|tools|general"  // only for configure_crew
    },
    "suggested_prompt": "cleaned, enhanced version of the request"
}

Examples:
- "get the latest news from switzerland" -> generate_crew
- "research competitors and write a summary" -> generate_crew
- "gather news, create a presentation, and email the team" -> generate_crew
- "build a team to handle customer support" -> generate_crew
- "create an agent that analyzes data" -> generate_agent
- "make me a chatbot for support" -> generate_agent
- "create a task to check server status" -> generate_task
- "run crew" / "ec" -> execute_crew
- "change model" / "select tools" / "update max rpm" -> configure_crew
"""

IMPROVE_PROMPT_TEMPLATE = """You are an expert prompt engineer. You improve the prompt fields of AI agent and task configurations so they produce better LLM results.

You receive a JSON object with:
- "target": what is being improved — "agent" (role/goal/backstory), "task" (description/expected_output), "template", or "chat" (a free-form user request typed into the chat)
- "fields": the current prompt field texts to improve
- "instructions": optional user guidance for the rewrite (may be null)

Rewrite every field in "fields" applying prompt-engineering best practices:
1. PRESERVE INTENT — never change what the prompt is for; sharpen and clarify it.
2. BE SPECIFIC — replace vague language with concrete actions, methods, and success criteria.
3. MEASURABLE OUTPUTS — expected outputs must state the deliverable's structure, sections, and quality standards a reviewer can check.
4. NO INVENTION — do not add tools, URLs, data sources, or facts that are not implied by the original text.
5. KEEP PLACEHOLDERS — any {variable} placeholders must be preserved exactly as written.
6. RIGHT-SIZED — role: a short specific title; goal: 1-2 sentences containing an action verb; backstory: 1-3 sentences (10-60 words) of relevant professional expertise; task description: >= 20 words covering context, objective, and method; expected_output: >= 15 words naming the deliverable and its structure.
7. COHERENT SET — the improved fields must read as one consistent configuration, not independent rewrites.
8. Follow "instructions" when present, as long as they don't conflict with rules 1-5.

If a field is already strong, refine it lightly rather than rewriting for its own sake.

For target "chat" the field is the user's own request to an AI workflow assistant: keep it written as a first-person request (not an agent/task config), sharpen what is being asked for, name the desired deliverable and its structure, and keep it to a few sentences. Never answer the request — only rewrite it.

Return ONLY a single valid JSON object with EXACTLY the same keys as "fields" and the improved text as values — double quotes, no markdown, no commentary, no trailing commas.

Example
Input: {"target": "agent", "fields": {"role": "helper", "goal": "help with data", "backstory": "knows data"}, "instructions": null}
Output: {"role": "Data Analysis Specialist", "goal": "Analyze the provided datasets to surface trends, anomalies, and key metrics, then deliver clear, decision-ready summaries", "backstory": "Seasoned data analyst with deep experience in exploratory analysis, statistics, and business reporting, known for turning messy data into concise, actionable insights."}

Example
Input: {"target": "task", "fields": {"description": "check the website", "expected_output": "a report"}, "instructions": "focus on accessibility"}
Output: {"description": "Review the website's pages for accessibility issues: evaluate semantic structure, color contrast, keyboard navigation, alt text coverage, and form labeling, noting each issue with its location and severity.", "expected_output": "An accessibility review report with an issue table (location, WCAG criterion, severity, recommended fix), a summary of the most critical problems, and prioritized remediation steps."}

Example
Input: {"target": "chat", "fields": {"message": "make me something about sales"}, "instructions": null}
Output: {"message": "Create a dashboard of our sales performance for the last quarter: headline KPI tiles (revenue, units sold, average deal size, win rate), a monthly revenue trend chart, and a breakdown table by region and product line."}"""

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
        "name": "generate_crew_plan",
        "description": "Lightweight template for the crew PLAN OUTLINE phase (skeleton only)",
        "template": GENERATE_CREW_PLAN_TEMPLATE,
        "is_active": True
    },
    {
        "name": "detect_intent",
        "description": "Template for detecting user intent in natural language messages",
        "template": DETECT_INTENT_TEMPLATE,
        "is_active": True
    },
    {
        "name": "improve_prompt",
        "description": "Template for improving agent/task prompt fields with prompt-engineering best practices",
        "template": IMPROVE_PROMPT_TEMPLATE,
        "is_active": True
    },
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