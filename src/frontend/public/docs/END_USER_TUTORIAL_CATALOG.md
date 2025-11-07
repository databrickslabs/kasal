# Kasal End‑User Tutorial — Build a Blog Workflow (with Catalog)

Audience: End users (marketers, PMs, analysts) creating multi‑agent workflows — no admin setup required

Status: Screenshot‑ready script (placeholders provided); suitable for website docs or in‑product walkthrough

Prerequisites:
- You can access the Kasal app (local or hosted)
- Recommended route: http://localhost:3000/workflow
- You have permissions to create agents, tasks, and crews (default end‑user role)

Contents
- Chapter 1: First Contact — Orientation and first agent
- Chapter 2: Build the Team — Writer and Editor + pipeline
- Chapter 3: First Execution — Run and iterate in Chat
- Chapter 4: Catalog — Discover, import, and customize templates
- Chapter 5: Collaboration — Save, share, and version

Notes for screenshots
- Use images under src/docs/images/. Suggested filenames are included below as placeholders.
- Capture the UI with consistent light/dark theme and window size.

---

## Chapter 1 — First Contact
Scene: Sarah discovers Kasal and wants to automate content creation.

1) Open the Workflow Designer
- UI Path: Left sidebar → Workflows → Designer (or direct route /workflow)
- Expected Result: A blank canvas with left sidebar sections (Agents, Tasks, Crews, Catalog) and a Chat panel.
- Screenshot: images/workflow-designer-blank.png

2) Create your first agent — “Content Researcher”
- Click: Agents → + Add Agent
- Fill:
  - Name: Content Researcher
  - Role: Expert Research Analyst
  - Goal: Identify trending topics and synthesize findings
  - Backstory: You are a seasoned researcher with a B2B focus...
  - Model: Use your organization default or any available model
- Save
- Expected Result: A new Agent node appears on the canvas.
- Screenshot: images/agent-content-researcher-created.png

3) Add your first task — “Topic Research”
- Click: Tasks → + Add Task
- Fill:
  - Title: Topic Research
  - Description: Research 5 trending topics in {industry} and provide 2–3 sentence summaries per topic.
  - Expected Output: A structured list of 5 topics with short summaries.
- Save
- Connect the Task to “Content Researcher” on the canvas (drag connector from Task to Agent if needed).
- Expected Result: Task → Agent connection shown on the canvas.
- Screenshot: images/task-topic-research-connected.png

Callouts
- Tip: Keep task descriptions actionable and specific.
- Tip: Use placeholders like {industry} to parameterize inputs at run time.

---

## Chapter 2 — Build the Team
Scene: Sarah expands the team with Writer and Editor.

4) Add the Writer agent
- Click: Agents → + Add Agent
- Fill:
  - Name: Content Writer
  - Role: Blog Writer
  - Goal: Draft publishable blog posts based on topic briefs
  - Backstory: Skilled at clarity, narrative flow, and brand tone
  - Model: Organization default or writing‑optimized model
- Save
- Expected Result: “Content Writer” agent appears on the canvas.
- Screenshot: images/agent-content-writer-created.png

5) Add the Editor agent
- Click: Agents → + Add Agent
- Fill:
  - Name: Content Editor
  - Role: Editor & SEO Specialist
  - Goal: Improve grammar, clarity, structure, and basic SEO
  - Backstory: Detail‑oriented editor with web publishing experience
  - Model: Organization default
- Save
- Expected Result: “Content Editor” agent appears on the canvas.
- Screenshot: images/agent-content-editor-created.png

6) Create the pipeline tasks and link them
- Add Task: Draft Article
  - Description: Write a blog post (800–1200 words) from the topic research.
  - Expected Output: A well‑structured draft with headings and intro/conclusion.
  - Connect to: Content Writer
- Add Task: Edit & Optimize
  - Description: Improve grammar, clarity, headings, and add basic SEO keywords.
  - Expected Output: Polished, publication‑ready article.
  - Connect to: Content Editor
- Link sequence on canvas: Topic Research → Draft Article → Edit & Optimize
- Expected Result: A 3‑step chain connected to their respective agents.
- Screenshot: images/pipeline-three-tasks-linked.png

Callouts
- Tip: Keep each task focused on one outcome to simplify iteration.
- Tip: Use descriptive names so teammates instantly understand each step.

---

## Chapter 3 — First Execution
Scene: Sarah runs the workflow and iterates quickly.

7) Configure the Crew
- UI Path: Crews → Configure Crew (or “Create Crew” button on the canvas)
- Choose: Sequential process
- Confirm order: Researcher → Writer → Editor
- Optionally define runtime variables: {industry}, {tone}
- Save configuration
- Expected Result: Crew settings reflect the 3‑step sequence.
- Screenshot: images/crew-config-sequential.png

8) Run via Chat
- UI Path: Chat panel
- Prompt: “Create a blog post about sustainable technology in consumer electronics.”
- Action: Click Run / Send
- Observe: Real‑time execution with hand‑offs across agents.
- Expected Result: Final output appears in Chat/Results after all steps complete.
- Screenshot: images/chat-execution-trace.png

9) Iterate and improve
- If tone/structure needs changes, adjust:
  - Agent prompts (Writer/Editor)
  - Task descriptions and expected outputs
- Re‑run with the same Chat prompt or a tweaked one.
- Expected Result: Improved quality on second run.
- Screenshot: images/iteration-improved-output.png

Callouts
- Tip: Small prompt edits can produce large quality differences — change one thing at a time.
- Tip: Keep a stable prompt template once results meet your standard.

---

## Chapter 4 — Catalog (Discover, Import, Customize)
Scene: Sarah jumpstarts production by using Catalog templates.

10) Browse the Catalog
- UI Path: Left sidebar → Catalog
- Explore: Categories like Content, Research, Marketing
- Use: Search and filters (complexity, multi‑agent, approval steps)
- Expected Result: List of ready‑made templates and components.
- Screenshot: images/catalog-browse.png

11) Preview a template
- Select: “Blog Production Pipeline — Medium‑Length Post”
- Review:
  - Included agents (Researcher, Writer, Editor)
  - Task sequence and expected outputs
  - Example inputs and notes
- Expected Result: Template preview modal/page with details.
- Screenshot: images/catalog-template-preview.png

12) Import template to canvas
- Click: Use this template
- Action: The template workflow is added to your current canvas
- Rename: e.g., “Sustainable Tech Blog v1”
- Expected Result: A pre‑wired workflow on the canvas ready to run.
- Screenshot: images/catalog-imported-workflow.png

13) Customize and swap components
- Replace the template’s Writer with your own “Content Writer” agent if desired
- Edit task prompts to match your industry and brand voice
- Validate the sequence and connections
- Expected Result: Tailored template reflecting your team’s preferences.
- Screenshot: images/catalog-customized-workflow.png

14) Parameterize inputs
- Identify variables: {industry}, {brand_voice}, {length}
- Provide sensible defaults or leave them input‑driven at run time
- Save as: Workflow preset for reuse
- Expected Result: Reusable configuration that speeds future runs.
- Screenshot: images/catalog-parameterized-preset.png

15) Run the catalog‑based workflow
- Trigger: Run from Chat or from the workflow action menu
- Compare: Results vs. your scratch‑built pipeline
- Keep: The better workflow — or combine the best parts of both
- Expected Result: Production‑ready output with minimal setup time.
- Screenshot: images/catalog-run-results.png

Callouts
- Tip: Templates are great starting points; always adjust prompts to your brand voice.
- Tip: Save multiple presets for different content types (e.g., short‑form, long‑form, technical).

---

## Chapter 5 — Collaboration & Sharing
Scene: Sarah shares the workflow and builds a repeatable practice.

16) Save, share, and version
- Action: Save workflow configuration
- Share: With teammates (set view/use permissions)
- Version: Create v2 variants for experimentation
- Expected Result: Team can reuse and iterate on a single source of truth.
- Screenshot: images/sharing-and-versioning.png

17) Review execution history
- UI Path: Workflow → History (or Runs/Executions)
- Inspect: Timestamps, prompts, outputs
- Export: Results for publishing or downstream tools
- Expected Result: Clear audit trail and reproducibility.
- Screenshot: images/execution-history.png

Callouts
- Tip: Keep the best run as a benchmark to evaluate future changes.
- Tip: Add brief notes to runs for context (who ran, why, what changed).

---

## Checklist — What you’ve learned
- Create agents and tasks, and connect them into a crew
- Run a sequential multi‑agent pipeline via Chat and iterate
- Discover templates in the Catalog and import them to your canvas
- Customize, parameterize, and save reusable presets
- Share workflows, version them, and review execution history

## Next steps
- Build a “Topic Cluster” workflow (multiple posts per pillar topic)
- Add approval gates (human‑in‑the‑loop) between Draft and Edit
- Create a library of presets for different industries or tones
- Ask an admin to enable any enterprise features you need (quotas, model defaults, etc.)

