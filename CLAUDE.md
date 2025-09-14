# CLAUDE.md

Project-wide instructions for Claude Code (claude.ai/code) when working with the Kasal codebase.

## Context Layering

Claude reads context from multiple CLAUDE.md files:
- **This file**: Project-wide patterns and rules
- **src/backend/CLAUDE.md**: Backend-specific instructions
- **src/frontend/CLAUDE.md**: Frontend-specific instructions

## Important Project Rules

### Dependencies
- **Main requirements file is at `src/requirements.txt`** (not backend/requirements.txt)
- Install dependencies: `pip install -r src/requirements.txt`
- Key dependencies include: psutil (for process management), crewai, litellm, databricks-sdk

### Documentation Location
- **ALWAYS create documentation in `src/docs/` directory**
- Do not create docs in the root `docs/` folder
- Frontend copies from `src/docs/` to `public/docs/` for display
- Follow existing documentation patterns and naming conventions

### Test Files Location
- **ALWAYS create test scripts and temporary files in `/tmp` folder**
- Do not create test files in the project directory
- Use paths like `/tmp/test_script.py` for testing
- This keeps the project directory clean

### Service Management
- **DO NOT restart backend or frontend services** - They are managed externally
- Backend uses `--reload` flag and auto-detects code changes
- Frontend uses hot module replacement (HMR) and auto-updates in browser
- Check service status: `ps aux | grep uvicorn` (backend) or `ps aux | grep "npm start"` (frontend)

### Code Quality Standards
- **CRITICAL: All operations must be async and non-blocking**
- **CRITICAL: Never include real URLs, endpoints, or addresses in code**
- Always use placeholder values like "https://example.com" or environment variables
- Follow clean architecture principles
- Never commit without running linting tools

### Build and Deploy
- **Build frontend static assets**: `python src/build.py`
- **Deploy application**: `python src/deploy.py`

## Architecture Overview

Kasal is an AI agent workflow orchestration platform with a **clean architecture pattern**:

**Frontend (React + TypeScript)** → **API (FastAPI)** → **Services** → **Repositories** → **Database**

### Technology Stack
- **Backend**: FastAPI + SQLAlchemy 2.0 + Alembic (Python 3.9+)
- **Frontend**: React 18 + TypeScript + Material-UI + ReactFlow
- **AI Engine**: CrewAI framework for agent orchestration
- **Database**: SQLite (dev) / PostgreSQL (prod)
- **Authentication**: JWT tokens with Databricks OAuth

### Project Structure
```
src/
├── backend/                  # FastAPI backend (see backend/CLAUDE.md)
│   ├── src/                 # Core application code
│   ├── tests/               # Unit and integration tests
│   └── migrations/          # Database migrations
├── frontend/                # React frontend (see frontend/CLAUDE.md)
│   └── src/                 # React application
└── frontend_static/         # Built frontend assets
```

## Development Workflow

### Quick Start
1. **Backend**: `cd src/backend && ./run.sh` (auto-reloads on changes)
2. **Frontend**: `cd src/frontend && npm start` (hot module replacement)
3. **Tests**: See respective CLAUDE.md files for testing commands

### Key Principles
- **Clean Architecture**: Separation of concerns across layers
- **Async-First**: All I/O operations must be async
- **Type Safety**: Strong typing in both backend (mypy) and frontend (TypeScript)
- **Test Coverage**: Minimum 80% for backend, comprehensive frontend testing

## Special Considerations

### Memory and Persistence
- CrewAI crews generate deterministic IDs for memory persistence
- Group isolation ensures tenant data separation
- Databricks Vector Search integration for advanced memory backends

### Model Integration
- Support for multiple LLM providers (Databricks, OpenAI, Anthropic, etc.)
- Model configurations in `src/backend/src/seeds/model_configs.py`
- Automatic handling of provider-specific requirements

### Databricks Apps Integration
- **When searching for Databricks Apps information, always check first**: https://apps-cookbook.dev/docs/streamlit/authentication/users_obo
- This reference covers authentication patterns and user on-behalf-of (OBO) flows

## MCP Knowledge Graph Integration

### Purpose
The MCP Knowledge Graph provides persistent memory and relationship tracking across development sessions. It maintains project context, architectural decisions, and component relationships in a structured knowledge base.

### **CRITICAL REQUIREMENT**: Always Query First
- **MUST use `aim_search_nodes()` or `aim_read_graph()` BEFORE any operation**
- **MUST query existing knowledge before making changes to understand context**
- **MUST update knowledge graph when code changes occur using `aim_add_observations()`**
- **Never work without first consulting the knowledge graph**

### Key Features
- **Entity Management**: Track components, decisions, requirements, and technical concepts
- **Relationship Mapping**: Connect entities with typed relations (depends_on, implements, requires, etc.)
- **Context Separation**: Organize knowledge by domain (architecture, frontend, backend, ai, infrastructure)
- **Session Persistence**: Knowledge persists across development sessions
- **Search & Discovery**: Query-based search for entities and relationships

### Usage Patterns for Kasal

#### Architectural Decisions
```
aim_create_entities({
  context: "architecture",
  entities: [{
    name: "CleanArchitecture",
    entityType: "pattern",
    observations: ["Separation of concerns", "FastAPI -> Services -> Repositories -> Database"]
  }]
})
```

#### Component Relationships
```
aim_create_relations({
  context: "architecture",
  relations: [{
    from: "ProcessCrewExecutor",
    to: "CrewAI",
    relationType: "integrates_with"
  }]
})
```

#### Requirements Tracking
```
aim_create_entities({
  context: "requirements",
  entities: [{
    name: "AsyncOperations",
    entityType: "requirement",
    observations: ["All I/O must be async", "Non-blocking operations required"]
  }]
})
```

### Best Practices
- **Before Sessions**: Use `aim_read_graph()` to understand current project state
- **During Development**: Add entities for new components with `aim_create_entities()` and their relationships with `aim_create_relations()`
- **After Sessions**: Update observations with `aim_add_observations()` for progress and learnings
- **Context Organization**: Use separate contexts (architecture, frontend, backend, ai, requirements)
- **Relationship Types**: Use consistent relation types (implements, depends_on, integrates_with, requires)

### **MANDATORY Development Workflow with Knowledge Graph**

#### **Every Session MUST Follow This Pattern:**
1. **Session Start**:
   - `aim_read_graph({context: "architecture"})` - Load project state
   - `aim_search_nodes({query: "relevant_component"})` - Find related components
   - Review existing entities and relationships BEFORE making changes

2. **Before Any Code Change**:
   - Query knowledge graph for affected components using `aim_search_nodes()`
   - Understand existing relationships and dependencies
   - Plan changes considering architectural impact

3. **During Development**:
   - Create entities for new components: `aim_create_entities()`
   - Map relationships: `aim_create_relations()`
   - Update observations: `aim_add_observations()`
   - Document decisions: Add architectural reasoning to observations

4. **After Code Changes**:
   - Update entity observations with `aim_add_observations()` for what was changed
   - Create/update relationships for new dependencies using `aim_create_relations()`
   - Document impact on existing components

5. **Session End**:
   - Final knowledge graph update with progress using `aim_add_observations()`
   - Document learnings and decisions made
   - Update component status and relationships

### **Knowledge Graph Maintenance Rules**

#### **When Code Changes Occur - MUST Update Graph:**
1. **New Files/Components**: Create corresponding entities immediately
2. **Deleted Components**: Remove entities and relationships
3. **Modified Components**: Update observations with changes
4. **New Dependencies**: Create new relationships
5. **Architectural Decisions**: Document reasoning in observations

#### **Search Patterns Before Operations:**
```
# Before working on authentication:
aim_search_nodes({context: "backend", query: "auth"})

# Before modifying a service:
aim_search_nodes({context: "backend", query: "ServiceName"})

# Before UI changes:
aim_search_nodes({context: "frontend", query: "component_name"})
```

## Memory Contexts and Organization

### **Context Usage Guidelines**
- **architecture**: System design patterns, architectural decisions, cross-cutting concerns
- **backend**: Services, repositories, APIs, database models, business logic
- **frontend**: React components, UI patterns, user workflows, design decisions
- **ai**: CrewAI configurations, agent behaviors, model integrations, AI workflows
- **requirements**: User stories, acceptance criteria, feature specifications

### **Entity Types by Context**
- **architecture**: pattern, requirement, subsystem, principle
- **backend**: service, repository, model, api_endpoint, helper
- **frontend**: react_component, hook, utility, page, layout
- **ai**: agent, crew, model, prompt, workflow
- **requirements**: user_story, acceptance_criteria, feature

For detailed backend instructions, see: **src/backend/CLAUDE.md**
For detailed frontend instructions, see: **src/frontend/CLAUDE.md**
- always make sure whenever you develop anything you need to stick to service architecture pattern, and unit of work architecture pattern and repository architecture pattern.