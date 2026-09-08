# CONTRIBUTING.md

## Welcome to Kasal

Kasal is an AI agent workflow orchestration platform that transforms complex AI orchestration into an intuitive visual experience. This guide will help you contribute effectively to our enterprise-ready platform.

## Quick Start for Contributors

### Prerequisites
- **Python 3.11** for backend development
- **Node.js 22+** for frontend development (optional)
- **Git** for version control

### 5-Minute Setup
```bash
# Clone and setup
git clone <repository-url>
cd kasal

# Backend setup (required) — Python deps are managed with uv (no requirements.txt)
cd src/backend
uv sync            # install dependencies (creates .venv)
./run.sh sqlite    # SQLite for development (run.sh runs `uv sync` for you)

# Frontend setup (optional - only if working on UI)
cd ../frontend
npm ci
npm start  # http://localhost:3000
```

**API Access**: Backend runs at http://localhost:8000

## Architecture Overview

Kasal follows a clean layered architecture:

```
Visual Workflow Designer (React) → FastAPI → Agentic Engine → Database
```

### Key Characteristics
- **AI-First Platform**: Native Kasal runtime with an optional CrewAI framework adapter
- **Clean Architecture**: Repository → Service → API pattern with clear separation of concerns
- **Enterprise Ready**: Built for Databricks deployment with OAuth and production-grade patterns
- **Typing**: TypeScript checks plus strict mypy diagnostics tracked by a no-new-errors baseline

### Tech Stack
- **Backend**: FastAPI, SQLAlchemy 2.0, CrewAI, pytest
- **Frontend**: React 18, TypeScript, Zustand, Material-UI, ReactFlow
- **Database**: SQLite (dev), PostgreSQL (prod) with Alembic migrations

## Key Directories for Contributors

### Backend (`src/backend/src/`)
```
├── api/             # FastAPI route handlers (controllers)
├── services/        # Business logic layer (main work area)
├── repositories/    # Data access layer (Repository pattern)
├── services/execution/  # Shared kernel and Kasal/CrewAI framework adapters
├── models/          # SQLAlchemy database models
├── schemas/         # Pydantic validation schemas
└── core/            # Dependencies, logging, base service/repository
```

### Frontend (`src/frontend/src/`)
```
├── app/             # Application shell and session workspace
├── features/        # Chat, workflow canvas, catalog, activity and configuration
├── shared/          # Reusable UI, API client and types
└── store/           # Zustand state management
```

### Documentation (`src/docs/`)
All project documentation including architecture guides, best practices, and deployment instructions.

Backend typing debt and dependency exposure are documented in [Validation and security](src/docs/VALIDATION_AND_SECURITY.md). Run `uv run mypy src` to see the full strict typing report; a passing baseline gate does not mean that report is clean.

## Development Workflow

### 1. Before You Start
- Read `src/docs/ARCHITECTURE_GUIDE.md` and `src/docs/DEVELOPER_GUIDE.md`
- Understand this is an **agentic orchestration platform** - familiarize yourself with AI agent concepts
- Review existing code in the area you plan to work on

### 2. Development Process

**Backend Development:**
```bash
# Start development server (run.sh runs `uv sync` for you)
cd src/backend
./run.sh sqlite  # or ./run.sh for PostgreSQL

# Code quality (run before committing)
uv run black src tests && uv run isort src tests
uv run python check_types.py  # reject new errors; known debt is reported
uv run ruff check src tests run_tests.py check_types.py
uv run lint-imports
```

**Database Changes:**
```bash
# Create migration for model changes
cd src/backend
alembic revision --autogenerate -m "description"
alembic upgrade head
```

**Testing (Required):**
```bash
cd src/backend
uv run python run_tests.py  # All tests
uv run python run_tests.py --coverage --html-coverage  # With coverage report
```

### 3. Critical Development Standards

**Testing Requirements:**
- **80%+ test coverage** mandatory
- Write tests alongside implementation
- Unit tests for individual components
- Integration tests for workflows
- Mock external dependencies (LLMs, databases)

**Code Quality Standards:**
- **Backend**: Black formatting, isort imports, mypy baseline checking, Ruff linting
- **Frontend**: TypeScript strict mode, ESLint
- **Architecture**: Follow Repository → Service → API pattern
- **Async/Await**: All database operations must be async

## Key Development Patterns

### Backend Patterns You Must Follow

**Repository Pattern:**
```python
class AgentRepository(BaseRepository[Agent]):
    async def get_by_name(self, name: str) -> Optional[Agent]:
        query = select(Agent).where(Agent.name == name)
        result = await self.session.execute(query)
        return result.scalars().first()
```

**Service Layer:**
```python
class AgentService(BaseService[Agent, AgentRepository]):
    async def create_agent(self, agent_data: AgentCreate) -> Agent:
        # Business logic here
        return await self.repository.create(agent_data)
```

**Dependency Injection:**
```python
# Use FastAPI's DI system
async def create_agent(
    agent_data: AgentCreate,
    agent_service: AgentService = Depends(get_agent_service)
):
    return await agent_service.create_agent(agent_data)
```

### Frontend Patterns

**Custom Hooks:**
```typescript
export const useAgents = () => {
  const [agents, setAgents] = useState<Agent[]>([]);
  const [loading, setLoading] = useState(false);
  
  const fetchAgents = useCallback(async () => {
    // API logic
  }, []);
  
  return { agents, loading, fetchAgents };
};
```

**Zustand State:**
```typescript
interface WorkflowState {
  nodes: Node[];
  addNode: (node: Node) => void;
}

export const useWorkflowStore = create<WorkflowState>((set) => ({
  nodes: [],
  addNode: (node) => set((state) => ({ nodes: [...state.nodes, node] })),
}));
```

## Testing Strategy

### Backend Testing (80%+ Coverage Required)
```bash
# Run all tests
uv run python run_tests.py

# Specific test types
uv run python run_tests.py --type unit
uv run python run_tests.py --type integration

# With coverage reporting
uv run python run_tests.py --coverage --html-coverage
```

**Test Structure:**
- **Unit Tests**: Individual components in isolation
- **Integration Tests**: Component interactions and full workflows
- **Mocking**: Mock external dependencies (LLM providers, external APIs)
- **Fixtures**: Use pytest fixtures for common test data

### Frontend Testing
```bash
cd src/frontend
npm test        # Jest + React Testing Library
npm run test:e2e # Cypress for end-to-end testing
```

## Common Gotchas & Important Notes

### Critical Requirements
- **Dependencies via uv**: run `uv sync` in `src/backend` (uv manages the `.venv`); there is no `requirements.txt`. Prefix tools with `uv run`
- **Database migrations**: Required for any model changes
- **Type safety**: Use TypeScript/Python type hints extensively
- **Async operations**: All database calls must be async
- **Clean architecture**: Never bypass the Repository → Service → API pattern

### Development Tips
- **SQLite for development**, PostgreSQL for production (automatic switch)
- **Frontend is optional** for backend-only contributions
- **CrewAI knowledge helpful** but not required - focus on the abstractions
- **Visual workflow designer** is core to user experience
- **Enterprise patterns** - code must be production-ready for Databricks

## Documentation Requirements

### Must Read Before Contributing
1. **`src/docs/ARCHITECTURE_GUIDE.md`** - System architecture and patterns
2. **`src/docs/DEVELOPER_GUIDE.md`** - Detailed setup and extension patterns
3. **`src/docs/CODE_STRUCTURE_GUIDE.md`** - Where things live and how to navigate the repo
4. **`src/docs/crewai-engine-refactor-proposal.md`** - CrewAI engine layout (if working on agents)

### When Contributing
- Update relevant documentation for new features
- Add docstrings for all public APIs
- Update `src/docs/` files as needed (they auto-sync to frontend)

## Contribution Checklist

Before submitting your contribution:

- [ ] **Setup**: Development environment working correctly
- [ ] **Architecture**: Follows established Repository → Service → API pattern
- [ ] **Testing**: 80%+ test coverage with meaningful tests
- [ ] **Code Quality**: Passes Black, isort, Ruff and the mypy no-new-errors gate
- [ ] **Database**: Includes Alembic migrations for model changes
- [ ] **Documentation**: Updates relevant docs and includes docstrings
- [ ] **Type Safety**: Full type hints in Python, strict TypeScript
- [ ] **Async**: All database operations use async/await
- [ ] **Error Handling**: Comprehensive error handling implemented

## Getting Help

### Key Resources
- **Documentation**: Check `src/docs/` for comprehensive guides
- **Code Examples**: Look at existing implementations in similar areas
- **Architecture Questions**: Review `ARCHITECTURE.md` and existing patterns
- **Testing**: See `tests/` directory for examples

### Understanding the Domain
This is an **agentic orchestration platform** - you're building tools that help users create, manage, and monitor autonomous AI agents. The visual workflow designer, agent configurations, and execution monitoring are core to the user experience.

## Deployment & Production

Contributors should understand:
- **Target Platform**: Databricks Apps
- **Authentication**: OAuth integration
- **Scalability**: Enterprise-grade patterns
- **Monitoring**: Execution tracking and performance insights

Deploy command: `python src/deploy.py --app-name <name> --user-name <email>`

---

**Welcome to the team!** Kasal is building the future of AI agent orchestration. Your contributions help make sophisticated AI workflows accessible to everyone.