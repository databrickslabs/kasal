# CONTRIBUTING.md

## Welcome to Kasal

Kasal is an AI agent workflow orchestration platform that transforms complex AI orchestration into an intuitive visual experience. This guide will help you contribute effectively to our enterprise-ready platform.

## Quick Start for Contributors

### Prerequisites
- **Python 3.9+** for backend development
- **Node.js 16+** for frontend development (optional)
- **Git** for version control

### 5-Minute Setup
```bash
# Clone and setup
git clone <repository-url>
cd kasal

# Backend setup (required)
source venv/bin/activate  # From project root
cd src/backend
./run.sh sqlite  # SQLite for development

# Frontend setup (optional - only if working on UI)
cd src/frontend
npm install
npm start  # http://localhost:3000
```

**API Access**: Backend runs at http://localhost:8000

## Architecture Overview

Kasal follows a clean layered architecture:

```
Visual Workflow Designer (React) → FastAPI → Agentic Engine → Database
```

### Key Characteristics
- **AI-First Platform**: Framework-agnostic agentic orchestration with CrewAI implementation and extensible engine architecture
- **Clean Architecture**: Repository → Service → API pattern with clear separation of concerns
- **Enterprise Ready**: Built for Databricks deployment with OAuth and production-grade patterns
- **Type-Safe**: Full TypeScript frontend, Python type hints throughout backend

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
├── engines/crewai/  # AI agent implementation
├── models/          # SQLAlchemy database models
├── schemas/         # Pydantic validation schemas
└── core/            # Dependencies, logging, Unit of Work
```

### Frontend (`src/frontend/src/`)
```
├── components/      # React components by feature
├── store/           # Zustand state management
├── api/             # API service layer
└── types/           # TypeScript definitions
```

### Documentation (`src/docs/`)
All project documentation including architecture guides, best practices, and deployment instructions.

## Development Workflow

### 1. Before You Start
- Read `src/docs/ARCHITECTURE.md` and `src/docs/BEST_PRACTICES.md`
- Understand this is an **agentic orchestration platform** - familiarize yourself with AI agent concepts
- Review existing code in the area you plan to work on

### 2. Development Process

**Backend Development:**
```bash
# Activate environment
source venv/bin/activate  # From project root

# Start development server
cd src/backend
./run.sh sqlite  # or ./run.sh for PostgreSQL

# Code quality (run before committing)
python -m black src tests && python -m isort src tests
python -m mypy src
python -m flake8 src tests
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
python run_tests.py  # All tests
python run_tests.py --coverage --html-coverage  # With coverage report
```

### 3. Critical Development Standards

**Testing Requirements:**
- **80%+ test coverage** mandatory
- Write tests alongside implementation
- Unit tests for individual components
- Integration tests for workflows
- Mock external dependencies (LLMs, databases)

**Code Quality Standards:**
- **Backend**: Black formatting, isort imports, mypy type checking, flake8 linting
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
python run_tests.py

# Specific test types
python run_tests.py --type unit
python run_tests.py --type integration

# With coverage reporting
python run_tests.py --coverage --html-coverage
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
- **Always activate venv**: `source venv/bin/activate` from project root
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
1. **`src/docs/ARCHITECTURE.md`** - System architecture and patterns
2. **`src/docs/GETTING_STARTED.md`** - Detailed setup instructions
3. **`src/docs/BEST_PRACTICES.md`** - Development guidelines
4. **`src/docs/CREWAI_ENGINE.md`** - AI engine specifics (if working on agents)

### When Contributing
- Update relevant documentation for new features
- Add docstrings for all public APIs
- Update `src/docs/` files as needed (they auto-sync to frontend)

## Contribution Checklist

Before submitting your contribution:

- [ ] **Setup**: Development environment working correctly
- [ ] **Architecture**: Follows established Repository → Service → API pattern
- [ ] **Testing**: 80%+ test coverage with meaningful tests
- [ ] **Code Quality**: Passes Black, isort, mypy, flake8
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