# Kasal Developer Documentation

> **Build AI Workflows in 30 Seconds** - Production-ready from the start

---

## Quick Start

### Install & Run
```bash
# Clone and setup
git clone https://github.com/youorg/kasal
cd kasal && pip install -r src/requirements.txt

# Start services
cd src/backend && ./run.sh  # Backend: http://localhost:8000
cd src/frontend && npm start # Frontend: http://localhost:3000
```

### Your First AI Agent (30 seconds)
```python
from kasal import Agent, Task, Crew

# Create agent
researcher = Agent(
    role="Research Analyst",
    goal="Find market insights",
    model="gpt-4"
)

# Define task
task = Task(
    description="Analyze competitor pricing",
    agent=researcher
)

# Run workflow
crew = Crew(agents=[researcher], tasks=[task])
result = crew.kickoff()
```

**That's it!** Your AI agent is running.

---

## Architecture

### Clean Architecture Pattern
```
Frontend → API → Service → Repository → Database
   ↓        ↓       ↓          ↓           ↓
React   FastAPI  Business  Data Layer  PostgreSQL
```

### Tech Stack
| Layer | Technology | Purpose |
|-------|------------|---------|
| **Frontend** | React 18 + TypeScript | Interactive UI |
| **API** | FastAPI + Pydantic | Type-safe endpoints |
| **AI Engine** | CrewAI + LangChain | Agent orchestration |
| **Database** | PostgreSQL/SQLite | Data persistence |
| **Auth** | JWT + Databricks OAuth | Security |

---

## Core Concepts

### Agents
```python
agent = Agent(
    role="Data Scientist",
    goal="Analyze patterns",
    backstory="Expert in ML",
    tools=[SearchTool(), AnalysisTool()],
    model="databricks-llama-70b"
)
```

### Tasks
```python
task = Task(
    description="Generate weekly report",
    expected_output="Markdown report with insights",
    agent=analyst_agent,
    async_execution=True
)
```

### Crews
```python
crew = Crew(
    agents=[researcher, writer, reviewer],
    tasks=[research_task, write_task, review_task],
    process="hierarchical",  # or "sequential"
    memory=True  # Enable persistent memory
)
```

### Tools
```python
# Built-in tools
from kasal.tools import (
    WebSearchTool,
    FileReadTool,
    DatabaseQueryTool,
    CodeExecutionTool
)

# Custom tool
class CustomAPITool(BaseTool):
    name = "api_caller"
    description = "Call external APIs"

    def _run(self, query: str) -> str:
        # Your implementation
        return api_response
```

---

## API Reference

### Authentication
```bash
# Get token
POST /api/v1/auth/login
{
  "username": "user@example.com",
  "password": "secure_password"
}

# Use token
GET /api/v1/crews
Authorization: Bearer <token>
```

### Core Endpoints
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/crews` | List all crews |
| POST | `/api/v1/crews` | Create new crew |
| POST | `/api/v1/executions` | Start execution |
| GET | `/api/v1/executions/{id}` | Get status |
| GET | `/api/v1/traces/{job_id}` | Get execution trace |

### WebSocket Events
```javascript
// Connect to real-time updates
const ws = new WebSocket('ws://localhost:8000/ws');

ws.onmessage = (event) => {
  const data = JSON.parse(event.data);
  // Handle: task_start, task_complete, error, etc.
};
```

---

## Testing

### Unit Tests
```python
# tests/test_agent.py
async def test_agent_execution():
    agent = Agent(role="Tester", goal="Validate")
    result = await agent.execute("Test task")
    assert result.success == True
```

### Run Tests
```bash
# All tests with coverage
python run_tests.py --coverage

# Specific test file
python -m pytest tests/unit/test_file.py -v

# With HTML report
python run_tests.py --coverage --html-coverage
```

---

## Deployment

### Docker
```dockerfile
FROM python:3.9-slim
WORKDIR /app
COPY src/requirements.txt .
RUN pip install -r requirements.txt
COPY . .
CMD ["uvicorn", "src.backend.main:app"]
```

### Environment Variables
```bash
# .env file
DATABASE_URL=postgresql://user:pass@localhost/kasal
DATABRICKS_TOKEN=your_token
OPENAI_API_KEY=your_key
JWT_SECRET=your_secret
```

### Production Checklist
- Enable CORS for your domain
- Set secure JWT secret
- Configure rate limiting
- Enable SSL/TLS
- Set up monitoring (Datadog/New Relic)
- Configure backup strategy

---

## Advanced Topics

<details>
<summary><strong>Custom Memory Backends</strong></summary>

```python
class DatabricksMemory(BaseMemory):
    def __init__(self, index_name: str):
        self.client = DatabricksVectorSearch()
        self.index = index_name

    async def store(self, data: dict):
        await self.client.upsert(self.index, data)

    async def retrieve(self, query: str, k: int = 5):
        return await self.client.search(self.index, query, k)
```
</details>

<details>
<summary><strong>Async Operations</strong></summary>

```python
# CRITICAL: All I/O must be async
async def process_crew_execution(crew_id: str):
    async with get_db_session() as session:
        repo = CrewRepository(session)
        crew = await repo.get(crew_id)

        # Parallel execution
        tasks = [process_task(t) for t in crew.tasks]
        results = await asyncio.gather(*tasks)

        return results
```
</details>

<details>
<summary><strong>Error Handling</strong></summary>

```python
from kasal.exceptions import KasalError

try:
    result = await crew.kickoff()
except AgentExecutionError as e:
    logger.error(f"Agent failed: {e.agent_name}")
    # Retry logic
except TaskTimeoutError as e:
    logger.error(f"Task timeout: {e.task_id}")
    # Fallback strategy
```
</details>

<details>
<summary><strong>Performance Optimization</strong></summary>

```python
# Connection pooling
engine = create_async_engine(
    DATABASE_URL,
    pool_size=20,
    max_overflow=40,
    pool_pre_ping=True
)

# Batch operations
async def bulk_create_tasks(tasks: List[TaskSchema]):
    async with UnitOfWork() as uow:
        await uow.tasks.bulk_create(tasks)
        await uow.commit()

# Caching
from functools import lru_cache

@lru_cache(maxsize=100)
def get_model_config(model_name: str):
    return ModelConfig.get(model_name)
```
</details>

---

## Security

### Authentication Flow
```python
# JWT token generation
def create_access_token(user_id: str) -> str:
    payload = {
        "sub": user_id,
        "exp": datetime.utcnow() + timedelta(hours=24)
    }
    return jwt.encode(payload, JWT_SECRET, algorithm="HS256")

# Protected endpoint
@router.get("/protected")
async def protected_route(
    current_user: User = Depends(get_current_user)
):
    return {"user": current_user.email}
```

### Best Practices
- **Never commit secrets** - Use environment variables
- **Input validation** - Pydantic schemas everywhere
- **SQL injection prevention** - SQLAlchemy ORM only
- **Rate limiting** - Implement per-user limits
- **Audit logging** - Track all mutations

---

## Resources

### Quick Links
- [API Playground](/api/docs)
- [Video Tutorials](https://kasal.ai/videos)
- [Discord Community](https://discord.gg/kasal)
- [Report Issues](https://github.com/kasal/issues)

### Code Examples
- [Basic Agent Setup](https://github.com/kasal/examples/basic)
- [Multi-Agent Collaboration](https://github.com/kasal/examples/multi-agent)
- [Custom Tools](https://github.com/kasal/examples/tools)
- [Production Deployment](https://github.com/kasal/examples/deploy)

### Support
- **Chat**: Available in-app 24/7
- **Email**: dev@kasal.ai
- **Slack**: #kasal-developers

---

*Build smarter, ship faster with Kasal*