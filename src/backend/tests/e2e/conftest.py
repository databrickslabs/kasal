"""
Configuration and fixtures for end-to-end tests.
"""

import asyncio
import os
from typing import AsyncGenerator, Generator

import pytest
import pytest_asyncio
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.pool import NullPool

from src.db.base import Base
from src.main import app
from src.core.config import get_settings
from src.db.session import get_db


# Override test database URL
TEST_DATABASE_URL = os.getenv(
    "TEST_DATABASE_URL",
    "sqlite+aiosqlite:///:memory:"
)


@pytest.fixture(scope="session")
def event_loop() -> Generator:
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture(scope="function")
async def test_db_engine():
    """Create a test database engine."""
    engine = create_async_engine(
        TEST_DATABASE_URL,
        echo=False,
        poolclass=NullPool,
    )
    
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    
    yield engine
    
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    
    await engine.dispose()


@pytest_asyncio.fixture(scope="function")
async def test_db_session(test_db_engine) -> AsyncGenerator[AsyncSession, None]:
    """Create a test database session."""
    async_session_maker = async_sessionmaker(
        test_db_engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    
    async with async_session_maker() as session:
        yield session


@pytest_asyncio.fixture(scope="function")
async def async_client(test_db_session) -> AsyncGenerator[AsyncClient, None]:
    """Create an async HTTP client for testing."""
    
    async def override_get_db():
        yield test_db_session
    
    app.dependency_overrides[get_db] = override_get_db
    
    async with AsyncClient(app=app, base_url="http://test") as client:
        yield client
    
    app.dependency_overrides.clear()


@pytest.fixture
def mock_execution_result():
    """Mock execution result for flight search."""
    return {
        "value": """I found several flight options from Zurich to Montreal on July 20th, 2025:

**Direct Flights:**
1. Air Canada AC879
   - Departure: 10:15 AM (ZRH)
   - Arrival: 12:45 PM (YUL)
   - Duration: 8h 30m
   - Price: $1,250 CAD

2. Swiss LX86
   - Departure: 1:05 PM (ZRH)
   - Arrival: 3:40 PM (YUL)
   - Duration: 8h 35m
   - Price: $1,380 CAD

**Connecting Flights:**
1. Lufthansa via Frankfurt
   - Total duration: 11h 20m
   - Price: $980 CAD

2. KLM via Amsterdam
   - Total duration: 12h 15m
   - Price: $890 CAD

Recommendation: The Air Canada direct flight offers the best value for time saved."""
    }


@pytest.fixture
def mock_news_result():
    """Mock execution result for news aggregation."""
    return {
        "summary": "Latest updates on AI and Data Summit Snowflake June 2025",
        "articles": [
            {
                "title": "Snowflake Announces Major AI Features at Data Summit 2025",
                "date": "June 15, 2025",
                "source": "TechCrunch",
                "summary": "Snowflake unveiled new AI-powered data analysis tools..."
            },
            {
                "title": "Data Summit 2025: The Future of Cloud Analytics",
                "date": "June 16, 2025",
                "source": "Forbes",
                "summary": "Industry leaders gather to discuss the convergence of AI and data..."
            }
        ]
    }


@pytest.fixture
def sample_traces():
    """Sample execution traces for testing."""
    return [
        {
            "id": 1,
            "job_id": "test-execution-id",
            "created_at": "2025-06-20T08:00:00",
            "event_type": "crew_start",
            "event_source": "Flight Search Crew",
            "event_context": "Starting execution",
            "output": None
        },
        {
            "id": 2,
            "job_id": "test-execution-id",
            "created_at": "2025-06-20T08:00:01",
            "event_type": "agent_start",
            "event_source": "Flight Search Agent",
            "event_context": "Initializing agent",
            "output": None
        },
        {
            "id": 3,
            "job_id": "test-execution-id",
            "created_at": "2025-06-20T08:00:02",
            "event_type": "task_start",
            "event_source": "Search Flights Task",
            "event_context": "Searching flights from Zurich to Montreal",
            "output": None
        },
        {
            "id": 4,
            "job_id": "test-execution-id",
            "created_at": "2025-06-20T08:00:10",
            "event_type": "tool_call",
            "event_source": "web_search",
            "event_context": "Searching for flight information",
            "output": {"query": "flights Zurich Montreal July 20 2025"}
        },
        {
            "id": 5,
            "job_id": "test-execution-id",
            "created_at": "2025-06-20T08:00:20",
            "event_type": "task_end",
            "event_source": "Search Flights Task",
            "event_context": "Task completed successfully",
            "output": {"status": "success"}
        },
        {
            "id": 6,
            "job_id": "test-execution-id",
            "created_at": "2025-06-20T08:00:21",
            "event_type": "crew_end",
            "event_source": "Flight Search Crew",
            "event_context": "Execution completed",
            "output": None
        }
    ]