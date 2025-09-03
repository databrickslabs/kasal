#!/bin/bash

# Trap Ctrl+C and kill all child processes
trap 'echo "Shutting down..."; kill $(jobs -p); exit' INT TERM

# Default to PostgreSQL if no argument is provided
DB_TYPE=${1:-postgres}

if [ "$DB_TYPE" = "sqlite" ]; then
    echo "Starting application with SQLite database"
    export DATABASE_TYPE=sqlite
    export SQLITE_DB_PATH=./app.db
elif [ "$DB_TYPE" = "postgres" ]; then
    echo "Starting application with PostgreSQL database"
    export DATABASE_TYPE=postgres
else
    echo "Invalid database type. Using PostgreSQL as default."
    export DATABASE_TYPE=postgres
fi

# Disable CrewAI telemetry
export OTEL_SDK_DISABLED=true
export CREWAI_DISABLE_TELEMETRY=true

# CRITICAL: Use NullPool to prevent asyncpg connection issues with multiple event loops
# This is needed for Databricks memory operations that run in separate event loops
export USE_NULLPOOL=true

# Check if port 8000 is already in use
if lsof -Pi :8000 -sTCP:LISTEN -t >/dev/null ; then
    echo "Port 8000 is already in use. Killing existing process..."
    lsof -ti:8000 | xargs kill -9
    sleep 1
fi

# Run the FastAPI application in development mode
# Using exec to replace the shell process with uvicorn
exec uvicorn src.main:app --reload --host 0.0.0.0 --port 8000 