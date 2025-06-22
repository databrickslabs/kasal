#!/bin/bash

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

# Run the FastAPI application in development mode
uvicorn src.main:app --reload --host 0.0.0.0 --port 8000 