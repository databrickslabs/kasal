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

# Run the FastAPI application in development mode
uvicorn src.main:app --reload --host 0.0.0.0 --port 8000 