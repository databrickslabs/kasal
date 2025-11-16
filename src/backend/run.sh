#!/bin/bash

# Kasal Backend Runner with Advanced Logging Control
# ====================================================

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to show help
show_help() {
    cat << EOF
$(echo -e "${GREEN}Kasal Backend Runner - Advanced Logging Control${NC}")
================================================

${BLUE}Usage:${NC}
    ./run.sh [OPTIONS] [DB_TYPE]

${BLUE}Database Types:${NC}
    postgres    Use PostgreSQL database (default)
                - Connects to external PostgreSQL server
                - Uses POSTGRES_* environment variables for connection
                - Better for production and multi-user scenarios

    sqlite      Use SQLite database
                - Uses local file (./app.db)
                - No external database server required
                - Good for development and testing

${BLUE}Options:${NC}
    -h, --help              Show this help message
    -q, --quiet             Suppress debug logging (WARNING globally, but INFO for crew/flow executions)
    -v, --verbose           Enable verbose logging (shows DEBUG for app, but NOT SQL queries)
    -d, --debug             Enable debug mode for all loggers (shows DEBUG + SQL queries)
    --no-console            Disable console output (file logging only)
    --no-file               Disable file logging (console output only)

${BLUE}Logging Control Environment Variables:${NC}

    ${GREEN}Global Controls:${NC}
    KASAL_LOG_LEVEL         Set global log level (DEBUG|INFO|WARNING|ERROR|CRITICAL|OFF)
    KASAL_DEBUG_ALL         Enable debug for all loggers (true|false)
    KASAL_LOG_CONSOLE       Enable/disable console output (true|false)
    KASAL_LOG_FILE          Enable/disable file output (true|false)
    KASAL_LOG_THIRD_PARTY   Set third-party library log level

    ${GREEN}Domain-Specific Controls:${NC}
    KASAL_LOG_CREW          Control crew execution logs
    KASAL_LOG_FLOW          Control flow execution logs
    KASAL_LOG_SYSTEM        Control system logs
    KASAL_LOG_LLM           Control LLM interaction logs
    KASAL_LOG_API           Control API request/response logs
    KASAL_LOG_DATABASE      Control database operation logs
    KASAL_LOG_SCHEDULER     Control scheduler logs
    KASAL_LOG_GUARDRAILS    Control guardrails validation logs

    ${GREEN}Databricks Memory Logs:${NC}
    KASAL_LOG_DATABRICKS_VECTOR   Vector search operations
    KASAL_LOG_DATABRICKS_SHORT    Short-term memory operations
    KASAL_LOG_DATABRICKS_LONG     Long-term memory operations
    KASAL_LOG_DATABRICKS_ENTITY   Entity memory operations

    ${GREEN}Third-Party Library Controls:${NC}
    KASAL_LOG_SQLALCHEMY    SQLAlchemy ORM logs
    KASAL_LOG_UVICORN       Uvicorn server logs
    KASAL_LOG_CREWAI        CrewAI framework logs
    KASAL_LOG_MLFLOW        MLflow tracking logs
    KASAL_LOG_HTTPX         HTTP client logs

${BLUE}Examples:${NC}

    # Run with PostgreSQL (default)
    ./run.sh
    ./run.sh postgres

    # Run with SQLite
    ./run.sh sqlite

    # Verbose mode with PostgreSQL (app debug, no SQL)
    ./run.sh -v postgres

    # Verbose mode with SQLite
    ./run.sh -v sqlite

    # Debug mode with PostgreSQL (app debug + SQL queries)
    ./run.sh -d postgres

    # Debug mode with SQLite (app debug + SQL queries)
    ./run.sh -d sqlite

    # Quiet mode (suppress debug logs)
    ./run.sh -q postgres
    ./run.sh -q sqlite

    # Run with specific domain debugging
    KASAL_LOG_CREW=DEBUG KASAL_LOG_LLM=DEBUG ./run.sh

    # Debug flow executions specifically
    KASAL_LOG_FLOW=DEBUG ./run.sh

    # Debug only database operations
    KASAL_LOG_DATABASE=DEBUG KASAL_LOG_SQLALCHEMY=DEBUG ./run.sh

${BLUE}Log Files:${NC}
    Logs are stored in: ./logs/
    - crew.log              Crew execution logs
    - flow.log              Flow execution logs
    - system.log            System operations
    - api.log               API requests
    - llm.log               LLM interactions
    - database.log          Database operations
    - scheduler.log         Scheduled tasks
    - guardrails.log        Validation logs

EOF
}

# Function to print current configuration
print_config() {
    echo -e "\n${GREEN}Current Logging Configuration:${NC}"
    echo "================================"

    # Global settings
    echo -e "${BLUE}Global Settings:${NC}"
    echo "  Log Level: ${KASAL_LOG_LEVEL:-INFO}"
    echo "  Debug All: ${KASAL_DEBUG_ALL:-false}"
    echo "  Console Output: ${KASAL_LOG_CONSOLE:-true}"
    echo "  File Output: ${KASAL_LOG_FILE:-true}"
    echo "  Third-Party Level: ${KASAL_LOG_THIRD_PARTY:-WARNING}"

    # Check for domain-specific overrides
    echo -e "\n${BLUE}Domain Overrides:${NC}"
    overrides_found=false
    for var in KASAL_LOG_CREW KASAL_LOG_FLOW KASAL_LOG_SYSTEM KASAL_LOG_LLM KASAL_LOG_API \
               KASAL_LOG_DATABASE KASAL_LOG_SCHEDULER KASAL_LOG_GUARDRAILS \
               KASAL_LOG_DATABRICKS_VECTOR KASAL_LOG_DATABRICKS_SHORT \
               KASAL_LOG_DATABRICKS_LONG KASAL_LOG_DATABRICKS_ENTITY; do
        if [ ! -z "${!var}" ]; then
            domain_name=$(echo $var | sed 's/KASAL_LOG_//')
            echo "  $domain_name: ${!var}"
            overrides_found=true
        fi
    done

    if [ "$overrides_found" = false ]; then
        echo "  (none)"
    fi

    echo "================================"
    echo ""
}

# Parse command line arguments
DB_TYPE=""
SHOW_CONFIG=true

while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            show_help
            exit 0
            ;;
        -q|--quiet)
            export KASAL_LOG_LEVEL=WARNING
            export KASAL_LOG_APP=WARNING
            export KASAL_LOG_THIRD_PARTY=ERROR
            export KASAL_LOG_FLOW=INFO
            export KASAL_LOG_CREW=INFO
            echo -e "${YELLOW}Quiet mode enabled - suppressing debug logs (preserving execution logs)${NC}"
            shift
            ;;
        -v|--verbose)
            export KASAL_LOG_LEVEL=DEBUG
            export KASAL_LOG_APP=DEBUG
            echo -e "${YELLOW}Verbose mode enabled - showing debug logs${NC}"
            shift
            ;;
        -d|--debug)
            export KASAL_DEBUG_ALL=true
            export SQL_DEBUG=true
            export KASAL_LOG_DATABASE=DEBUG
            echo -e "${YELLOW}Debug mode enabled for all loggers (including SQLAlchemy)${NC}"
            shift
            ;;
        --no-console)
            export KASAL_LOG_CONSOLE=false
            echo -e "${YELLOW}Console output disabled - file logging only${NC}"
            shift
            ;;
        --no-file)
            export KASAL_LOG_FILE=false
            echo -e "${YELLOW}File logging disabled - console output only${NC}"
            shift
            ;;
        --no-config)
            SHOW_CONFIG=false
            shift
            ;;
        postgres|sqlite)
            DB_TYPE=$1
            shift
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            echo "Use -h or --help for usage information"
            exit 1
            ;;
    esac
done

# Default to PostgreSQL if no DB type specified
if [ -z "$DB_TYPE" ]; then
    DB_TYPE="postgres"
fi

# Trap Ctrl+C and kill all child processes
trap 'echo "Shutting down..."; kill $(jobs -p); exit' INT TERM

# Set database configuration
if [ "$DB_TYPE" = "sqlite" ]; then
    echo -e "${GREEN}Starting application with SQLite database${NC}"
    export DATABASE_TYPE=sqlite
    export SQLITE_DB_PATH=./app.db
elif [ "$DB_TYPE" = "postgres" ]; then
    echo -e "${GREEN}Starting application with PostgreSQL database${NC}"
    export DATABASE_TYPE=postgres
else
    echo -e "${YELLOW}Invalid database type. Using PostgreSQL as default.${NC}"
    export DATABASE_TYPE=postgres
fi

# Set default log level if not specified
if [ -z "$KASAL_LOG_LEVEL" ]; then
    export KASAL_LOG_LEVEL=INFO
fi

# Apply default third-party library suppression if not set
if [ -z "$KASAL_LOG_THIRD_PARTY" ]; then
    export KASAL_LOG_THIRD_PARTY=WARNING
fi

# Disable CrewAI telemetry
export OTEL_SDK_DISABLED=true
export CREWAI_DISABLE_TELEMETRY=true

# Legacy support - map old LOG_LEVEL to new system
if [ ! -z "$LOG_LEVEL" ] && [ -z "$KASAL_LOG_LEVEL" ]; then
    export KASAL_LOG_LEVEL=$LOG_LEVEL
fi

# INTELLIGENT ENGINE SELECTION: The backend now uses dual engines for optimal performance
# - FastAPI requests use pooled connections (20x faster)
# - Background tasks/CrewAI use NullPool (event loop isolation)
# Set USE_NULLPOOL=true to force NullPool for all contexts (slower but safer)
export USE_NULLPOOL=true

# Show current configuration if not disabled
if [ "$SHOW_CONFIG" = true ]; then
    print_config
fi

# Check if port 8000 is already in use
if lsof -Pi :8000 -sTCP:LISTEN -t >/dev/null 2>&1; then
    echo -e "${YELLOW}Port 8000 is already in use. Killing existing process...${NC}"
    lsof -ti:8000 | xargs kill -9 2>/dev/null
    sleep 1
fi

# Create logs directory if it doesn't exist
mkdir -p logs

echo -e "${GREEN}Starting Kasal backend server...${NC}"
echo -e "${BLUE}Logs will be written to ./logs/${NC}"
echo -e "${YELLOW}Press Ctrl+C to stop the server${NC}\n"

# Run the FastAPI application in development mode
# Using exec to replace the shell process with uvicorn
exec uvicorn src.main:app --reload --host 0.0.0.0 --port 8000 