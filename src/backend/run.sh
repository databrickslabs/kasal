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
    sqlite      Use SQLite database (default)
                - Uses local file (./app.db)
                - No external database server required
                - Good for development and testing

    postgres    Use PostgreSQL database
                - Connects to external PostgreSQL server
                - Uses POSTGRES_* environment variables for connection
                - Better for production and multi-user scenarios

${BLUE}Options:${NC}
    -h, --help              Show this help message
    -q, --quiet             Suppress debug logging (WARNING globally, but INFO for crew/flow executions)
    -v, --verbose           Enable verbose logging (shows DEBUG for app, but NOT SQL queries)
    -d, --debug             Enable debug mode for all loggers (shows DEBUG + SQL queries)
    --no-console            Disable console output (file logging only)
    --no-file               Disable file logging (console output only)

${BLUE}Server Environment Variables:${NC}
    KASAL_BIND_HOST         Interface to bind (default 127.0.0.1, this machine
                            only). Set to 0.0.0.0 to expose the server on every
                            interface: with LOCAL_DEV_AUTH on, anyone who can
                            reach the port acts as the development user.
    KASAL_PORT              Port to listen on (default 8000)
    KASAL_KILL_PORT_OWNER   If the port is held by a process that is not a
                            Kasal server from this checkout, run.sh refuses to
                            start. Set to true to terminate that process instead.

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

    # Run with SQLite (default)
    ./run.sh
    ./run.sh sqlite

    # Run with PostgreSQL
    ./run.sh postgres

    # Expose the server on the network (explicit opt-in)
    KASAL_BIND_HOST=0.0.0.0 ./run.sh

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

# Default to SQLite if no DB type specified (pass "postgres" for PostgreSQL)
if [ -z "$DB_TYPE" ]; then
    DB_TYPE="sqlite"
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
    echo -e "${YELLOW}Invalid database type. Using SQLite as default.${NC}"
    export DATABASE_TYPE=sqlite
    export SQLITE_DB_PATH=./app.db
fi

# Set default log level if not specified
if [ -z "$KASAL_LOG_LEVEL" ]; then
    export KASAL_LOG_LEVEL=INFO
fi

# Apply default third-party library suppression if not set
if [ -z "$KASAL_LOG_THIRD_PARTY" ]; then
    export KASAL_LOG_THIRD_PARTY=WARNING
fi

# Disable CrewAI telemetry (do NOT set OTEL_SDK_DISABLED as it disables all OTel including App Telemetry)
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

# ---------------------------------------------------------------------------
# Clean up stale Kasal processes from previous runs.
# A plain port-8000 kill misses three things that survive restarts:
#   - the uvicorn --reload PARENT (not bound to the port; respawns workers)
#   - orphaned crew/flow execution subprocesses (multiprocessing spawn_main)
#   - their multiprocessing resource_tracker helpers
# Orphans keep logging into crew.log and fail noisily mid-LLM-call
# ("cannot schedule new futures after shutdown", event pairing warnings).
# Scoped by process working directory so we never touch other projects.
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
STALE_PATTERNS=(
    "uvicorn src.main:app"
    "multiprocessing.spawn import spawn_main"
    "multiprocessing.resource_tracker"
)

kill_stale_kasal() {
    local signal="$1" killed=0 pid cwd
    for pattern in "${STALE_PATTERNS[@]}"; do
        for pid in $(pgrep -f "$pattern" 2>/dev/null); do
            [ "$pid" = "$$" ] && continue
            cwd=$(lsof -a -p "$pid" -d cwd -Fn 2>/dev/null | sed -n 's/^n//p')
            case "$cwd" in
                "$SCRIPT_DIR"*)
                    kill "$signal" "$pid" 2>/dev/null && killed=$((killed+1))
                    ;;
            esac
        done
    done
    echo "$killed"
}

KILLED=$(kill_stale_kasal -TERM)
if [ "$KILLED" -gt 0 ]; then
    echo -e "${YELLOW}Killed $KILLED stale Kasal process(es) from a previous run${NC}"
    sleep 1
    kill_stale_kasal -9 >/dev/null  # escalate for anything that ignored SIGTERM
fi

KASAL_PORT="${KASAL_PORT:-8000}"

# If the port is still taken, the owner is NOT a Kasal server from this
# checkout (those were stopped above). It may be anything: another project,
# a server from another checkout, a database tunnel. Never kill it blindly.
PORT_PIDS=$(lsof -ti:"$KASAL_PORT" -sTCP:LISTEN 2>/dev/null)
if [ -n "$PORT_PIDS" ]; then
    echo -e "${YELLOW}Port $KASAL_PORT is already in use by:${NC}"
    for pid in $PORT_PIDS; do
        echo "  PID $pid: $(ps -o command= -p "$pid" 2>/dev/null | cut -c1-160)"
    done
    case "$(echo "${KASAL_KILL_PORT_OWNER:-false}" | tr '[:upper:]' '[:lower:]')" in
        1|true|yes|on)
            echo -e "${YELLOW}KASAL_KILL_PORT_OWNER is set: sending SIGTERM...${NC}"
            kill $PORT_PIDS 2>/dev/null
            for _ in 1 2 3 4 5; do
                lsof -ti:"$KASAL_PORT" -sTCP:LISTEN >/dev/null 2>&1 || break
                sleep 1
            done
            if lsof -ti:"$KASAL_PORT" -sTCP:LISTEN >/dev/null 2>&1; then
                echo -e "${RED}The process on port $KASAL_PORT ignored SIGTERM. Stop it manually.${NC}"
                exit 1
            fi
            echo -e "${GREEN}Port $KASAL_PORT is now free.${NC}"
            ;;
        *)
            echo -e "${RED}Refusing to start. Stop that process, choose another port with${NC}"
            echo -e "${RED}KASAL_PORT=<port>, or set KASAL_KILL_PORT_OWNER=true to terminate it.${NC}"
            exit 1
            ;;
    esac
fi

# Create logs directory if it doesn't exist
mkdir -p logs

# Sync dependencies with uv (best-effort — skips gracefully if offline).
# --frozen is REQUIRED: a plain `uv sync` re-resolves against the machine's
# configured index (~/.config/uv/uv.toml points at the internal pypi proxy)
# and rewrites uv.lock with proxy URLs that must never land in the public
# repo. --frozen installs exactly what the committed lock says.
echo -e "${BLUE}Syncing dependencies...${NC}"
if ! SYNC_OUTPUT=$(uv sync --frozen --quiet 2>&1); then
    echo -e "${YELLOW}Dependency sync failed:${NC}"
    echo "$SYNC_OUTPUT" | tail -n 20
    if [ ! -x .venv/bin/uvicorn ]; then
        echo -e "${RED}No usable virtualenv at .venv — cannot start. Fix the error above and retry.${NC}"
        exit 1
    fi
    echo -e "${YELLOW}Continuing with the existing .venv; it may not match uv.lock.${NC}"
fi

echo -e "${GREEN}Starting Kasal backend server...${NC}"
echo -e "${BLUE}Logs will be written to ./logs/${NC}"
echo -e "${YELLOW}Press Ctrl+C to stop the server${NC}\n"

# Run using the local venv directly (works offline, no uv resolution needed)
# --reload-dir src scopes the watcher to application code only. Without it,
# WatchFiles also watches tests/, so editing a test bounces the live server
# mid-execution and tears down in-flight crew/flow subprocesses, which stalls
# the reload until everything is force-killed.
#
# This now covers the agent runtime too: it used to live in backend/kasal_engine
# (outside src/), so editing the agent loop, the LLM transport or the event bus
# did NOT reload the server — despite that package documenting itself as
# "edit it directly like any other backend code".
#
# --timeout-graceful-shutdown bounds how long a reload waits for in-flight
# requests. Uvicorn's default is None = WAIT FOREVER, and the frontend holds
# open SSE streams (sse_router timeout defaults to 3600s) whose keepalive loop
# deliberately keeps them alive — so on every reload the old worker sat waiting
# on connections that would not close for an hour, and "WatchFiles detected
# changes... Reloading..." simply hung until something force-killed it. Five
# seconds is longer than any real request here and turns reload back into a
# second-or-two operation.
# A local run without the platform proxy: requests carry no identity header,
# so opt in to the development identity (see main.LocalDevAuthMiddleware).
# Production refuses this regardless of the value.
export LOCAL_DEV_AUTH="${LOCAL_DEV_AUTH:-true}"

# Bind to loopback by default. With no proxy in front, EVERY identity header
# is client-controlled and LOCAL_DEV_AUTH turns an anonymous request into the
# development user, so a network-reachable server is an open door (audit H3).
# Exposing it is an explicit choice: KASAL_BIND_HOST=0.0.0.0.
KASAL_BIND_HOST="${KASAL_BIND_HOST:-127.0.0.1}"
case "$KASAL_BIND_HOST" in
    127.0.0.1|localhost|::1) ;;
    *)
        echo -e "${RED}WARNING: binding to $KASAL_BIND_HOST exposes this server beyond this machine.${NC}"
        case "$(echo "$LOCAL_DEV_AUTH" | tr '[:upper:]' '[:lower:]')" in
            1|true|yes|on)
                echo -e "${RED}LOCAL_DEV_AUTH is on: anyone who can reach port $KASAL_PORT acts as the${NC}"
                echo -e "${RED}development user, and can claim any identity with a header.${NC}"
                ;;
        esac
        ;;
esac
echo -e "${BLUE}Listening on http://$KASAL_BIND_HOST:$KASAL_PORT${NC}"

exec .venv/bin/uvicorn src.main:app --reload --reload-dir src --host "$KASAL_BIND_HOST" --port "$KASAL_PORT" \
    --timeout-graceful-shutdown 5