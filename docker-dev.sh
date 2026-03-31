#!/usr/bin/env bash
###############################################################################
# Kasal – Docker Development Helper
#
# Usage:
#   ./docker-dev.sh up        Build & start all services
#   ./docker-dev.sh down      Stop and remove containers
#   ./docker-dev.sh restart   Restart all services
#   ./docker-dev.sh logs      Tail logs from all services
#   ./docker-dev.sh logs <s>  Tail logs for a specific service (backend|frontend|postgres)
#   ./docker-dev.sh shell <s> Open a shell in a running service container
#   ./docker-dev.sh build     Rebuild images without starting
#   ./docker-dev.sh clean     Stop containers AND remove volumes (full reset)
#   ./docker-dev.sh ps        Show running containers
#   ./docker-dev.sh test      Run backend tests inside container
###############################################################################

set -euo pipefail

COMPOSE_FILE="docker-compose.dev.yml"
PROJECT_NAME="kasal"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Ensure we run from the project root (where the compose file lives)
cd "$(dirname "$0")"

compose() {
  podman-compose -f "$COMPOSE_FILE" -p "$PROJECT_NAME" "$@"
}

usage() {
  cat <<EOF
${GREEN}Kasal Docker Development Helper${NC}

${BLUE}Usage:${NC}  ./docker-dev.sh <command> [args]

${BLUE}Commands:${NC}
  up              Build images and start all services
  down            Stop and remove containers (keeps volumes)
  restart         Restart all services
  logs [service]  Tail logs (optionally for a single service)
  shell <service> Open a shell in a running container
                  Services: backend, frontend, postgres
  build           Rebuild images without starting
  clean           Full reset: stop containers, remove volumes
  ps              Show running containers
  test            Run backend tests inside the container

${BLUE}Examples:${NC}
  ./docker-dev.sh up
  ./docker-dev.sh logs backend
  ./docker-dev.sh shell backend
  ./docker-dev.sh clean

EOF
}

case "${1:-help}" in
  up)
    echo -e "${GREEN}Building and starting Kasal dev environment...${NC}"
    compose up --build -d
    echo ""
    echo -e "${GREEN}Services are starting up:${NC}"
    echo -e "  Frontend : ${BLUE}http://localhost:${FRONTEND_PORT:-3000}${NC}"
    echo -e "  Backend  : ${BLUE}http://localhost:${BACKEND_PORT:-8000}${NC}"
    echo -e "  API Docs : ${BLUE}http://localhost:${BACKEND_PORT:-8000}/api-docs${NC}"
    echo -e "  Postgres : ${BLUE}localhost:${POSTGRES_PORT:-5432}${NC}"
    echo ""
    echo -e "${YELLOW}Run './docker-dev.sh logs' to follow output${NC}"
    ;;

  down)
    echo -e "${YELLOW}Stopping Kasal dev environment...${NC}"
    compose down
    echo -e "${GREEN}Done.${NC}"
    ;;

  restart)
    echo -e "${YELLOW}Restarting Kasal dev environment...${NC}"
    compose down
    compose up --build -d
    echo -e "${GREEN}Restarted.${NC}"
    ;;

  logs)
    if [ -n "${2:-}" ]; then
      compose logs -f "$2"
    else
      compose logs -f
    fi
    ;;

  shell)
    SERVICE="${2:-}"
    if [ -z "$SERVICE" ]; then
      echo -e "${RED}Error: specify a service name (backend, frontend, postgres)${NC}"
      exit 1
    fi
    case "$SERVICE" in
      backend)
        echo -e "${BLUE}Opening shell in backend container...${NC}"
        compose exec backend bash
        ;;
      frontend)
        echo -e "${BLUE}Opening shell in frontend container...${NC}"
        compose exec frontend sh
        ;;
      postgres)
        echo -e "${BLUE}Opening psql in postgres container...${NC}"
        compose exec postgres psql -U "${POSTGRES_USER:-postgres}" -d "${POSTGRES_DB:-kasal}"
        ;;
      *)
        echo -e "${RED}Unknown service: $SERVICE${NC}"
        echo "Available: backend, frontend, postgres"
        exit 1
        ;;
    esac
    ;;

  build)
    echo -e "${BLUE}Building images...${NC}"
    compose build
    echo -e "${GREEN}Build complete.${NC}"
    ;;

  clean)
    echo -e "${RED}Stopping containers and removing ALL volumes (full reset)...${NC}"
    compose down -v
    echo -e "${GREEN}Clean complete. Next 'up' will reinstall all dependencies.${NC}"
    ;;

  ps)
    compose ps
    ;;

  test)
    echo -e "${BLUE}Running backend tests...${NC}"
    compose exec backend uv run pytest tests/ -v
    ;;

  help|--help|-h)
    usage
    ;;

  *)
    echo -e "${RED}Unknown command: $1${NC}"
    usage
    exit 1
    ;;
esac

