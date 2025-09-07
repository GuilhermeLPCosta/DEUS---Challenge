#!/usr/bin/env bash
set -e

# Help menu
help() {
  cat <<EOF
Available commands:
  setup        - Set up virtual environment
  venv         - Activate virtual environment (source venv/bin/activate)
  build        - Build Docker images
  up           - Start services with mock data
  up-prod      - Start services with real ETL data
  down         - Stop all services
  clean        - Stop services and remove volumes
  logs         - Show all service logs
  logs-api     - Show API logs
  logs-migrations - Show migration logs
  start-etl    - Trigger ETL via API
  etl-status   - Check ETL status
  test         - Run application tests
  migration    - Create new migration (usage: ./project.sh migration "description")
  migrate      - Run migrations manually
  dev          - Quick development setup (clean, build, up)
  prod         - Production setup (clean, build, up-prod)
EOF
}

# Commands
setup() { ./setup_venv.sh; }

venv() { echo "Run: source venv/bin/activate"; }

build() { docker-compose build; }

up() {
  docker-compose --profile setup up -d
  echo "Services starting... API will be available at http://localhost:8000"
  echo "Run './project.sh logs' to see startup progress"
}

up-prod() {
  docker-compose up -d
  echo "Production services started"
  echo "Trigger ETL via: curl -X POST http://localhost:8000/etl/start"
}

down() { docker-compose down; }

clean() {
  docker-compose down -v
  docker system prune -f
}

logs() { docker-compose logs; }

logs-api() { docker-compose logs -f api; }

logs-migrations() { docker-compose logs migrations; }

start-etl() { curl -X POST "http://localhost:8000/etl/start"; }

etl-status() { curl -s "http://localhost:8000/etl/status" | jq .; }

test() {
  echo "Testing API endpoints..."
  curl -s http://localhost:8000/health | jq .
  curl -s "http://localhost:8000/actors?profession=actor&limit=3" | jq .
}

migration() {
  if [ -z "$1" ]; then
    echo "Usage: ./project.sh migration \"description of changes\""
    exit 1
  fi
  python scripts/run_migrations.py create "$1"
}

migrate() { python scripts/run_migrations.py; }

dev() {
  clean
  build
  up
  echo "Development environment ready!"
  echo "API: http://localhost:8000"
  echo "Docs: http://localhost:8000/docs"
}

prod() {
  clean
  build
  up-prod
  echo "Production environment ready!"
  echo "Start ETL with: ./project.sh start-etl"
}

# Entry point: loop through all arguments like make does
if [ $# -eq 0 ]; then
  help
  exit 0
fi

while [ $# -gt 0 ]; do
  cmd=$1
  shift
  if declare -f "$cmd" > /dev/null; then
    # If migration, pass description argument
    if [ "$cmd" = "migration" ]; then
      migration "$@"
      break
    else
      $cmd "$@"
    fi
  else
    echo "Unknown command: $cmd"
    help
    exit 1
  fi
done
