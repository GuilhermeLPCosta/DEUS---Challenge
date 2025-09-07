.PHONY: help setup venv clean build up down logs test etl setup-data

# Default target
help:
	@echo "Available commands:"
	@echo "  setup      - Set up virtual environment"
	@echo "  venv       - Activate virtual environment (source venv/bin/activate)"
	@echo "  build      - Build Docker images"
	@echo "  up         - Start services with mock data"
	@echo "  up-etl     - Start services with real ETL data"
	@echo "  down       - Stop all services"
	@echo "  clean      - Stop services and remove volumes"
	@echo "  logs       - Show all service logs"
	@echo "  logs-etl   - Show ETL logs"
	@echo "  logs-api   - Show API logs"
	@echo "  test       - Run application tests"
	@echo "  migration  - Create new migration (usage: make migration MSG='description')"
	@echo "  migrate    - Run migrations manually"

# Setup virtual environment
setup:
	./setup_venv.sh

# Activate virtual environment (reminder)
venv:
	@echo "Run: source venv/bin/activate"

# Build Docker images
build:
	docker-compose build

# Start services with mock data (default)
up:
	docker-compose --profile setup up -d
	@echo "Services starting... API will be available at http://localhost:8000"
	@echo "Run 'make logs' to see startup progress"

# Start services (ETL triggered via API)
up-prod:
	docker-compose up -d
	@echo "Production services started"
	@echo "Trigger ETL via: curl -X POST http://localhost:8000/etl/start"

# Stop all services
down:
	docker-compose down

# Stop services and remove volumes (clean slate)
clean:
	docker-compose down -v
	docker system prune -f

# Show all logs
logs:
	docker-compose logs

# Trigger ETL via API
start-etl:
	curl -X POST "http://localhost:8000/etl/start"

# Check ETL status
etl-status:
	curl -s "http://localhost:8000/etl/status" | jq .

# Show API logs
logs-api:
	docker-compose logs -f api

# Show migration logs
logs-migrations:
	docker-compose logs migrations

# Test API endpoints
test:
	@echo "Testing API endpoints..."
	@curl -s http://localhost:8000/health | jq .
	@curl -s "http://localhost:8000/actors?profession=actor&limit=3" | jq .

# Create new migration
migration:
ifndef MSG
	@echo "Usage: make migration MSG='description of changes'"
	@exit 1
endif
	python scripts/run_migrations.py create "$(MSG)"

# Run migrations manually
migrate:
	python scripts/run_migrations.py

# Quick development setup
dev: clean build up
	@echo "Development environment ready!"
	@echo "API: http://localhost:8000"
	@echo "Docs: http://localhost:8000/docs"

# Production setup
prod: clean build up-prod
	@echo "Production environment ready!"
	@echo "Start ETL with: make start-etl"