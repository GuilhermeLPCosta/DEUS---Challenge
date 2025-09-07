#!/bin/bash

# Startup script for Docker containers
set -e

echo "Starting IMDb application startup sequence..."

# Function to wait for database
wait_for_db() {
    echo "Waiting for database to be ready..."
    python -c "
import time
import sys
from app.config.settings import get_settings
from sqlalchemy import create_engine, text

settings = get_settings()
max_retries = 30
retry_interval = 2

for attempt in range(max_retries):
    try:
        engine = create_engine(settings.database_url)
        with engine.connect() as conn:
            conn.execute(text('SELECT 1'))
        print('Database is ready!')
        sys.exit(0)
    except Exception as e:
        print(f'Database not ready (attempt {attempt + 1}/{max_retries}): {e}')
        if attempt < max_retries - 1:
            time.sleep(retry_interval)
        else:
            print('Database failed to become ready')
            sys.exit(1)
"
}

# Function to run migrations
run_migrations() {
    echo "Running database migrations..."
    python scripts/run_migrations.py
    echo "Migrations completed successfully"
}

# Function to check if this is the first run
is_first_run() {
    python -c "
from app.config.settings import get_settings
from sqlalchemy import create_engine, text, inspect

try:
    settings = get_settings()
    engine = create_engine(settings.database_url)
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    
    # Check if core tables exist
    required_tables = ['people', 'titles', 'ratings', 'principals', 'etl_runs', 'actor_ratings']
    missing_tables = [table for table in required_tables if table not in tables]
    
    if missing_tables:
        print('FIRST_RUN')
    else:
        print('NOT_FIRST_RUN')
except Exception as e:
    print('FIRST_RUN')  # Assume first run if we can't check
"
}

# Main startup logic
case "$1" in
    "migrations")
        echo "Running migrations only..."
        wait_for_db
        run_migrations
        ;;
    "api")
        echo "Starting API server..."
        wait_for_db
        # API doesn't need to run migrations, they should be done by migrations service
        exec uvicorn app.api:app --host 0.0.0.0 --port 8000 --reload
        ;;

    "setup")
        echo "Running setup (mock data)..."
        wait_for_db
        exec python scripts/create_mock_data.py
        ;;
    *)
        echo "Usage: $0 {migrations|api|setup}"
        echo "  migrations - Run database migrations"
        echo "  api       - Start API server"
        echo "  setup     - Create mock data"
        exit 1
        ;;
esac