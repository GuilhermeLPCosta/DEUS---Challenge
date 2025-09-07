#!/usr/bin/env python3
"""
Database migration runner for Docker containers
"""

import os
import sys
import time
from pathlib import Path

# Add the app directory to Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

from alembic.config import Config
from alembic import command
from sqlalchemy import create_engine, text
from app.config.settings import get_settings
from app.services.logger_service import get_logger

logger = get_logger("migrations")


def wait_for_database(database_url: str, max_retries: int = 30, retry_interval: int = 2):
    """Wait for database to be available"""
    logger.info(f"Waiting for database to be available - max_retries: {max_retries}")
    
    for attempt in range(max_retries):
        try:
            engine = create_engine(database_url)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("Database connection successful")
            return True
        except Exception as e:
            logger.warning(f"Database connection attempt {attempt + 1} failed - error: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(retry_interval)
            else:
                logger.error("Database connection failed after all retries")
                return False
    
    return False


def run_migrations():
    """Run database migrations"""
    try:
        settings = get_settings()
        database_url = settings.database_url
        
        logger.info("Starting database migration process")
        
        # Wait for database to be available
        if not wait_for_database(database_url):
            logger.error("Database not available, cannot run migrations")
            sys.exit(1)
        
        # Setup Alembic configuration
        alembic_cfg = Config("alembic.ini")
        alembic_cfg.set_main_option("sqlalchemy.url", database_url)
        
        logger.info("Running database migrations")
        
        # Run migrations
        command.upgrade(alembic_cfg, "head")
        
        logger.info("Database migrations completed successfully")
        
    except Exception as e:
        logger.error(f"Migration failed - error: {str(e)}")
        sys.exit(1)


def create_migration(message: str):
    """Create a new migration"""
    try:
        settings = get_settings()
        database_url = settings.database_url
        
        # Setup Alembic configuration
        alembic_cfg = Config("alembic.ini")
        alembic_cfg.set_main_option("sqlalchemy.url", database_url)
        
        logger.info(f"Creating new migration - message: {message}")
        
        # Create migration
        command.revision(alembic_cfg, message=message, autogenerate=True)
        
        logger.info("Migration created successfully")
        
    except Exception as e:
        logger.error(f"Migration creation failed - error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        if sys.argv[1] == "create" and len(sys.argv) > 2:
            create_migration(" ".join(sys.argv[2:]))
        else:
            logger.error("Usage: python run_migrations.py [create <message>]")
            sys.exit(1)
    else:
        run_migrations()