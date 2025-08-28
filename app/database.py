import os
import logging
from typing import Generator
from sqlalchemy import create_engine, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool
from app.config import Config

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database setup
engine = create_engine(
    Config.DATABASE_URL,
    poolclass=QueuePool,
    pool_size=5,
    max_overflow=10,
    pool_pre_ping=True,
    echo=False
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

def get_db() -> Generator[Session, None, None]:
    """Dependency to get database session"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def init_database():
    """Initialize database schema"""
    try:
        # Read and execute schema file
        schema_path = os.path.join(os.path.dirname(__file__), 'schemas.sql')
        with open(schema_path, 'r') as f:
            schema_sql = f.read()
        
        with engine.connect() as conn:
            # Split by statements and execute each one
            statements = [stmt.strip() for stmt in schema_sql.split(';') if stmt.strip()]
            for statement in statements:
                try:
                    conn.execute(text(statement))
                    conn.commit()
                except Exception as e:
                    logger.warning(f"Statement execution warning: {e}")
                    # Continue with other statements
                    
        logger.info("Database schema initialized successfully")
        
    except Exception as e:
        logger.error(f"Error initializing database: {e}")
        raise

def execute_sql(sql: str, params: dict = None):
    """Execute SQL statement"""
    with engine.connect() as conn:
        result = conn.execute(text(sql), params or {})
        conn.commit()
        return result

def refresh_materialized_view():
    """Refresh the actor ratings materialized view"""
    try:
        execute_sql("SELECT refresh_actor_ratings();")
        logger.info("Materialized view refreshed successfully")
    except Exception as e:
        logger.error(f"Error refreshing materialized view: {e}")
        raise