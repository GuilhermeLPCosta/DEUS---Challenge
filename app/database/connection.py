"""
Simplified database connection using SQLAlchemy
"""

from typing import Optional

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.config.settings import get_settings
from app.services.monitoring_service import get_monitoring_service

monitoring = get_monitoring_service()
logger = monitoring.get_logger("database.connection")

# Global instances
_engine: Optional[Engine] = None
_session_factory: Optional[sessionmaker] = None


def get_engine() -> Engine:
    """Get database engine"""
    global _engine
    if _engine is None:
        settings = get_settings()

        logger.info("Creating database engine")

        _engine = create_engine(
            settings.database_url,
            pool_size=settings.db_pool_size,
            max_overflow=settings.db_max_overflow,
            echo=settings.db_echo,
            pool_pre_ping=True,
            pool_recycle=3600,
        )

    return _engine


def get_session_factory() -> sessionmaker:
    """Get session factory"""
    global _session_factory
    if _session_factory is None:
        engine = get_engine()
        _session_factory = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    return _session_factory


def get_db() -> Session:
    """Get database session for dependency injection"""
    session_factory = get_session_factory()
    session = session_factory()
    try:
        yield session
    finally:
        session.close()


def test_connection() -> bool:
    """Test database connectivity"""
    try:
        from sqlalchemy import text
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("Database connection test successful")
        return True
    except Exception as e:
        logger.error(f"Database connection test failed: {str(e)}")
        return False


def init_database():
    """Initialize database connection and create tables if needed"""
    try:
        # Test connection first
        if not test_connection():
            raise Exception("Database connection test failed")

        # Create tables using SQLAlchemy models if they don't exist
        from app.database.models import Base
        engine = get_engine()
        
        # Create all tables defined in models
        Base.metadata.create_all(engine)
        
        logger.info("Database initialized successfully")

    except Exception as e:
        logger.error(f"Database initialization failed: {str(e)}")
        # Don't raise the exception - just log it for now
        logger.warning("Continuing without database initialization")