"""
Environment-based configuration loader
"""

import os
from typing import Any, Dict, Optional


class Settings:
    """Configuration class using environment variables"""

    def __init__(self):
        # Environment
        self.environment = os.getenv("ENVIRONMENT", "development").lower()
        self.debug = os.getenv("DEBUG", "false").lower() in ("true", "1", "yes")

        # API Configuration
        self.api_host = os.getenv("API_HOST", "0.0.0.0")
        self.api_port = int(os.getenv("API_PORT", "8000"))
        self.api_title = os.getenv("API_TITLE", "IMDb Actors Rating API")
        self.api_version = os.getenv("API_VERSION", "1.0.0")
        self.api_log_level = os.getenv("API_LOG_LEVEL", "info")
        self.api_reload = os.getenv("API_RELOAD", "false").lower() in ("true", "1", "yes")
        self.api_default_limit = int(os.getenv("API_DEFAULT_LIMIT", "100"))
        self.api_max_limit = int(os.getenv("API_MAX_LIMIT", "1000"))

        # Database Configuration
        self.database_url = os.getenv(
            "DATABASE_URL", "postgresql://imdb_user:imdb_password@localhost:5432/imdb_db"
        )
        self.db_pool_size = int(os.getenv("DB_POOL_SIZE", "5"))
        self.db_max_overflow = int(os.getenv("DB_MAX_OVERFLOW", "10"))
        self.db_echo = os.getenv("DB_ECHO", "false").lower() in ("true", "1", "yes")

        # ETL Configuration
        self.imdb_base_url = os.getenv("IMDB_BASE_URL", "https://datasets.imdbws.com/")
        self.data_dir = os.getenv("DATA_DIR", "./data")
        self.chunk_size = int(os.getenv("CHUNK_SIZE", "10000"))
        self.max_retries = int(os.getenv("MAX_RETRIES", "3"))
        self.etl_timeout = int(os.getenv("ETL_TIMEOUT", "30"))

        # Security Configuration
        self.secret_key = os.getenv("SECRET_KEY", "")

        # Logging Configuration
        self.log_level = os.getenv("LOG_LEVEL", "INFO")
        self.log_format = os.getenv("LOG_FORMAT", "json")

        # Metrics Configuration
        self.metrics_enabled = os.getenv("METRICS_ENABLED", "true").lower() in ("true", "1", "yes")

        # Target professions for the API
        self.target_professions = ["actor", "actress"]

        # IMDb files to process
        self.imdb_files = {
            "name_basics": "name.basics.tsv.gz",
            "title_basics": "title.basics.tsv.gz",
            "title_principals": "title.principals.tsv.gz",
            "title_ratings": "title.ratings.tsv.gz",
        }

    def validate(self) -> None:
        """Validate configuration settings"""
        errors = []

        # Validate required settings for production
        if self.environment == "production":
            if not self.secret_key:
                errors.append("SECRET_KEY is required for production environment")
            if len(self.secret_key) < 32:
                errors.append("SECRET_KEY must be at least 32 characters long")

        # Validate numeric ranges
        if self.api_port < 1 or self.api_port > 65535:
            errors.append(f"API_PORT must be between 1 and 65535, got {self.api_port}")

        if self.api_default_limit > self.api_max_limit:
            errors.append(
                f"API_DEFAULT_LIMIT ({self.api_default_limit}) cannot exceed API_MAX_LIMIT ({self.api_max_limit})"
            )

        if self.chunk_size <= 0:
            errors.append(f"CHUNK_SIZE must be positive, got {self.chunk_size}")

        if self.max_retries < 0:
            errors.append(f"MAX_RETRIES must be non-negative, got {self.max_retries}")

        if self.db_pool_size <= 0:
            errors.append(f"DB_POOL_SIZE must be positive, got {self.db_pool_size}")

        if errors:
            raise ValueError("Configuration validation failed:\n" + "\n".join(errors))

    def get_environment_info(self) -> Dict[str, Any]:
        """Get environment information"""
        return {
            "environment": self.environment,
            "debug": self.debug,
            "api": {
                "host": self.api_host,
                "port": self.api_port,
                "log_level": self.api_log_level,
                "reload": self.api_reload,
            },
            "etl": {
                "base_url": self.imdb_base_url,
                "chunk_size": self.chunk_size,
                "max_retries": self.max_retries,
                "data_dir": self.data_dir,
            },
            "database": {"pool_size": self.db_pool_size, "max_overflow": self.db_max_overflow, "echo": self.db_echo},
        }


def load_settings() -> Settings:
    """Load and validate settings"""
    settings = Settings()
    settings.validate()
    return settings


# Global settings instance
_settings: Optional[Settings] = None


def get_settings() -> Settings:
    """Get global settings instance"""
    global _settings
    if _settings is None:
        _settings = load_settings()
    return _settings


# For backward compatibility
def get_config() -> Settings:
    """Get configuration instance"""
    return get_settings()


# Alias for backward compatibility
Config = get_config