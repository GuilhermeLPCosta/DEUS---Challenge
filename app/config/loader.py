"""
Configuration loader with validation and error handling
"""

import logging
import os
import sys
from typing import Any, Dict, Optional

from .settings import Settings

logger = logging.getLogger(__name__)


class ConfigurationError(Exception):
    """Custom exception for configuration errors"""

    def __init__(self, message: str, errors: Optional[Dict] = None):
        self.message = message
        self.errors = errors or {}
        super().__init__(self.message)


class ConfigurationLoader:
    """Configuration loader with validation and error reporting"""

    def __init__(self, env_file: Optional[str] = None):
        self.env_file = env_file or ".env"
        self._settings: Optional[Settings] = None

    def load_settings(self, validate: bool = True) -> Settings:
        """
        Load and validate application settings

        Args:
            validate: Whether to perform validation (default: True)

        Returns:
            SimpleSettings: Validated settings object

        Raises:
            ConfigurationError: If configuration is invalid
        """
        try:
            # Load environment file if it exists
            if os.path.exists(self.env_file):
                if not os.access(self.env_file, os.R_OK):
                    raise ConfigurationError(f"Environment file {self.env_file} exists but is not readable")
                logger.info(f"Loading configuration from {self.env_file}")
                self._load_env_file(self.env_file)
            else:
                logger.info("No .env file found, using environment variables and defaults")

            # Load settings
            settings = Settings()

            if validate:
                settings.validate()

            self._settings = settings
            logger.info(f"Configuration loaded successfully for environment: {settings.environment}")

            return settings

        except ValueError as e:
            raise ConfigurationError(str(e))
        except Exception as e:
            raise ConfigurationError(f"Failed to load configuration: {str(e)}")

    def _load_env_file(self, env_file_path: str):
        """Load environment variables from .env file"""
        try:
            with open(env_file_path, "r") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        key, value = line.split("=", 1)
                        key = key.strip()
                        value = value.strip().strip('"').strip("'")
                        if key and not os.getenv(key):  # Don't override existing env vars
                            os.environ[key] = value
        except Exception as e:
            raise ConfigurationError(f"Failed to load environment file {env_file_path}: {str(e)}")

    def get_settings(self) -> Optional[Settings]:
        """Get currently loaded settings"""
        return self._settings

    def reload_settings(self) -> Settings:
        """Reload settings from environment"""
        return self.load_settings()

    def validate_environment_file(self, env_file_path: str) -> Dict[str, Any]:
        """
        Validate an environment file without loading it

        Args:
            env_file_path: Path to the environment file

        Returns:
            Dict with validation results
        """
        result = {
            "valid": False,
            "errors": [],
            "warnings": [],
            "missing_required": [],
            "file_exists": False,
            "file_readable": False,
        }

        # Check file existence and readability
        if os.path.exists(env_file_path):
            result["file_exists"] = True
            if os.access(env_file_path, os.R_OK):
                result["file_readable"] = True
            else:
                result["errors"].append(f"File {env_file_path} is not readable")
                return result
        else:
            result["warnings"].append(f"Environment file {env_file_path} does not exist")

        # Try to load and validate settings
        try:
            temp_loader = ConfigurationLoader(env_file_path)
            settings = temp_loader.load_settings(validate=True)
            result["valid"] = True

            # Check for recommended settings
            if settings.environment == "production":
                if settings.debug:
                    result["warnings"].append("Debug mode is enabled in production")
                if settings.api_reload:
                    result["warnings"].append("API reload is enabled in production")

        except ConfigurationError as e:
            result["errors"].append(e.message)
            if e.errors:
                result["errors"].extend([f"{k}: {v}" for k, v in e.errors.items()])
        except Exception as e:
            result["errors"].append(f"Unexpected error: {str(e)}")

        return result


def load_configuration(env_file: Optional[str] = None, validate: bool = True, exit_on_error: bool = True) -> Settings:
    """
    Convenience function to load configuration with error handling

    Args:
        env_file: Path to environment file (optional)
        validate: Whether to perform validation
        exit_on_error: Whether to exit on configuration errors

    Returns:
        SimpleSettings: Loaded configuration
    """
    loader = ConfigurationLoader(env_file)

    try:
        return loader.load_settings(validate=validate)
    except ConfigurationError as e:
        logger.error(f"Configuration error: {e.message}")

        if e.errors:
            logger.error("Detailed errors:")
            for field, error_info in e.errors.items():
                if isinstance(error_info, dict):
                    logger.error(f"  {field}: {error_info.get('message', error_info)}")
                else:
                    logger.error(f"  {field}: {error_info}")

        if exit_on_error:
            logger.error("Exiting due to configuration errors")
            sys.exit(1)
        else:
            raise
    except Exception as e:
        logger.error(f"Unexpected configuration error: {str(e)}")
        if exit_on_error:
            sys.exit(1)
        else:
            raise


def get_configuration_summary(settings: Settings) -> str:
    """
    Generate a human-readable configuration summary

    Args:
        settings: Application settings

    Returns:
        str: Configuration summary
    """
    summary_lines = [
        f"Environment: {settings.environment}",
        f"Debug Mode: {settings.debug}",
        "",
        "API Configuration:",
        f"  Host: {settings.api_host}",
        f"  Port: {settings.api_port}",
        f"  Log Level: {settings.api_log_level}",
        f"  Reload: {settings.api_reload}",
        "",
        "ETL Configuration:",
        f"  Base URL: {settings.imdb_base_url}",
        f"  Data Directory: {settings.data_dir}",
        f"  Chunk Size: {settings.chunk_size}",
        f"  Max Retries: {settings.max_retries}",
        "",
        "Database Configuration:",
        f"  Pool Size: {settings.db_pool_size}",
        f"  Max Overflow: {settings.db_max_overflow}",
        f"  Echo: {settings.db_echo}",
        "",
        "Monitoring Configuration:",
        f"  Metrics Enabled: {settings.metrics_enabled}",
        f"  Log Format: {settings.log_format}",
    ]

    return "\n".join(summary_lines)