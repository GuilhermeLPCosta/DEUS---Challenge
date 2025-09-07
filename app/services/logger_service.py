"""
Centralized logging service with structured logging, exception handling, and context management
"""

import json
import logging
import sys
import traceback
from datetime import datetime
from typing import Any, Dict, Optional, Union
from contextlib import contextmanager
from functools import wraps
from pathlib import Path

from app.config.settings import get_settings


class StructuredFormatter(logging.Formatter):
    """Custom formatter for structured JSON logging"""
    
    def format(self, record: logging.LogRecord) -> str:
        # Base log entry
        log_entry = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }
        
        # Add extra fields if present
        if hasattr(record, 'extra_fields'):
            log_entry.update(record.extra_fields)
            
        # Add exception info if present
        if record.exc_info:
            log_entry["exception"] = {
                "type": record.exc_info[0].__name__,
                "message": str(record.exc_info[1]),
                "traceback": traceback.format_exception(*record.exc_info)
            }
            
        return json.dumps(log_entry, default=str)


class LoggerService:
    """Centralized logging service with context management"""
    
    def __init__(self):
        self.settings = get_settings()
        self._loggers: Dict[str, logging.Logger] = {}
        self._context: Dict[str, Any] = {}
        self._setup_root_logger()
    
    def _setup_root_logger(self):
        """Setup root logger configuration"""
        root_logger = logging.getLogger()
        root_logger.setLevel(getattr(logging, self.settings.log_level.upper()))
        
        # Remove existing handlers
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
        
        # Create console handler
        console_handler = logging.StreamHandler(sys.stdout)
        
        if self.settings.log_format.lower() == "json":
            formatter = StructuredFormatter()
        else:
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
        
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)
    
    def get_logger(self, name: str) -> 'ContextLogger':
        """Get or create a logger with the given name"""
        if name not in self._loggers:
            logger = logging.getLogger(name)
            self._loggers[name] = logger
        
        return ContextLogger(self._loggers[name], self._context.copy())
    
    def set_context(self, **kwargs):
        """Set global context for all loggers"""
        self._context.update(kwargs)
    
    def clear_context(self):
        """Clear global context"""
        self._context.clear()
    
    @contextmanager
    def context(self, **kwargs):
        """Temporary context manager for logging"""
        old_context = self._context.copy()
        self._context.update(kwargs)
        try:
            yield
        finally:
            self._context = old_context


class ContextLogger:
    """Logger wrapper with context support"""
    
    def __init__(self, logger: logging.Logger, context: Dict[str, Any]):
        self._logger = logger
        self._context = context
    
    def _log(self, level: int, message: str, **kwargs):
        """Internal logging method with context"""
        extra_fields = self._context.copy()
        extra_fields.update(kwargs)
        
        # Create a custom LogRecord with extra fields
        record = self._logger.makeRecord(
            self._logger.name, level, "", 0, message, (), None
        )
        record.extra_fields = extra_fields
        self._logger.handle(record)
    
    def debug(self, message: str, **kwargs):
        """Log debug message"""
        self._log(logging.DEBUG, message, **kwargs)
    
    def info(self, message: str, **kwargs):
        """Log info message"""
        self._log(logging.INFO, message, **kwargs)
    
    def warning(self, message: str, **kwargs):
        """Log warning message"""
        self._log(logging.WARNING, message, **kwargs)
    
    def error(self, message: str, **kwargs):
        """Log error message"""
        self._log(logging.ERROR, message, **kwargs)
    
    def critical(self, message: str, **kwargs):
        """Log critical message"""
        self._log(logging.CRITICAL, message, **kwargs)
    
    def exception(self, message: str, **kwargs):
        """Log exception with traceback"""
        kwargs['exc_info'] = True
        self._log(logging.ERROR, message, **kwargs)
    
    def with_context(self, **kwargs) -> 'ContextLogger':
        """Create new logger with additional context"""
        new_context = self._context.copy()
        new_context.update(kwargs)
        return ContextLogger(self._logger, new_context)


class ExceptionHandler:
    """Centralized exception handling with logging"""
    
    def __init__(self, logger_service: LoggerService):
        self.logger_service = logger_service
    
    def handle_exception(self, 
                        exc: Exception, 
                        context: str = "unknown",
                        logger_name: str = "exception_handler",
                        **extra_context) -> Dict[str, Any]:
        """Handle exception with proper logging and return error info"""
        logger = self.logger_service.get_logger(logger_name)
        
        error_info = {
            "error_type": type(exc).__name__,
            "error_message": str(exc),
            "context": context,
            "timestamp": datetime.utcnow().isoformat(),
        }
        error_info.update(extra_context)
        
        logger.exception(f"Exception in {context}", **error_info)
        
        return error_info
    
    def log_and_raise(self, 
                     exc: Exception, 
                     context: str = "unknown",
                     logger_name: str = "exception_handler",
                     **extra_context):
        """Log exception and re-raise it"""
        self.handle_exception(exc, context, logger_name, **extra_context)
        raise exc


def log_execution_time(logger_name: str = "performance"):
    """Decorator to log function execution time"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            logger = get_logger_service().get_logger(logger_name)
            start_time = datetime.utcnow()
            
            try:
                logger.debug(f"Starting {func.__name__}", 
                           function=func.__name__, 
                           module=func.__module__)
                
                result = func(*args, **kwargs)
                
                end_time = datetime.utcnow()
                duration = (end_time - start_time).total_seconds()
                
                logger.info(f"Completed {func.__name__}", 
                          function=func.__name__,
                          module=func.__module__,
                          duration_seconds=duration,
                          success=True)
                
                return result
                
            except Exception as e:
                end_time = datetime.utcnow()
                duration = (end_time - start_time).total_seconds()
                
                logger.error(f"Failed {func.__name__}", 
                           function=func.__name__,
                           module=func.__module__,
                           duration_seconds=duration,
                           success=False,
                           error=str(e))
                raise
        
        return wrapper
    return decorator


def handle_exceptions(logger_name: str = "exception_handler", context: str = None):
    """Decorator to handle exceptions with logging"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                exception_handler = get_exception_handler()
                func_context = context or f"{func.__module__}.{func.__name__}"
                exception_handler.handle_exception(e, func_context, logger_name)
                raise
        return wrapper
    return decorator


# Global instances
_logger_service: Optional[LoggerService] = None
_exception_handler: Optional[ExceptionHandler] = None


def get_logger_service() -> LoggerService:
    """Get global logger service instance"""
    global _logger_service
    if _logger_service is None:
        _logger_service = LoggerService()
    return _logger_service


def get_exception_handler() -> ExceptionHandler:
    """Get global exception handler instance"""
    global _exception_handler
    if _exception_handler is None:
        _exception_handler = ExceptionHandler(get_logger_service())
    return _exception_handler


def get_logger(name: str) -> ContextLogger:
    """Convenience function to get a logger"""
    return get_logger_service().get_logger(name)