"""
Service layer for the IMDb application.

This module provides service classes that encapsulate business logic
and integrate with resilience patterns like circuit breakers.
"""

from .monitoring_service import get_monitoring_service

__all__ = ["get_monitoring_service"]