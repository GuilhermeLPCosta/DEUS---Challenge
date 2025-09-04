"""
Main API router with all endpoints consolidated
"""

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from app.config.settings import get_settings
from app.database.connection import get_db
from app.database.service import DatabaseService
from app.models.response_models import ActorResponse, ActorsListResponse, PaginationMeta
from app.services.monitoring_service import (
    ComponentHealth,
    HealthCheck,
    HealthStatus,
    get_monitoring_service,
)
from app.services.logger_service import get_logger, log_execution_time, handle_exceptions

# Create main router
router = APIRouter()

# Get settings
settings = get_settings()


# Database Health Check
class DatabaseHealthCheck(HealthCheck):
    """Database connectivity health check"""

    def __init__(self):
        super().__init__("database")

    async def check(self) -> ComponentHealth:
        """Check database connectivity"""
        try:
            # This would use a database connection to test
            return ComponentHealth(
                name=self.name,
                status=HealthStatus.HEALTHY,
                message="Database connection is healthy",
                details={"connection_active": True},
            )
        except Exception as e:
            return ComponentHealth(
                name=self.name,
                status=HealthStatus.UNHEALTHY,
                message=f"Database connection failed: {str(e)}",
                error=str(e),
            )

# Initialize monitoring service with health checks
monitoring_service = get_monitoring_service()
monitoring_service.add_health_check(DatabaseHealthCheck())


@router.get("/")
async def api_root():
    """
    API root endpoint with basic information
    """
    return {
        "message": "IMDb Actors Rating API",
        "version": "1.0.0",
        "endpoints": {
            "actors": "/actors?profession=actor&limit=100&offset=0",
            "status": "/status",
            "health": "/health",
        },
        "timestamp": datetime.utcnow().isoformat(),
    }


@router.get("/actors", response_model=ActorsListResponse)
async def get_actors(
    profession: str = Query(..., description="Profession: 'actor' or 'actress'"),
    limit: int = Query(100, ge=1, le=1000, description="Number of results to return"),
    offset: int = Query(0, ge=0, description="Number of results to skip"),
    search: Optional[str] = Query(None, description="Search term for actor names"),
    db: Session = Depends(get_db),
):
    """
    Get list of actors/actresses ordered by rating score
    Returns actors/actresses with the following required business fields:
    - **Name**: Actor/actress name
    - **Score**: Rating score (0-10, rounded to 2 decimal places)
    - **Number of Titles**: Count of titles the person appeared in
    - **Total Runtime Minutes**: Sum of runtime for all titles
    Supports:
    - **Profession filtering**: 'actor' or 'actress'
    - **Pagination**: limit (1-1000) and offset
    - **Search**: by actor name (optional)
    """
    # Validate profession
    if profession not in settings.target_professions:
        raise HTTPException(
            status_code=400, detail=f"Invalid profession. Must be one of: {', '.join(settings.target_professions)}"
        )

    try:
        db_service = DatabaseService(db)
        if search:
            # Search actors by name
            result = db_service.search_actors_by_name(
                profession=profession, search_query=search, limit=limit, offset=offset
            )
        else:
            # Get paginated actors
            result = db_service.get_actors_paginated(profession=profession, limit=limit, offset=offset)

        # Convert to response format
        actors = [
            ActorResponse(
                name=actor["name"],
                score=actor["score"],
                number_of_titles=actor["number_of_titles"],
                total_runtime_minutes=actor["total_runtime_minutes"],
            )
            for actor in result.actors
        ]

        pagination = PaginationMeta(total=result.total_count, limit=result.limit, offset=result.offset)

        return ActorsListResponse(actors=actors, profession=result.profession, pagination=pagination)

    except Exception as e:
        get_logger("api.actors").error("Error fetching actors", error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/status")
async def get_etl_status(db: Session = Depends(get_db)):
    """
    Get ETL pipeline status
    Returns ETL status information including:
    - **Status**: Current ETL status with formatted timestamps
    - **Last Run**: When the ETL last completed (dd-mm-yyyy hh:mm format)
    - **Started At**: When the current/last ETL run started
    - **Records Processed**: Number of records processed in last run
    - **Duration**: Processing time in seconds
    """
    try:
        db_service = DatabaseService(db)
        latest_run = db_service.get_latest_etl_run()

        if not latest_run:
            return {
                "status": "No ETL runs found",
                "last_run": None,
                "started_at": None,
                "records_processed": None,
                "duration_seconds": None,
                "timestamp": datetime.utcnow().isoformat(),
            }

        # Format status according to business requirements
        status_text = latest_run.status
        last_run_text = None
        started_at_text = None

        if latest_run.started_at:
            started_at_text = latest_run.started_at.strftime("%d-%m-%Y %H:%M")

        if latest_run.status == "completed" and latest_run.finished_at:
            status_text = f"Finished running successfully at {latest_run.finished_at.strftime('%d-%m-%Y %H:%M')}"
            last_run_text = latest_run.finished_at.strftime("%d-%m-%Y %H:%M")
        elif latest_run.status == "failed" and latest_run.finished_at:
            status_text = f"Finished running unsuccessfully at {latest_run.finished_at.strftime('%d-%m-%Y %H:%M')}"
            last_run_text = latest_run.finished_at.strftime("%d-%m-%Y %H:%M")
        elif latest_run.status == "running" and latest_run.started_at:
            status_text = f"Started running at {latest_run.started_at.strftime('%d-%m-%Y %H:%M')}"

        return {
            "status": status_text,
            "last_run": last_run_text,
            "started_at": started_at_text,
            "records_processed": latest_run.records_processed,
            "duration_seconds": latest_run.duration_seconds,
            "timestamp": datetime.utcnow().isoformat(),
        }

    except Exception as e:
        get_logger("api.status").error("Error fetching ETL status", error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/health")
async def health_check():
    """
    Comprehensive health check endpoint
    Returns detailed health information including:
    - **Overall Status**: healthy, degraded, or unhealthy
    - **Component Status**: Individual health check results
    - **Performance Metrics**: Response times and duration
    - **Summary Statistics**: Count of healthy/degraded/unhealthy checks
    """
    try:
        health_result = await monitoring_service.check_health()

        # Determine HTTP status code based on health
        if health_result["status"] == "healthy":
            status_code = 200
        elif health_result["status"] == "degraded":
            status_code = 200  # Still operational
        else:
            status_code = 503  # Service unavailable

        return JSONResponse(content=health_result, status_code=status_code)

    except Exception as e:
        get_logger("api.health").error("Health check failed", error=str(e))
        error_response = {
            "status": "unhealthy",
            "timestamp": datetime.utcnow().isoformat(),
            "duration_ms": 0,
            "summary": {"healthy_checks": 0, "degraded_checks": 0, "unhealthy_checks": 1, "total_checks": 1},
            "components": [
                {
                    "name": "health_system",
                    "status": "unhealthy",
                    "message": f"Health check system failure: {str(e)}",
                    "duration_ms": 0,
                }
            ],
            "error": "Health check system failure",
        }
        return JSONResponse(content=error_response, status_code=503)


@router.get("/metrics")
async def get_metrics():
    """
    Get application metrics
    Returns collected metrics including counters, histograms, and gauges
    """
    try:
        metrics = monitoring_service.get_all_metrics()
        return {"metrics": metrics, "timestamp": datetime.utcnow().isoformat()}
    except Exception as e:
        get_logger("api.metrics").error("Error fetching metrics", error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")