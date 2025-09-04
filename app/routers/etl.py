"""
ETL API router for managing IMDb data ingestion
"""

import asyncio
from datetime import datetime
from typing import Optional
from enum import Enum

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.config.settings import get_settings
from app.database.connection import get_db
from app.database.service import DatabaseService
from app.etl import IMDbETL
from app.services.logger_service import get_logger, log_execution_time, handle_exceptions

# Create ETL router
router = APIRouter(prefix="/etl", tags=["ETL"])

# Get settings
settings = get_settings()

# Logger
logger = get_logger("etl_api")


class ETLFileType(str, Enum):
    """Available ETL file types"""
    PEOPLE = "people"
    TITLES = "titles"
    RATINGS = "ratings"
    PRINCIPALS = "principals"


class ETLStatus(str, Enum):
    """ETL status types"""
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"

@handle_exceptions("etl_api", "background_etl_task")
async def run_etl_background(file_type: Optional[str] = None):
    """Background task to run ETL pipeline"""
    logger.info(f"Starting background ETL task - file_type: {file_type}")
    
    try:
        etl = IMDbETL()
        
        if file_type:
            # Process single file
            logger.info(f"Processing single file - file_type: {file_type}")
            
            file_mapping = {
                'people': ('name_basics', etl.process_people),
                'titles': ('title_basics', etl.process_titles), 
                'ratings': ('title_ratings', etl.process_ratings),
                'principals': ('title_principals', etl.process_principals)
            }
            
            if file_type not in file_mapping:
                raise ValueError(f"Invalid file type: {file_type}")
            
            file_key, processor = file_mapping[file_type]
            
            # Download and process file
            file_path = etl.download_file(file_key)
            records = processor(file_path)
            
            logger.info(f"Single file processing completed - file_type: {file_type}, records_processed: {records}")
            
            # Refresh materialized view
            etl.refresh_materialized_view()
            
        else:
            # Run full pipeline
            logger.info("Starting full ETL pipeline")
            result = etl.run_full_pipeline()
            
            if result["success"]:
                logger.info("Full ETL pipeline completed successfully", 
                           duration_seconds=result["duration_seconds"],
                           records_processed=result["records_processed"],
                           run_id=result["run_id"])
            else:
                logger.error("Full ETL pipeline failed", 
                           error=result["error"],
                           run_id=result.get("run_id"))
                
    except Exception as e:
        logger.exception("ETL background task failed", 
                        file_type=file_type,
                        error=str(e))
        raise


@router.post("/start")
async def start_etl(
    background_tasks: BackgroundTasks,
    file_type: Optional[ETLFileType] = Query(None, description="Specific file to process (optional)"),
    force: bool = Query(False, description="Force start even if ETL is already running"),
    db: Session = Depends(get_db)
):
    """
    Start ETL pipeline
    
    - **file_type**: Optional specific file to process (people, titles, ratings, principals)
    - **force**: Force start even if another ETL is running
    
    Returns ETL run information and starts processing in background
    """
    logger.info("ETL start requested", 
               file_type=file_type.value if file_type else None,
               force=force)
    
    try:
        db_service = DatabaseService(db)
        
        # Check if ETL is already running (unless forced)
        if not force:
            latest_run = db_service.get_latest_etl_run()
            if latest_run and latest_run.status == "running":
                logger.warning("ETL already running", run_id=latest_run.id)
                raise HTTPException(
                    status_code=409, 
                    detail=f"ETL is already running (Run ID: {latest_run.id}). Use force=true to override."
                )
        
        # Create new ETL run record
        etl = IMDbETL()
        run_id = etl.log_etl_run("running", datetime.now())
        
        logger.info("ETL run created", run_id=run_id)
        
        # Start background task
        background_tasks.add_task(
            run_etl_background, 
            file_type.value if file_type else None
        )
        
        return {
            "message": "ETL started successfully",
            "run_id": run_id,
            "status": "running",
            "file_type": file_type.value if file_type else "all",
            "started_at": datetime.now().isoformat(),
            "estimated_duration": "30-60 minutes" if not file_type else "5-15 minutes"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Failed to start ETL", 
                        file_type=file_type.value if file_type else None,
                        error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to start ETL: {str(e)}")


@router.get("/status")
async def get_etl_status(
    run_id: Optional[int] = Query(None, description="Specific run ID to check"),
    db: Session = Depends(get_db)
):
    """
    Get ETL status
    
    - **run_id**: Optional specific run ID to check (defaults to latest)
    
    Returns detailed ETL status information
    """
    logger.debug("ETL status requested", run_id=run_id)
    
    try:
        db_service = DatabaseService(db)
        
        if run_id:
            etl_run = db_service.get_etl_run_by_id(run_id)
            if not etl_run:
                raise HTTPException(status_code=404, detail=f"ETL run {run_id} not found")
        else:
            etl_run = db_service.get_latest_etl_run()
            if not etl_run:
                return {
                    "message": "No ETL runs found",
                    "status": "never_run",
                    "timestamp": datetime.now().isoformat()
                }
        
        # Format status according to business requirements
        status_info = {
            "run_id": etl_run.id,
            "status": etl_run.status,
            "started_at": etl_run.started_at.isoformat() if etl_run.started_at else None,
            "finished_at": etl_run.finished_at.isoformat() if etl_run.finished_at else None,
            "duration_seconds": etl_run.duration_seconds,
            "records_processed": etl_run.records_processed,
            "error_message": etl_run.error_message,
            "timestamp": datetime.now().isoformat()
        }
        
        # Add formatted status message
        if etl_run.status == "completed" and etl_run.finished_at:
            status_info["message"] = f"Finished running successfully at {etl_run.finished_at.strftime('%d-%m-%Y %H:%M')}"
        elif etl_run.status == "failed" and etl_run.finished_at:
            status_info["message"] = f"Finished running unsuccessfully at {etl_run.finished_at.strftime('%d-%m-%Y %H:%M')}"
        elif etl_run.status == "running" and etl_run.started_at:
            status_info["message"] = f"Started running at {etl_run.started_at.strftime('%d-%m-%Y %H:%M')}"
        else:
            status_info["message"] = f"Status: {etl_run.status}"
        
        logger.debug("ETL status retrieved", 
                    run_id=etl_run.id,
                    status=etl_run.status)
        
        return status_info
        
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Failed to get ETL status", 
                        run_id=run_id,
                        error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to get ETL status: {str(e)}")


@router.get("/history")
async def get_etl_history(
    limit: int = Query(10, ge=1, le=100, description="Number of runs to return"),
    db: Session = Depends(get_db)
):
    """
    Get ETL run history
    
    - **limit**: Number of recent runs to return (1-100)
    
    Returns list of recent ETL runs with their status
    """
    logger.debug(f"ETL history requested - limit: {limit}")
    
    try:
        db_service = DatabaseService(db)
        etl_runs = db_service.get_etl_runs(limit=limit)
        
        history = []
        for run in etl_runs:
            run_info = {
                "run_id": run.id,
                "status": run.status,
                "started_at": run.started_at.isoformat() if run.started_at else None,
                "finished_at": run.finished_at.isoformat() if run.finished_at else None,
                "duration_seconds": run.duration_seconds,
                "records_processed": run.records_processed,
                "error_message": run.error_message if run.status == "failed" else None
            }
            history.append(run_info)
        
        logger.debug(f"ETL history retrieved - count: {len(history)}")
        
        return {
            "runs": history,
            "total_returned": len(history),
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to get ETL history - limit: {limit}, error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get ETL history: {str(e)}")


@router.post("/refresh-view")
async def refresh_materialized_view(db: Session = Depends(get_db)):
    """
    Refresh the actor_ratings materialized view
    
    This endpoint refreshes the materialized view that powers the actors API.
    Use this after manual data updates or to ensure data consistency.
    """
    logger.info(f"Materialized view refresh requested")
    
    try:
        etl = IMDbETL()
        etl.refresh_materialized_view()
        
        logger.info(f"Materialized view refreshed successfully")
        
        return {
            "message": "Materialized view refreshed successfully",
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to refresh materialized view - error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to refresh materialized view: {str(e)}")


@router.delete("/cancel/{run_id}")
async def cancel_etl_run(run_id: int, db: Session = Depends(get_db)):
    """
    Cancel a running ETL process
    
    - **run_id**: ID of the ETL run to cancel
    
    Note: This marks the run as failed in the database but cannot stop 
    background processes that are already running.
    """
    logger.info("ETL cancellation requested", run_id=run_id)
    
    try:
        db_service = DatabaseService(db)
        etl_run = db_service.get_etl_run_by_id(run_id)
        
        if not etl_run:
            raise HTTPException(status_code=404, detail=f"ETL run {run_id} not found")
        
        if etl_run.status != "running":
            raise HTTPException(
                status_code=400, 
                detail=f"ETL run {run_id} is not running (status: {etl_run.status})"
            )
        
        # Update status to failed
        etl = IMDbETL()
        etl.log_etl_run(
            "failed", 
            etl_run.started_at, 
            datetime.now(),
            etl_run.records_processed or 0,
            "Cancelled by user"
        )
        
        logger.info("ETL run cancelled", run_id=run_id)
        
        return {
            "message": f"ETL run {run_id} cancelled",
            "run_id": run_id,
            "status": "cancelled",
            "timestamp": datetime.now().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Failed to cancel ETL run", 
                        run_id=run_id,
                        error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to cancel ETL run: {str(e)}")