from fastapi import FastAPI, HTTPException, Depends, Query
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List, Optional
import logging
from datetime import datetime
import uvicorn

from app.config import Config
from app.database import get_db, init_database

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title=Config.API_TITLE,
    version=Config.API_VERSION,
    description="API to retrieve IMDb actors/actresses ordered by rating score"
)

# Pydantic models for API responses
from pydantic import BaseModel

class ActorResponse(BaseModel):
    name: str
    score: float
    number_of_titles: int
    total_runtime_minutes: int

class ActorsListResponse(BaseModel):
    actors: List[ActorResponse]
    total: int
    limit: int
    offset: int
    profession: str

class ETLStatusResponse(BaseModel):
    status: str
    last_run: Optional[str] = None
    started_at: Optional[str] = None
    records_processed: Optional[int] = None
    duration_seconds: Optional[float] = None

@app.on_event("startup")
async def startup_event():
    """Initialize database on startup"""
    try:
        init_database()
        logger.info("Database initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        raise

@app.get("/")
async def root():
    """Root endpoint with API information"""
    return {
        "message": "IMDb Actors Rating API",
        "version": Config.API_VERSION,
        "endpoints": {
            "actors": "/actors?profession=actor&limit=100&offset=0",
            "status": "/status"
        }
    }

@app.get("/actors", response_model=ActorsListResponse)
async def get_actors(
    profession: str = Query(..., description="Profession: 'actor' or 'actress'"),
    limit: int = Query(Config.DEFAULT_LIMIT, ge=1, le=Config.MAX_LIMIT, 
                      description="Number of results to return"),
    offset: int = Query(0, ge=0, description="Number of results to skip"),
    db: Session = Depends(get_db)
):
    """
    Get list of actors/actresses ordered by rating score
    
    - **profession**: Must be 'actor' or 'actress'
    - **limit**: Number of results (1-1000, default 100)
    - **offset**: Pagination offset (default 0)
    """
    
    # Validate profession
    if profession not in Config.TARGET_PROFESSIONS:
        raise HTTPException(
            status_code=400, 
            detail=f"Profession must be one of: {', '.join(Config.TARGET_PROFESSIONS)}"
        )
    
    try:
        # Get total count
        count_query = text("""
            SELECT COUNT(*) 
            FROM actor_ratings 
            WHERE profession = :profession
        """)
        
        total_result = db.execute(count_query, {"profession": profession})
        total = total_result.fetchone()[0]
        
        # Get actors data
        actors_query = text("""
            SELECT 
                primary_name as name,
                ROUND(score::numeric, 2) as score,
                number_of_titles,
                total_runtime_minutes
            FROM actor_ratings
            WHERE profession = :profession
            ORDER BY score DESC, number_of_titles DESC
            LIMIT :limit OFFSET :offset
        """)
        
        result = db.execute(actors_query, {
            "profession": profession,
            "limit": limit,
            "offset": offset
        })
        
        actors = []
        for row in result:
            actors.append(ActorResponse(
                name=row.name,
                score=float(row.score or 0),
                number_of_titles=row.number_of_titles or 0,
                total_runtime_minutes=row.total_runtime_minutes or 0
            ))
        
        return ActorsListResponse(
            actors=actors,
            total=total,
            limit=limit,
            offset=offset,
            profession=profession
        )
        
    except Exception as e:
        logger.error(f"Error fetching actors: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/status", response_model=ETLStatusResponse)
async def get_etl_status(db: Session = Depends(get_db)):
    """
    Get ETL pipeline status
    
    Returns information about the last ETL run including:
    - Status (success/failed/running)
    - Start and finish times
    - Number of records processed
    - Duration
    """
    
    try:
        # Get latest ETL run
        status_query = text("""
            SELECT 
                status,
                started_at,
                finished_at,
                records_processed,
                error_message
            FROM etl_runs 
            ORDER BY started_at DESC 
            LIMIT 1
        """)
        
        result = db.execute(status_query)
        row = result.fetchone()
        
        if not row:
            return ETLStatusResponse(
                status="No ETL runs found",
                last_run=None,
                started_at=None,
                records_processed=None
            )
        
        status_text = row.status
        if status_text == 'success':
            status_text = f"Finished running successfully at {row.finished_at.strftime('%d-%m-%Y %H:%M')}"
        elif status_text == 'failed':
            status_text = f"Finished running unsuccessfully at {row.finished_at.strftime('%d-%m-%Y %H:%M')}"
        elif status_text == 'running':
            status_text = f"Started running at {row.started_at.strftime('%d-%m-%Y %H:%M')}"
        
        # Calculate duration if both times are available
        duration_seconds = None
        if row.finished_at and row.started_at:
            duration = row.finished_at - row.started_at
            duration_seconds = duration.total_seconds()
        
        return ETLStatusResponse(
            status=status_text,
            last_run=row.finished_at.strftime('%d-%m-%Y %H:%M') if row.finished_at else None,
            started_at=row.started_at.strftime('%d-%m-%Y %H:%M') if row.started_at else None,
            records_processed=row.records_processed,
            duration_seconds=duration_seconds
        )
        
    except Exception as e:
        logger.error(f"Error fetching ETL status: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/health")
async def health_check(db: Session = Depends(get_db)):
    """Health check endpoint"""
    try:
        # Simple database connectivity check
        db.execute(text("SELECT 1"))
        return {"status": "healthy", "timestamp": datetime.now().isoformat()}
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail="Service unavailable")

# Error handlers
@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail, "timestamp": datetime.now().isoformat()}
    )

@app.exception_handler(500)
async def internal_server_error_handler(request, exc):
    return JSONResponse(
        status_code=500,
        content={
            "detail": "Internal server error",
            "timestamp": datetime.now().isoformat()
        }
    )

def main():
    """Main entry point for API server"""
    uvicorn.run(
        "app.api:app",
        host=Config.API_HOST,
        port=Config.API_PORT,
        reload=False,
        log_level="info"
    )

if __name__ == "__main__":
    main()