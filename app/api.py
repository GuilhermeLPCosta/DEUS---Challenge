import uvicorn
from fastapi import FastAPI

from app.config.settings import get_settings
from app.database.connection import init_database
from app.routers.main import router as main_router
from app.routers.etl import router as etl_router

# Initialize configuration
settings = get_settings()

# Initialize FastAPI app
app = FastAPI(
    title=settings.api_title,
    version=settings.api_version,
    description="API to retrieve IMDb actors/actresses ordered by rating score",
)

# Include routers
app.include_router(main_router)
app.include_router(etl_router)


@app.on_event("startup")
async def startup_event():
    """Initialize database on startup"""
    try:
        # Initialize database
        init_database()
        print(f"API server starting up - version {settings.api_version}")
    except Exception as e:
        print(f"Warning: Failed to initialize database: {e}")
        print("API will start but database operations may fail")
        # Don't raise - allow API to start even if DB init fails


def main():
    """Main entry point for API server"""
    print(f"Starting API server on {settings.api_host}:{settings.api_port}")

    uvicorn.run(
        "app.api:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=settings.api_reload,
        log_level=settings.api_log_level,
    )


if __name__ == "__main__":
    main()
    