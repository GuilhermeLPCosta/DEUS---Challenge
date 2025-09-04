"""
Consolidated database service using SQLAlchemy ORM
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import and_, desc
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.services.logger_service import get_logger

from .models import ActorRating, ETLRun

@dataclass
class ActorSearchResult:
    """Actor search result with pagination info"""

    actors: List[Dict[str, Any]]
    total_count: int
    limit: int
    offset: int
    profession: str


@dataclass
class ETLRunResult:
    """ETL run result"""

    id: int
    started_at: datetime
    finished_at: Optional[datetime]
    status: str
    records_processed: Optional[int]
    error_message: Optional[str]
    duration_seconds: Optional[int]


class DatabaseService:
    """Consolidated database service using SQLAlchemy ORM"""

    def __init__(self, session: Session):
        self.session = session
        self.logger = get_logger("database.service")

    # Actor-related methods
    def get_actors_paginated(self, profession: str, limit: int = 100, offset: int = 0) -> ActorSearchResult:
        """Get paginated list of actors by profession"""
        self.logger.debug(f"Getting paginated actors - profession: {profession}, limit: {limit}, offset: {offset}")

        try:
            # Get total count
            total_count = self.session.query(ActorRating).filter(ActorRating.profession == profession).count()

            # Get actors with pagination
            actors_query = (
                self.session.query(ActorRating)
                .filter(ActorRating.profession == profession)
                .order_by(desc(ActorRating.score), desc(ActorRating.number_of_titles))
                .limit(limit)
                .offset(offset)
            )

            actors = [actor.to_dict() for actor in actors_query.all()]

            self.logger.info(
                f"Retrieved paginated actors - profession: {profession}, total: {total_count}, returned: {len(actors)}, limit: {limit}, offset: {offset}"
            )

            return ActorSearchResult(
                actors=actors, total_count=total_count, limit=limit, offset=offset, profession=profession
            )

        except SQLAlchemyError as e:
            self.logger.error(f"Failed to get paginated actors - profession: {profession}, error: {str(e)}")
            raise

    def search_actors_by_name(
        self, profession: str, search_query: str, limit: int = 100, offset: int = 0
    ) -> ActorSearchResult:
        """Search actors by name with pagination"""
        self.logger.debug(
            f"Searching actors by name - profession: {profession}, query: {search_query}, limit: {limit}, offset: {offset}"
        )

        try:
            # Create search filter
            search_filter = and_(
                ActorRating.profession == profession, ActorRating.primary_name.ilike(f"%{search_query}%")
            )

            # Get total count for search
            total_count = self.session.query(ActorRating).filter(search_filter).count()

            # Get search results
            actors_query = (
                self.session.query(ActorRating)
                .filter(search_filter)
                .order_by(desc(ActorRating.score), desc(ActorRating.number_of_titles))
                .limit(limit)
                .offset(offset)
            )

            actors = [actor.to_dict() for actor in actors_query.all()]

            self.logger.info(
                f"Search completed - profession: {profession}, query: {search_query}, total: {total_count}, returned: {len(actors)}"
            )

            return ActorSearchResult(
                actors=actors, total_count=total_count, limit=limit, offset=offset, profession=profession
            )

        except SQLAlchemyError as e:
            self.logger.error(
                f"Failed to search actors by name - profession: {profession}, query: {search_query}, error: {str(e)}"
            )
            raise

    def get_actor_by_name(self, name: str, profession: str) -> Optional[Dict[str, Any]]:
        """Get actor by exact name and profession"""
        self.logger.debug(f"Getting actor by name - name: {name}, profession: {profession}")

        try:
            actor = (
                self.session.query(ActorRating)
                .filter(and_(ActorRating.primary_name == name, ActorRating.profession == profession))
                .first()
            )

            if actor:
                self.logger.info(f"Actor found - name: {name}, profession: {profession}")
                return actor.to_dict()

            self.logger.warning(f"Actor not found - name: {name}, profession: {profession}")
            return None

        except SQLAlchemyError as e:
            self.logger.error("Failed to get actor by name", name=name, profession=profession, error=str(e))
            raise

    def get_top_actors(self, profession: str, min_titles: int = 5, limit: int = 100) -> List[Dict[str, Any]]:
        """Get top-rated actors with minimum number of titles"""
        self.logger.debug("Getting top actors", profession=profession, min_titles=min_titles, limit=limit)

        try:
            actors_query = (
                self.session.query(ActorRating)
                .filter(and_(ActorRating.profession == profession, ActorRating.number_of_titles >= min_titles))
                .order_by(desc(ActorRating.score))
                .limit(limit)
            )

            actors = [actor.to_dict() for actor in actors_query.all()]

            self.logger.info("Top actors retrieved", profession=profession, min_titles=min_titles, count=len(actors))

            return actors

        except SQLAlchemyError as e:
            self.logger.error("Failed to get top actors", profession=profession, error=str(e))
            raise

    def count_actors_by_profession(self, profession: str) -> int:
        """Count total actors by profession"""
        try:
            count = self.session.query(ActorRating).filter(ActorRating.profession == profession).count()

            self.logger.debug("Counted actors by profession", profession=profession, count=count)

            return count

        except SQLAlchemyError as e:
            self.logger.error("Failed to count actors by profession", profession=profession, error=str(e))
            raise

    # ETL-related methods
    def create_etl_run(self) -> ETLRunResult:
        """Create a new ETL run record"""
        self.logger.debug("Creating new ETL run")

        try:
            etl_run = ETLRun(started_at=datetime.utcnow(), status="running")

            self.session.add(etl_run)
            self.session.commit()

            result = ETLRunResult(
                id=etl_run.id,
                started_at=etl_run.started_at,
                finished_at=etl_run.finished_at,
                status=etl_run.status,
                records_processed=etl_run.records_processed,
                error_message=etl_run.error_message,
                duration_seconds=etl_run.duration_seconds,
            )

            self.logger.info("ETL run created", etl_run_id=etl_run.id)
            return result

        except SQLAlchemyError as e:
            self.session.rollback()
            self.logger.error("Failed to create ETL run", error=str(e))
            raise

    def update_etl_run(
        self, etl_run_id: int, status: str, records_processed: Optional[int] = None, error_message: Optional[str] = None
    ) -> ETLRunResult:
        """Update ETL run status"""
        self.logger.debug("Updating ETL run", etl_run_id=etl_run_id, status=status)

        try:
            etl_run = self.session.query(ETLRun).filter(ETLRun.id == etl_run_id).first()

            if not etl_run:
                raise ValueError(f"ETL run {etl_run_id} not found")

            etl_run.status = status
            if records_processed is not None:
                etl_run.records_processed = records_processed
            if error_message is not None:
                etl_run.error_message = error_message

            if status in ["completed", "failed"]:
                etl_run.finished_at = datetime.utcnow()
                if etl_run.started_at:
                    duration = etl_run.finished_at - etl_run.started_at
                    etl_run.duration_seconds = int(duration.total_seconds())

            self.session.commit()

            result = ETLRunResult(
                id=etl_run.id,
                started_at=etl_run.started_at,
                finished_at=etl_run.finished_at,
                status=etl_run.status,
                records_processed=etl_run.records_processed,
                error_message=etl_run.error_message,
                duration_seconds=etl_run.duration_seconds,
            )

            self.logger.info("ETL run updated", etl_run_id=etl_run_id, status=status)
            return result

        except SQLAlchemyError as e:
            self.session.rollback()
            self.logger.error("Failed to update ETL run", etl_run_id=etl_run_id, error=str(e))
            raise

    def get_latest_etl_run(self) -> Optional[ETLRunResult]:
        """Get the latest ETL run"""
        self.logger.debug("Getting latest ETL run")

        try:
            etl_run = self.session.query(ETLRun).order_by(desc(ETLRun.started_at)).first()

            if etl_run:
                result = ETLRunResult(
                    id=etl_run.id,
                    started_at=etl_run.started_at,
                    finished_at=etl_run.finished_at,
                    status=etl_run.status,
                    records_processed=etl_run.records_processed,
                    error_message=etl_run.error_message,
                    duration_seconds=etl_run.duration_seconds,
                )

                self.logger.debug("Latest ETL run found", etl_run_id=etl_run.id)
                return result

            self.logger.debug("No ETL runs found")
            return None

        except SQLAlchemyError as e:
            self.logger.error("Failed to get latest ETL run", error=str(e))
            raise

    def get_etl_runs(self, limit: int = 10) -> List[ETLRunResult]:
        """Get recent ETL runs"""
        self.logger.debug("Getting ETL runs", limit=limit)

        try:
            etl_runs = self.session.query(ETLRun).order_by(desc(ETLRun.started_at)).limit(limit).all()

            results = [
                ETLRunResult(
                    id=run.id,
                    started_at=run.started_at,
                    finished_at=run.finished_at,
                    status=run.status,
                    records_processed=run.records_processed,
                    error_message=run.error_message,
                    duration_seconds=run.duration_seconds,
                )
                for run in etl_runs
            ]

            self.logger.debug("ETL runs retrieved", count=len(results))
            return results

        except SQLAlchemyError as e:
            self.logger.error("Failed to get ETL runs", error=str(e))
            raise

    def get_etl_run_by_id(self, etl_run_id: int) -> Optional[ETLRunResult]:
        """Get ETL run by ID"""
        self.logger.debug("Getting ETL run by ID", etl_run_id=etl_run_id)

        try:
            etl_run = self.session.query(ETLRun).filter(ETLRun.id == etl_run_id).first()

            if etl_run:
                result = ETLRunResult(
                    id=etl_run.id,
                    started_at=etl_run.started_at,
                    finished_at=etl_run.finished_at,
                    status=etl_run.status,
                    records_processed=etl_run.records_processed,
                    error_message=etl_run.error_message,
                    duration_seconds=etl_run.duration_seconds,
                )

                self.logger.debug("ETL run found", etl_run_id=etl_run_id, status=etl_run.status)
                return result

            self.logger.debug("ETL run not found", etl_run_id=etl_run_id)
            return None

        except SQLAlchemyError as e:
            self.logger.error("Failed to get ETL run by ID", etl_run_id=etl_run_id, error=str(e))
            raise

    # Health check methods
    def check_database_health(self) -> Dict[str, Any]:
        """Check database connectivity and basic health"""
        self.logger.debug("Checking database health")

        try:
            # Test basic connectivity
            from sqlalchemy import text

            self.session.execute(text("SELECT 1"))

            # Get table counts
            actor_count = self.session.query(ActorRating).count()
            etl_run_count = self.session.query(ETLRun).count()

            health_info = {
                "status": "healthy",
                "actor_count": actor_count,
                "etl_run_count": etl_run_count,
                "connection_active": True,
            }

            self.logger.debug("Database health check passed", **health_info)
            return health_info

        except SQLAlchemyError as e:
            health_info = {"status": "unhealthy", "error": str(e), "connection_active": False}

            self.logger.error("Database health check failed", error=str(e))
            return health_info

    # Utility methods
    def execute_raw_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """Execute raw SQL query (for migrations, etc.)"""
        self.logger.debug("Executing raw query", query=query[:100] + "..." if len(query) > 100 else query)

        try:
            from sqlalchemy import text

            result = self.session.execute(text(query), params or {})
            return result

        except SQLAlchemyError as e:
            self.logger.error(
                "Raw query execution failed", query=query[:100] + "..." if len(query) > 100 else query, error=str(e)
            )
            raise

    def commit(self):
        """Commit current transaction"""
        try:
            self.session.commit()
        except SQLAlchemyError as e:
            self.session.rollback()
            self.logger.error("Failed to commit transaction", error=str(e))
            raise

    def rollback(self):
        """Rollback current transaction"""
        self.session.rollback()

    def close(self):
        """Close the database session"""
        if self.session:
            self.session.close()