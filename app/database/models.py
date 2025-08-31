"""
SQLAlchemy ORM models for database tables
"""

from sqlalchemy import Boolean, Column, DateTime, Float, Integer, String, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func

Base = declarative_base()


class ActorRating(Base):
    """Actor ratings materialized view as ORM model"""

    __tablename__ = "actor_ratings"

    id = Column(Integer, primary_key=True, autoincrement=True)
    primary_name = Column(String(255), nullable=False, index=True)
    profession = Column(String(50), nullable=False, index=True)
    score = Column(Float, nullable=False, index=True)
    number_of_titles = Column(Integer, nullable=False)
    total_runtime_minutes = Column(Integer, nullable=False)

    def to_dict(self):
        """Convert to dictionary"""
        return {
            "name": self.primary_name,
            "score": round(float(self.score or 0), 2),
            "number_of_titles": self.number_of_titles or 0,
            "total_runtime_minutes": self.total_runtime_minutes or 0,
            "profession": self.profession,
        }


class ETLRun(Base):
    """ETL run tracking table"""

    __tablename__ = "etl_runs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    started_at = Column(DateTime, nullable=False, default=func.now())
    finished_at = Column(DateTime, nullable=True)
    status = Column(String(20), nullable=False, default="running")  # running, completed, failed
    records_processed = Column(Integer, nullable=True)
    error_message = Column(Text, nullable=True)
    duration_seconds = Column(Integer, nullable=True)

    def to_dict(self):
        """Convert to dictionary"""
        return {
            "id": self.id,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "status": self.status,
            "records_processed": self.records_processed,
            "error_message": self.error_message,
            "duration_seconds": self.duration_seconds,
        }


class Person(Base):
    """Person table for IMDb data"""

    __tablename__ = "people"

    nconst = Column(String(20), primary_key=True)
    primary_name = Column(String(255), nullable=False, index=True)
    birth_year = Column(Integer, nullable=True)
    death_year = Column(Integer, nullable=True)
    primary_profession = Column(Text, nullable=True)
    known_for_titles = Column(Text, nullable=True)

    def to_dict(self):
        """Convert to dictionary"""
        return {
            "nconst": self.nconst,
            "primary_name": self.primary_name,
            "birth_year": self.birth_year,
            "death_year": self.death_year,
            "primary_profession": self.primary_profession,
            "known_for_titles": self.known_for_titles,
        }


class Title(Base):
    """Title table for IMDb data"""

    __tablename__ = "titles"

    tconst = Column(String(20), primary_key=True)
    title_type = Column(String(50), nullable=True)
    primary_title = Column(String(500), nullable=False, index=True)
    original_title = Column(String(500), nullable=True)
    is_adult = Column(Boolean, nullable=True, default=False)
    start_year = Column(Integer, nullable=True)
    end_year = Column(Integer, nullable=True)
    runtime_minutes = Column(Integer, nullable=True)
    genres = Column(Text, nullable=True)

    def to_dict(self):
        """Convert to dictionary"""
        return {
            "tconst": self.tconst,
            "title_type": self.title_type,
            "primary_title": self.primary_title,
            "original_title": self.original_title,
            "is_adult": self.is_adult,
            "start_year": self.start_year,
            "end_year": self.end_year,
            "runtime_minutes": self.runtime_minutes,
            "genres": self.genres,
        }


class Principal(Base):
    """Title principals table for IMDb data"""

    __tablename__ = "principals"

    id = Column(Integer, primary_key=True, autoincrement=True)
    tconst = Column(String(20), nullable=False, index=True)
    ordering = Column(Integer, nullable=False)
    nconst = Column(String(20), nullable=False, index=True)
    category = Column(String(50), nullable=True)
    job = Column(Text, nullable=True)
    characters = Column(Text, nullable=True)

    def to_dict(self):
        """Convert to dictionary"""
        return {
            "tconst": self.tconst,
            "ordering": self.ordering,
            "nconst": self.nconst,
            "category": self.category,
            "job": self.job,
            "characters": self.characters,
        }


class Rating(Base):
    """Title ratings table for IMDb data"""

    __tablename__ = "ratings"

    tconst = Column(String(20), primary_key=True)
    average_rating = Column(Float, nullable=False)
    num_votes = Column(Integer, nullable=False)

    def to_dict(self):
        """Convert to dictionary"""
        return {"tconst": self.tconst, "average_rating": self.average_rating, "num_votes": self.num_votes}