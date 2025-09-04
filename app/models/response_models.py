"""
Pydantic models for API responses
"""

from typing import List

from pydantic import BaseModel, Field, validator


class ActorResponse(BaseModel):
    """Individual actor response"""

    name: str = Field(..., description="Actor/actress name")
    score: float = Field(..., description="Rating score (0-10)")
    number_of_titles: int = Field(..., description="Number of titles")
    total_runtime_minutes: int = Field(..., description="Total runtime in minutes")

    @validator("score")
    def validate_score(cls, v):
        """Ensure score is properly rounded"""
        return round(v, 2)

class PaginationMeta(BaseModel):
    """Pagination metadata"""

    total: int = Field(..., description="Total number of records")
    limit: int = Field(..., description="Number of records per page")
    offset: int = Field(..., description="Number of records skipped")


class ActorsListResponse(BaseModel):
    """Response for actors list endpoint"""

    actors: List[ActorResponse] = Field(..., description="List of actors/actresses")
    profession: str = Field(..., description="Profession filter applied")
    pagination: PaginationMeta = Field(..., description="Pagination information")