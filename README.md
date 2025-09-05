# IMDb Actors Rating API

A high-performance REST API that provides ranked lists of actors and actresses based on their IMDb movie ratings. Built with FastAPI, PostgreSQL, and Docker for scalable data processing and fast query responses.


## Overview

This API processes IMDb datasets to provide fast, paginated access to actors and actresses ranked by their average movie ratings. It features:

- **Fast Queries**: Pre-computed actor ratings for sub-second response times
- **Scalable Architecture**: Docker-based microservices with PostgreSQL
- **Real-time ETL**: Automated data ingestion from IMDb datasets
- **Production Ready**: Comprehensive logging, monitoring, and error handling

### Requirements

The API returns actors/actresses with these key metrics:
- **Name**: Actor/actress name
- **Score**: Average rating (0-10, rounded to 2 decimal places)
- **Number of Titles**: Count of movies/shows they appeared in
- **Total Runtime**: Sum of runtime minutes across all their titles

## Features

### Core API Features
- **Search & Filter**: Search by name, filter by profession (actor/actress)
- **Pagination**: Configurable limits (1-1000 records) with offset support
- **High Performance**: Sub-second response times via pre-computed data
- **Rich Metadata**: Comprehensive actor statistics and rankings

### ETL & Data Management
- **Automated ETL**: Download and process IMDb datasets automatically
- **Real-time Updates**: Refresh computed data on-demand
- **Mock Data**: Generate test data for development
- **ETL Monitoring**: Track processing status and history

### Operations & Monitoring
- **Health Checks**: Comprehensive system health monitoring
- **Metrics**: Performance and usage metrics collection
- **Structured Logging**: JSON-formatted logs with context
- **Docker Ready**: Full containerization with docker-compose

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   FastAPI App   │    │   PostgreSQL    │    │   IMDb Data     │
│                 │    │                 │    │                 │
│  ┌───────────┐  │    │  ┌───────────┐  │    │  ┌───────────┐  │
│  │    API    │◄─┼────┼─►│   Tables  │  │    │  │ .tsv.gz   │  │
│  │ Endpoints │  │    │  │           │  │    │  │   Files   │  │
│  └───────────┘  │    │  └───────────┘  │    │  └───────────┘  │
│                 │    │                 │    │                 │
│  ┌───────────┐  │    │  ┌───────────┐  │    │                 │
│  │    ETL    │◄─┼────┼─►│Computed   │  │    │                 │
│  │ Pipeline  │  │    │  │Ratings    │  │    │                 │
│  └───────────┘  │    │  └───────────┘  │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### Component Overview

- **FastAPI Application**: REST API with automatic OpenAPI documentation
- **PostgreSQL Database**: Relational database with optimized indexes
- **ETL Pipeline**: Data extraction, transformation, and loading system
- **Docker Containers**: Isolated, scalable deployment environment

## Quick Start

### Prerequisites
- Docker & Docker Compose
- 4GB+ RAM (for processing IMDb datasets)
- 10GB+ disk space (for IMDb data storage)

### 1. Clone and Start
```bash
git clone <repository-url>
cd DEUS
docker-compose up -d
```

### 2. Create Mock Data (for testing)
```bash
docker exec imdb_api python scripts/create_mock_data.py
```

### 3. Access the API
- **API Base**: http://localhost:8000
- **Documentation**: http://localhost:8000/docs
- **Health Check**: http://localhost:8000/health

### 4. Test API Endpoints
```bash
# Get top actors
curl "http://localhost:8000/actors?profession=actor&limit=5"

# Search for specific actor
curl "http://localhost:8000/actors?profession=actor&search=Tom&limit=10"

# Get ETL status
curl "http://localhost:8000/status"
```

### Docker Compose Configuration

The `docker-compose.yml` defines:
- **PostgreSQL**: Database with health checks
- **API Service**: FastAPI application with auto-reload
- **Networking**: Internal Docker network for service communication
- **Volumes**: Persistent data storage and code mounting

## API Documentation

### Base URL
```
http://localhost:8000
```

### Authentication
Currently no authentication required (can be added for production).

### Endpoints

#### 1. Get Actors/Actresses
```http
GET /actors
```

**Parameters:**
- `profession` (required): "actor" or "actress"
- `limit` (optional): Number of results (1-1000, default: 100)
- `offset` (optional): Number of results to skip (default: 0)
- `search` (optional): Search term for actor names

**Response:**
```json
{
  "actors": [
    {
      "name": "Leonardo DiCaprio",
      "score": 8.45,
      "number_of_titles": 25,
      "total_runtime_minutes": 3420
    }
  ],
  "profession": "actor",
  "pagination": {
    "total": 1500,
    "limit": 100,
    "offset": 0
  }
}
```

#### 2. Health Check
```http
GET /health
```

**Response:**
```json
{
  "status": "healthy",
  "timestamp": "2025-08-31T08:30:00Z",
  "duration_ms": 45.2,
  "components": {
    "database": {
      "status": "healthy",
      "message": "Database connection is healthy"
    }
  }
}
```

#### 3. ETL Status
```http
GET /status
```

**Response:**
```json
{
  "status": "Finished running successfully at 31-08-2025 08:30",
  "last_run": "31-08-2025 08:30",
  "records_processed": 1250000,
  "duration_seconds": 1800
}
```

#### 4. Start ETL Process
```http
POST /etl/start
```

**Parameters:**
- `file_type` (optional): "people", "titles", "ratings", or "principals"
- `force` (optional): Force start even if ETL is running

#### 5. Refresh Actor Ratings
```http
POST /etl/refresh-view
```

Refreshes the pre-computed actor ratings table.

### Interactive Documentation

Visit http://localhost:8000/docs for interactive Swagger UI documentation with:
- Try-it-out functionality
- Request/response examples
- Parameter descriptions
- Authentication testing

## Data Flow

### 1. Data Ingestion Flow
```
IMDb Datasets → Download → Extract → Transform → Load → PostgreSQL
     ↓              ↓         ↓          ↓        ↓         ↓
name.basics.tsv → Pandas → Clean → Validate → Bulk Insert → people
title.basics.tsv → Pandas → Clean → Validate → Bulk Insert → titles
title.ratings.tsv → Pandas → Clean → Validate → Bulk Insert → ratings
title.principals.tsv → Pandas → Clean → Validate → Bulk Insert → principals
```

### 2. Query Processing Flow
```
API Request → Validation → Database Query → Response Formatting → JSON Response
     ↓            ↓              ↓                ↓                    ↓
/actors?... → Check params → SELECT FROM → Transform to → {"actors": [...]}
              profession     actor_ratings   response model   "pagination": {...}
```

### 3. Rating Computation Flow
```
Base Tables → Join Operations → Aggregation → Storage
     ↓              ↓              ↓           ↓
people + principals + titles + ratings → GROUP BY → AVG/COUNT/SUM → actor_ratings
```

## ETL Pipeline

### Overview
The ETL (Extract, Transform, Load) pipeline processes IMDb datasets to create optimized actor ratings.

### IMDb Data Sources
- **name.basics.tsv.gz**: Person information (actors, directors, etc.)
- **title.basics.tsv.gz**: Movie/show information
- **title.ratings.tsv.gz**: User ratings for titles
- **title.principals.tsv.gz**: Cast and crew information

### Processing Steps

#### 1. Extract Phase
```python
# Download files from IMDb
etl = IMDbETL()
file_path = etl.download_file('name_basics')
```

#### 2. Transform Phase
```python
# Clean and filter data
df = pd.read_csv(file_path, sep='\t', na_values='\\N')
df_actors = df[df['primaryProfession'].str.contains('actor|actress')]
```

#### 3. Load Phase
```python
# Bulk insert to database
session.add_all(people_objects)
session.commit()
```

#### 4. Compute Ratings
```python
# Calculate actor ratings
INSERT INTO actor_ratings (...)
SELECT 
    p.primary_name,
    AVG(r.average_rating) as score,
    COUNT(DISTINCT pr.tconst) as number_of_titles
FROM people p
JOIN principals pr ON p.nconst = pr.nconst
JOIN ratings r ON pr.tconst = r.tconst
GROUP BY p.primary_name
```

### ETL Commands

#### Run Full Pipeline
```bash
# Process all IMDb files
docker exec imdb_api python app/etl.py

# Or via API
curl -X POST "http://localhost:8000/etl/start"
```

#### Process Single File
```bash
# Process only people data
curl -X POST "http://localhost:8000/etl/start?file_type=people"
```

#### Refresh Computed Data
```bash
# Refresh actor ratings
curl -X POST "http://localhost:8000/etl/refresh-view"
```

### ETL Monitoring

#### Check Status
```bash
curl "http://localhost:8000/etl/status"
```

#### View History
```bash
curl "http://localhost:8000/etl/history?limit=10"
```

## Database Schema

### Core Tables

#### people
```sql
CREATE TABLE people (
    nconst VARCHAR(20) PRIMARY KEY,
    primary_name VARCHAR(255) NOT NULL,
    birth_year INTEGER,
    death_year INTEGER,
    primary_profession TEXT,
    known_for_titles TEXT
);
```

#### titles
```sql
CREATE TABLE titles (
    tconst VARCHAR(20) PRIMARY KEY,
    title_type VARCHAR(50),
    primary_title VARCHAR(500) NOT NULL,
    original_title VARCHAR(500),
    is_adult BOOLEAN DEFAULT FALSE,
    start_year INTEGER,
    end_year INTEGER,
    runtime_minutes INTEGER,
    genres TEXT
);
```

#### ratings
```sql
CREATE TABLE ratings (
    tconst VARCHAR(20) PRIMARY KEY,
    average_rating FLOAT NOT NULL,
    num_votes INTEGER NOT NULL
);
```

#### principals
```sql
CREATE TABLE principals (
    id SERIAL PRIMARY KEY,
    tconst VARCHAR(20) NOT NULL,
    ordering INTEGER NOT NULL,
    nconst VARCHAR(20) NOT NULL,
    category VARCHAR(50),
    job TEXT,
    characters TEXT
);
```

### Computed Tables

#### actor_ratings (Pre-computed for Performance)
```sql
CREATE TABLE actor_ratings (
    id SERIAL PRIMARY KEY,
    primary_name VARCHAR(255) NOT NULL,
    profession VARCHAR(50) NOT NULL,
    score FLOAT NOT NULL,
    number_of_titles INTEGER NOT NULL,
    total_runtime_minutes INTEGER NOT NULL
);
```

#### etl_runs (ETL Tracking)
```sql
CREATE TABLE etl_runs (
    id SERIAL PRIMARY KEY,
    started_at TIMESTAMP NOT NULL,
    finished_at TIMESTAMP,
    status VARCHAR(20) NOT NULL,
    records_processed INTEGER,
    error_message TEXT,
    duration_seconds INTEGER
);
```

### Indexes for Performance
```sql
-- Actor ratings indexes
CREATE INDEX idx_actor_ratings_profession ON actor_ratings(profession);
CREATE INDEX idx_actor_ratings_score ON actor_ratings(score DESC);
CREATE INDEX idx_actor_ratings_name ON actor_ratings(primary_name);

-- Search indexes
CREATE INDEX idx_people_name ON people(primary_name);
CREATE INDEX idx_principals_nconst ON principals(nconst);
CREATE INDEX idx_principals_tconst ON principals(tconst);
```

## Development

### Project Structure
```
imdb-actors-api/
├── app/                    # Main application code
│   ├── config/            # Configuration management
│   ├── database/          # Database models and connections
│   ├── models/            # Pydantic response models
│   ├── routers/           # FastAPI route handlers
│   ├── services/          # Business logic services
│   ├── api.py             # FastAPI application setup
│   └── etl.py             # ETL pipeline implementation
├── scripts/               # Utility scripts
├── docker-compose.yml     # Docker services configuration
├── Dockerfile             # Container build instructions
├── requirements.txt       # Python dependencies
└── README.md              # This file
```

### Scaling Considerations

#### Horizontal Scaling
- Multiple API instances behind load balancer
- Shared PostgreSQL database
- Redis for caching (future enhancement)

#### Database Optimization
- Connection pooling (configured via `DB_POOL_SIZE`)
- Read replicas for query distribution
- Partitioning for large datasets

#### Monitoring & Observability
- Health check endpoints for load balancer
- Structured logging for log aggregation
- Metrics collection for monitoring systems

### Security Considerations

#### API Security
- Rate limiting (implement with nginx or API gateway)
- Input validation (handled by Pydantic models)
- SQL injection prevention (SQLAlchemy ORM)

#### Database Security
- Connection encryption (SSL)
- Principle of least privilege for database users
- Regular security updates

#### Container Security
- Non-root user in containers
- Minimal base images
- Regular image updates