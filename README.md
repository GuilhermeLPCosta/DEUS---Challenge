# IMDb Actors Rating API

A high-performance REST API that provides ranked lists of actors and actresses based on their IMDb movie ratings. Built with FastAPI, PostgreSQL, and Docker for scalable data processing and fast query responses.


## 🎯 Overview

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

## ✨ Features

### Core API Features
- 🔍 **Search & Filter**: Search by name, filter by profession (actor/actress)
- 📄 **Pagination**: Configurable limits (1-1000 records) with offset support
- ⚡ **High Performance**: Sub-second response times via pre-computed data
- 📊 **Rich Metadata**: Comprehensive actor statistics and rankings

### ETL & Data Management
- 🔄 **Automated ETL**: Download and process IMDb datasets automatically
- 📈 **Real-time Updates**: Refresh computed data on-demand
- 🎭 **Mock Data**: Generate test data for development
- 📋 **ETL Monitoring**: Track processing status and history

### Operations & Monitoring
- 🏥 **Health Checks**: Comprehensive system health monitoring
- 📊 **Metrics**: Performance and usage metrics collection
- 📝 **Structured Logging**: JSON-formatted logs with context
- 🐳 **Docker Ready**: Full containerization with docker-compose

## 🏗️ Architecture

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

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose
- 4GB+ RAM (for processing IMDb datasets)
- 10GB+ disk space (for IMDb data storage)

### 1. Clone and Start
```bash
git clone <repository-url>
cd imdb-actors-api
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

## 📦 Installation

### Option 1: Docker (Recommended)

1. **Clone Repository**
   ```bash
   git clone <repository-url>
   cd imdb-actors-api
   ```

2. **Environment Setup**
   ```bash
   cp env-example .env
   # Edit .env file with your configuration
   ```

3. **Start Services**
   ```bash
   docker-compose up -d
   ```

4. **Verify Installation**
   ```bash
   docker-compose ps
   curl http://localhost:8000/health
   ```
