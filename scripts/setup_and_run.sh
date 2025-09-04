#!/bin/bash

echo "🚀 Setting up IMDb API with PostgreSQL"
echo "======================================"

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker and try again."
    exit 1
fi

# Start PostgreSQL container
echo "📦 Starting PostgreSQL container..."
docker-compose up -d postgres

# Wait for PostgreSQL to be ready
echo "⏳ Waiting for PostgreSQL to be ready..."
timeout=60
counter=0
while ! docker-compose exec -T postgres pg_isready -U imdb_user -d imdb_db > /dev/null 2>&1; do
    if [ $counter -ge $timeout ]; then
        echo "❌ PostgreSQL failed to start within $timeout seconds"
        exit 1
    fi
    echo "   Waiting... ($counter/$timeout)"
    sleep 2
    counter=$((counter + 2))
done

echo "✅ PostgreSQL is ready!"

# Create mock data
echo "📊 Creating mock data..."
python scripts/create_mock_data.py

if [ $? -eq 0 ]; then
    echo "✅ Mock data created successfully!"
    
    # Start the API server
    echo "🌐 Starting API server..."
    echo "   API will be available at: http://localhost:8000"
    echo "   API documentation at: http://localhost:8000/docs"
    echo "   Press Ctrl+C to stop"
    echo ""
    
    python run_server.py
else
    echo "❌ Failed to create mock data"
    exit 1
fi