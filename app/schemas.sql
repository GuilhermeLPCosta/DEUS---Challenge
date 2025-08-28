-- IMDb Database Schema

-- People table (from name.basics.tsv)
CREATE TABLE IF NOT EXISTS people (
    nconst VARCHAR(10) PRIMARY KEY,
    primary_name TEXT NOT NULL,
    birth_year INTEGER,
    death_year INTEGER,
    primary_profession TEXT[],
    known_for_titles TEXT[],
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Titles table (from title.basics.tsv)
CREATE TABLE IF NOT EXISTS titles (
    tconst VARCHAR(10) PRIMARY KEY,
    title_type VARCHAR(20),
    primary_title TEXT,
    original_title TEXT,
    is_adult BOOLEAN,
    start_year INTEGER,
    end_year INTEGER,
    runtime_minutes INTEGER,
    genres TEXT[],
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Ratings table (from title.ratings.tsv)
CREATE TABLE IF NOT EXISTS ratings (
    tconst VARCHAR(10) PRIMARY KEY REFERENCES titles(tconst),
    average_rating DECIMAL(3,1),
    num_votes INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Principals table (from title.principals.tsv)
CREATE TABLE IF NOT EXISTS principals (
    id SERIAL PRIMARY KEY,
    tconst VARCHAR(10) REFERENCES titles(tconst),
    ordering INTEGER,
    nconst VARCHAR(10) REFERENCES people(nconst),
    category VARCHAR(50),
    job TEXT,
    characters TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(tconst, ordering)
);

-- ETL runs tracking table
CREATE TABLE IF NOT EXISTS etl_runs (
    id SERIAL PRIMARY KEY,
    run_type VARCHAR(20) NOT NULL, -- 'full' or 'incremental'
    status VARCHAR(20) NOT NULL, -- 'running', 'success', 'failed'
    started_at TIMESTAMP NOT NULL,
    finished_at TIMESTAMP,
    records_processed INTEGER DEFAULT 0,
    error_message TEXT,
    file_checksums JSONB -- Store file hashes to detect changes
);

-- Actor ratings materialized view for performance
CREATE MATERIALIZED VIEW IF NOT EXISTS actor_ratings AS
SELECT 
    p.nconst,
    p.primary_name,
    pr.category as profession,
    COUNT(DISTINCT pr.tconst) as number_of_titles,
    AVG(r.average_rating) as score,
    SUM(COALESCE(t.runtime_minutes, 0)) as total_runtime_minutes
FROM people p
JOIN principals pr ON p.nconst = pr.nconst
JOIN titles t ON pr.tconst = t.tconst
JOIN ratings r ON t.tconst = r.tconst
WHERE pr.category IN ('actor', 'actress')
GROUP BY p.nconst, p.primary_name, pr.category;

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_people_profession ON people USING GIN(primary_profession);
CREATE INDEX IF NOT EXISTS idx_principals_category ON principals(category);
CREATE INDEX IF NOT EXISTS idx_principals_nconst ON principals(nconst);
CREATE INDEX IF NOT EXISTS idx_principals_tconst ON principals(tconst);
CREATE INDEX IF NOT EXISTS idx_titles_type ON titles(title_type);
CREATE INDEX IF NOT EXISTS idx_ratings_rating ON ratings(average_rating);
CREATE INDEX IF NOT EXISTS idx_actor_ratings_profession ON actor_ratings(profession);
CREATE INDEX IF NOT EXISTS idx_actor_ratings_score ON actor_ratings(score DESC);

-- Function to refresh materialized view
CREATE OR REPLACE FUNCTION refresh_actor_ratings()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW actor_ratings;
END;
$$ LANGUAGE plpgsql;