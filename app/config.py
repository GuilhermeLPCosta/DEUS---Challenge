import os
from typing import List

class Config:
    # Database
    DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://application:secretpassword@localhost:5432/application")
    
    # IMDb Dataset URLs
    IMDB_BASE_URL = "https://datasets.imdbws.com/"
    IMDB_FILES = {
        'name_basics': 'name.basics.tsv.gz',
        'title_basics': 'title.basics.tsv.gz',
        'title_ratings': 'title.ratings.tsv.gz',
        'title_principals': 'title.principals.tsv.gz'
    }
    
    # ETL Configuration
    DATA_DIR = os.getenv("DATA_DIR", "/app/data")
    CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "10000"))  # Process in chunks
    MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
    
     # API Configuration
    API_HOST = os.getenv("API_HOST", "0.0.0.0")
    API_PORT = int(os.getenv("API_PORT", "8000"))
    API_TITLE = "IMDb Actors Rating API"
    API_VERSION = "1.0.0"
    
    # Professions we care about
    TARGET_PROFESSIONS = ['actor', 'actress']
    
    # Default API limits
    DEFAULT_LIMIT = 100
    MAX_LIMIT = 1000