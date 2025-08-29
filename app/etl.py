import os
import gzip
import hashlib
import logging
import pandas as pd
import requests
from datetime import datetime
from typing import Dict, Optional
from sqlalchemy import text
from app.config import Config
from app.database import engine, init_database, refresh_materialized_view

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class IMDbETL:
    def __init__(self):
        self.config = Config()
        self.data_dir = self.config.DATA_DIR
        os.makedirs(self.data_dir, exist_ok=True)
        
    def download_file(self, filename: str) -> str:
        """Download IMDb file if not exists or changed"""
        url = f"{self.config.IMDB_BASE_URL}{filename}"
        local_path = os.path.join(self.data_dir, filename)
        
        logger.info(f"Checking file: {filename}")
        
        # Check if we need to download
        try:
            response = requests.head(url, timeout=30)
            response.raise_for_status()
            
            remote_size = int(response.headers.get('content-length', 0))
            
            if os.path.exists(local_path):
                local_size = os.path.getsize(local_path)
                if local_size == remote_size:
                    logger.info(f"File {filename} is up to date")
                    return local_path
                    
        except Exception as e:
            logger.warning(f"Could not check remote file info: {e}")
        
        # Download file
        logger.info(f"Downloading {filename}...")
        response = requests.get(url, stream=True, timeout=300)
        response.raise_for_status()
        
        with open(local_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
                
        logger.info(f"Downloaded {filename} successfully")
        return local_path
    
    def get_file_hash(self, filepath: str) -> str:
        """Calculate file hash for change detection"""
        hash_sha256 = hashlib.sha256()
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_sha256.update(chunk)
        return hash_sha256.hexdigest()
    
    def read_tsv_file(self, filepath: str) -> pd.DataFrame:
        """Read compressed TSV file"""
        logger.info(f"Reading {filepath}")
        
        if filepath.endswith('.gz'):
            with gzip.open(filepath, 'rt', encoding='utf-8') as f:
                df = pd.read_csv(f, sep='\t', na_values='\\N', low_memory=False)
        else:
            df = pd.read_csv(filepath, sep='\t', na_values='\\N', low_memory=False)
            
        logger.info(f"Read {len(df)} rows from {filepath}")
        return df
    
    def process_people(self, df: pd.DataFrame) -> int:
        """Process people data"""
        logger.info("Processing people data...")
        
        # Clean and prepare data
        df = df.copy()
        df['primary_profession'] = df['primaryProfession'].fillna('').apply(
            lambda x: x.split(',') if x else []
        )
        df['known_for_titles'] = df['knownForTitles'].fillna('').apply(
            lambda x: x.split(',') if x else []
        )
        
        # Convert years to integers, handle null values
        df['birthYear'] = pd.to_numeric(df['birthYear'], errors='coerce')
        df['deathYear'] = pd.to_numeric(df['deathYear'], errors='coerce')
        
        # Prepare for insertion
        columns_map = {
            'nconst': 'nconst',
            'primaryName': 'primary_name',
            'birthYear': 'birth_year',
            'deathYear': 'death_year',
            'primary_profession': 'primary_profession',
            'known_for_titles': 'known_for_titles'
        }
        
        df_clean = df[list(columns_map.keys())].rename(columns=columns_map)
        df_clean['updated_at'] = datetime.now()
        
        # Batch insert
        records_processed = 0
        chunk_size = self.config.CHUNK_SIZE
        
        with engine.connect() as conn:
            # Clear existing data
            conn.execute(text("TRUNCATE TABLE people RESTART IDENTITY CASCADE"))
            
            for i in range(0, len(df_clean), chunk_size):
                chunk = df_clean.iloc[i:i + chunk_size]
                
                # Convert arrays to PostgreSQL array format
                chunk_dict = chunk.to_dict('records')
                for record in chunk_dict:
                    if record['primary_profession']:
                        record['primary_profession'] = '{' + ','.join(record['primary_profession']) + '}'
                    else:
                        record['primary_profession'] = '{}'
                        
                    if record['known_for_titles']:
                        record['known_for_titles'] = '{' + ','.join(record['known_for_titles']) + '}'
                    else:
                        record['known_for_titles'] = '{}'
                
                # Insert chunk
                insert_sql = """
                INSERT INTO people (nconst, primary_name, birth_year, death_year, 
                                  primary_profession, known_for_titles, updated_at)
                VALUES (%(nconst)s, %(primary_name)s, %(birth_year)s, %(death_year)s, 
                       %(primary_profession)s, %(known_for_titles)s, %(updated_at)s)
                ON CONFLICT (nconst) DO UPDATE SET
                    primary_name = EXCLUDED.primary_name,
                    birth_year = EXCLUDED.birth_year,
                    death_year = EXCLUDED.death_year,
                    primary_profession = EXCLUDED.primary_profession,
                    known_for_titles = EXCLUDED.known_for_titles,
                    updated_at = EXCLUDED.updated_at
                """
                
                conn.execute(text(insert_sql), chunk_dict)
                records_processed += len(chunk_dict)
                
                if records_processed % 50000 == 0:
                    logger.info(f"Processed {records_processed} people records")
            
            conn.commit()
        
        logger.info(f"Processed {records_processed} people records total")
        return records_processed
    
    def process_titles(self, df: pd.DataFrame) -> int:
        """Process titles data"""
        logger.info("Processing titles data...")
        
        df = df.copy()
        df['genres'] = df['genres'].fillna('').apply(
            lambda x: x.split(',') if x else []
        )
        
        # Convert numeric columns
        df['startYear'] = pd.to_numeric(df['startYear'], errors='coerce')
        df['endYear'] = pd.to_numeric(df['endYear'], errors='coerce')  
        df['runtimeMinutes'] = pd.to_numeric(df['runtimeMinutes'], errors='coerce')
        df['isAdult'] = df['isAdult'].astype(bool)
        
        columns_map = {
            'tconst': 'tconst',
            'titleType': 'title_type',
            'primaryTitle': 'primary_title',
            'originalTitle': 'original_title',
            'isAdult': 'is_adult',
            'startYear': 'start_year',
            'endYear': 'end_year',
            'runtimeMinutes': 'runtime_minutes',
            'genres': 'genres'
        }
        
        df_clean = df[list(columns_map.keys())].rename(columns=columns_map)
        df_clean['updated_at'] = datetime.now()
        
        records_processed = 0
        chunk_size = self.config.CHUNK_SIZE
        
        with engine.connect() as conn:
            conn.execute(text("TRUNCATE TABLE titles RESTART IDENTITY CASCADE"))
            
            for i in range(0, len(df_clean), chunk_size):
                chunk = df_clean.iloc[i:i + chunk_size]
                chunk_dict = chunk.to_dict('records')
                
                for record in chunk_dict:
                    if record['genres']:
                        record['genres'] = '{' + ','.join(record['genres']) + '}'
                    else:
                        record['genres'] = '{}'
                
                insert_sql = """
                INSERT INTO titles (tconst, title_type, primary_title, original_title,
                                  is_adult, start_year, end_year, runtime_minutes, 
                                  genres, updated_at)
                VALUES (%(tconst)s, %(title_type)s, %(primary_title)s, %(original_title)s,
                       %(is_adult)s, %(start_year)s, %(end_year)s, %(runtime_minutes)s,
                       %(genres)s, %(updated_at)s)
                ON CONFLICT (tconst) DO UPDATE SET
                    title_type = EXCLUDED.title_type,
                    primary_title = EXCLUDED.primary_title,
                    original_title = EXCLUDED.original_title,
                    is_adult = EXCLUDED.is_adult,
                    start_year = EXCLUDED.start_year,
                    end_year = EXCLUDED.end_year,
                    runtime_minutes = EXCLUDED.runtime_minutes,
                    genres = EXCLUDED.genres,
                    updated_at = EXCLUDED.updated_at
                """
                
                conn.execute(text(insert_sql), chunk_dict)
                records_processed += len(chunk_dict)
                
                if records_processed % 50000 == 0:
                    logger.info(f"Processed {records_processed} title records")
            
            conn.commit()
        
        logger.info(f"Processed {records_processed} title records total")
        return records_processed
    
    def process_ratings(self, df: pd.DataFrame) -> int:
        """Process ratings data"""
        logger.info("Processing ratings data...")
        
        df = df.copy()
        df['averageRating'] = pd.to_numeric(df['averageRating'], errors='coerce')
        df['numVotes'] = pd.to_numeric(df['numVotes'], errors='coerce')
        
        columns_map = {
            'tconst': 'tconst',
            'averageRating': 'average_rating',
            'numVotes': 'num_votes'
        }
        
        df_clean = df[list(columns_map.keys())].rename(columns=columns_map)
        df_clean['updated_at'] = datetime.now()
        
        records_processed = 0
        chunk_size = self.config.CHUNK_SIZE
        
        with engine.connect() as conn:
            conn.execute(text("TRUNCATE TABLE ratings RESTART IDENTITY CASCADE"))
            
            for i in range(0, len(df_clean), chunk_size):
                chunk = df_clean.iloc[i:i + chunk_size]
                chunk_dict = chunk.to_dict('records')
                
                insert_sql = """
                INSERT INTO ratings (tconst, average_rating, num_votes, updated_at)
                VALUES (%(tconst)s, %(average_rating)s, %(num_votes)s, %(updated_at)s)
                ON CONFLICT (tconst) DO UPDATE SET
                    average_rating = EXCLUDED.average_rating,
                    num_votes = EXCLUDED.num_votes,
                    updated_at = EXCLUDED.updated_at
                """
                
                conn.execute(text(insert_sql), chunk_dict)
                records_processed += len(chunk_dict)
                
                if records_processed % 50000 == 0:
                    logger.info(f"Processed {records_processed} rating records")
            
            conn.commit()
        
        logger.info(f"Processed {records_processed} rating records total")
        return records_processed
    
    def process_principals(self, df: pd.DataFrame) -> int:
        """Process principals data"""
        logger.info("Processing principals data...")
        
        # Filter for actors/actresses only
        df = df[df['category'].isin(self.config.TARGET_PROFESSIONS)].copy()
        
        df['ordering'] = pd.to_numeric(df['ordering'], errors='coerce')
        
        columns_map = {
            'tconst': 'tconst',
            'ordering': 'ordering',
            'nconst': 'nconst',
            'category': 'category',
            'job': 'job',
            'characters': 'characters'
        }
        
        df_clean = df[list(columns_map.keys())].rename(columns=columns_map)
        df_clean['updated_at'] = datetime.now()
        
        records_processed = 0
        chunk_size = self.config.CHUNK_SIZE
        
        with engine.connect() as conn:
            conn.execute(text("TRUNCATE TABLE principals RESTART IDENTITY CASCADE"))
            
            for i in range(0, len(df_clean), chunk_size):
                chunk = df_clean.iloc[i:i + chunk_size]
                chunk_dict = chunk.to_dict('records')
                
                insert_sql = """
                INSERT INTO principals (tconst, ordering, nconst, category, job, 
                                      characters, updated_at)
                VALUES (%(tconst)s, %(ordering)s, %(nconst)s, %(category)s, %(job)s,
                       %(characters)s, %(updated_at)s)
                ON CONFLICT (tconst, ordering) DO UPDATE SET
                    nconst = EXCLUDED.nconst,
                    category = EXCLUDED.category,
                    job = EXCLUDED.job,
                    characters = EXCLUDED.characters,
                    updated_at = EXCLUDED.updated_at
                """
                
                conn.execute(text(insert_sql), chunk_dict)
                records_processed += len(chunk_dict)
                
                if records_processed % 50000 == 0:
                    logger.info(f"Processed {records_processed} principal records")
            
            conn.commit()
        
        logger.info(f"Processed {records_processed} principal records total")
        return records_processed
    
    def log_etl_run(self, status: str, started_at: datetime, 
                    finished_at: Optional[datetime] = None,
                    records_processed: int = 0, error_message: str = None,
                    file_checksums: Dict[str, str] = None) -> int:
        """Log ETL run to database"""
        with engine.connect() as conn:
            insert_sql = """
            INSERT INTO etl_runs (run_type, status, started_at, finished_at, 
                                records_processed, error_message, file_checksums)
            VALUES (%(run_type)s, %(status)s, %(started_at)s, %(finished_at)s,
                   %(records_processed)s, %(error_message)s, %(file_checksums)s)
            RETURNING id
            """
            
            result = conn.execute(text(insert_sql), {
                'run_type': 'full',
                'status': status,
                'started_at': started_at,
                'finished_at': finished_at,
                'records_processed': records_processed,
                'error_message': error_message,
                'file_checksums': file_checksums
            })
            conn.commit()
            return result.fetchone()[0]
    
    def run_etl(self):
        """Run complete ETL pipeline"""
        started_at = datetime.now()
        total_records = 0
        file_checksums = {}
        
        logger.info("Starting IMDb ETL pipeline...")
        
        try:
            # Initialize database
            init_database()
            
            # Download and process files
            files_to_process = [
                ('name_basics', self.process_people),
                ('title_basics', self.process_titles),
                ('title_ratings', self.process_ratings),
                ('title_principals', self.process_principals)
            ]
            
            for file_key, processor_func in files_to_process:
                filename = self.config.IMDB_FILES[file_key]
                
                # Download file
                filepath = self.download_file(filename)
                file_checksums[filename] = self.get_file_hash(filepath)
                
                # Read and process data
                df = self.read_tsv_file(filepath)
                records_processed = processor_func(df)
                total_records += records_processed
                
                logger.info(f"Completed processing {filename}")
            
            # Refresh materialized view
            refresh_materialized_view()
            
            finished_at = datetime.now()
            duration = finished_at - started_at
            
            # Log successful run
            run_id = self.log_etl_run(
                status='success',
                started_at=started_at,
                finished_at=finished_at,
                records_processed=total_records,
                file_checksums=file_checksums
            )
            
            logger.info(f"ETL completed successfully in {duration}")
            logger.info(f"Total records processed: {total_records}")
            logger.info(f"ETL run ID: {run_id}")
            
        except Exception as e:
            finished_at = datetime.now()
            error_msg = str(e)
            
            logger.error(f"ETL failed: {error_msg}")
            
            # Log failed run
            self.log_etl_run(
                status='failed',
                started_at=started_at,
                finished_at=finished_at,
                records_processed=total_records,
                error_message=error_msg,
                file_checksums=file_checksums
            )
            
            raise

def main():
    """Main entry point for ETL"""
    etl = IMDbETL()
    etl.run_etl()

if __name__ == "__main__":
    main()