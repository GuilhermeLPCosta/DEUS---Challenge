"""
Simplified ETL pipeline for IMDb data processing using SQLAlchemy ORM
"""

import gzip
import os
import time
from datetime import datetime
from typing import Dict

import requests
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
from app.config.settings import get_settings
from app.database.connection import get_engine
from app.database.models import ActorRating, ETLRun, Person, Principal, Rating, Title
from app.services.logger_service import get_logger, log_execution_time, handle_exceptions

# Initialize configuration
settings = get_settings()
logger = get_logger("etl")


class IMDbETL:
    """IMDb ETL pipeline using SQLAlchemy ORM"""

    def __init__(self):
        self.settings = settings
        self.data_dir = settings.data_dir
        
        # Create data directory
        os.makedirs(self.data_dir, exist_ok=True)
        
        # Initialize database connection with retry logic
        self._init_database_connection()

        logger.info(f"ETL initialized - data_dir: {self.data_dir}")

    def _init_database_connection(self, max_retries: int = 30, retry_interval: int = 2):
        """Initialize database connection with retry logic"""
        logger.info("Initializing database connection for ETL")
        
        for attempt in range(max_retries):
            try:
                self.engine = get_engine()
                self.Session = sessionmaker(bind=self.engine)
                
                # Test connection
                with self.Session() as session:
                    from sqlalchemy import text
                    session.execute(text("SELECT 1"))
                
                logger.info("Database connection established successfully")
                return
                
            except Exception as e:
                logger.warning(f"Database connection attempt {attempt + 1} failed - error: {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(retry_interval)
                else:
                    logger.error("Database connection failed after all retries")
                    raise Exception("Could not establish database connection for ETL")

    @log_execution_time("etl.download")
    @handle_exceptions("etl", "file_download")

    def download_file(self, file_key: str) -> str:
        """Download IMDb file if not exists or changed"""
        filename = self.settings.imdb_files[file_key]
        url = f"{self.settings.imdb_base_url}{filename}"
        local_path = os.path.join(self.data_dir, filename)

        logger.info(f"Starting file download - filename: {filename}, url: {url}")

        try:
            # Check if file exists and get remote file info
            response = requests.head(url, timeout=self.settings.etl_timeout)
            response.raise_for_status()

            remote_size = int(response.headers.get("content-length", 0))

            # Check if we need to download
            if os.path.exists(local_path):
                local_size = os.path.getsize(local_path)
                if local_size == remote_size:
                    logger.info(f"File already exists with correct size - filename: {filename}")
                    return local_path

            # Download file
            logger.info(f"Downloading file - filename: {filename}, size_mb: {remote_size / 1024 / 1024:.2f}")

            response = requests.get(url, stream=True, timeout=self.settings.etl_timeout)
            response.raise_for_status()

            chunk_size = 8192  # 8KB chunks
            with open(local_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)

            file_size = os.path.getsize(local_path)
            logger.info(f"File downloaded successfully - filename: {filename}, size_mb: {file_size / 1024 / 1024:.2f}")

            return local_path

        except Exception as e:
            logger.error(f"File download failed - filename: {filename}, error: {str(e)}")
            raise

    @log_execution_time("etl.process_people")
    @handle_exceptions("etl", "process_people")
    def process_people(self, file_path: str) -> int:
        """Process name.basics.tsv.gz file using streaming approach"""
        logger.info(f"Processing people file (streaming) - file_path: {file_path}")
        
        records_processed = 0
        batch_size = 100  # Small batch size for memory efficiency
        batch = []

        with self.Session() as session:
            # Clear existing data
            session.query(Person).delete()
            session.commit()

            with gzip.open(file_path, "rt", encoding="utf-8") as f:
                # Skip header
                header = f.readline()
                
                for line_num, line in enumerate(f, 1):
                    try:
                        fields = line.strip().split('\t')
                        if len(fields) < 6:
                            continue
                            
                        # Only process actors/actresses
                        primary_profession = fields[4] if fields[4] != '\\N' else ''
                        if not ('actor' in primary_profession.lower() or 'actress' in primary_profession.lower()):
                            continue
                        
                        person = Person(
                            nconst=fields[0],
                            primary_name=fields[1] if fields[1] != '\\N' else None,
                            birth_year=int(fields[2]) if fields[2] != '\\N' and fields[2].isdigit() else None,
                            death_year=int(fields[3]) if fields[3] != '\\N' and fields[3].isdigit() else None,
                            primary_profession=primary_profession if primary_profession else None,
                            known_for_titles=fields[5] if fields[5] != '\\N' else None,
                        )
                        
                        batch.append(person)
                        
                        if len(batch) >= batch_size:
                            session.add_all(batch)
                            session.commit()
                            records_processed += len(batch)
                            batch = []
                            
                            if records_processed % 1000 == 0:
                                logger.info(f"People processing progress - records_processed: {records_processed}")
                    
                    except Exception as e:
                        logger.warning(f"Error processing line {line_num}: {e}")
                        continue

                # Process remaining batch
                if batch:
                    session.add_all(batch)
                    session.commit()
                    records_processed += len(batch)

        logger.info(f"People processing completed - records_processed: {records_processed}")
        return records_processed

    @log_execution_time("etl.process_titles")
    @handle_exceptions("etl", "process_titles")
    def process_titles(self, file_path: str) -> int:
        """Process title.basics.tsv.gz file using streaming approach"""
        logger.info(f"Processing titles file (streaming) - file_path: {file_path}")
        
        records_processed = 0
        batch_size = 100
        batch = []

        with self.Session() as session:
            # Clear existing data
            session.query(Title).delete()
            session.commit()

            with gzip.open(file_path, "rt", encoding="utf-8") as f:
                # Skip header
                header = f.readline()
                
                for line_num, line in enumerate(f, 1):
                    try:
                        fields = line.strip().split('\t')
                        if len(fields) < 9:
                            continue
                        
                        title = Title(
                            tconst=fields[0],
                            title_type=fields[1] if fields[1] != '\\N' else None,
                            primary_title=fields[2] if fields[2] != '\\N' else None,
                            original_title=fields[3] if fields[3] != '\\N' else None,
                            is_adult=fields[4] == "1",
                            start_year=int(fields[5]) if fields[5] != '\\N' and fields[5].isdigit() else None,
                            end_year=int(fields[6]) if fields[6] != '\\N' and fields[6].isdigit() else None,
                            runtime_minutes=int(fields[7]) if fields[7] != '\\N' and fields[7].isdigit() else None,
                            genres=fields[8] if fields[8] != '\\N' else None,
                        )
                        
                        batch.append(title)
                        
                        if len(batch) >= batch_size:
                            session.add_all(batch)
                            session.commit()
                            records_processed += len(batch)
                            batch = []
                            
                            if records_processed % 1000 == 0:
                                logger.info(f"Titles processing progress - records_processed: {records_processed}")
                    
                    except Exception as e:
                        logger.warning(f"Error processing line {line_num}: {e}")
                        continue

                # Process remaining batch
                if batch:
                    session.add_all(batch)
                    session.commit()
                    records_processed += len(batch)

        logger.info(f"Titles processing completed - records_processed: {records_processed}")
        return records_processed

    @log_execution_time("etl.process_ratings")
    @handle_exceptions("etl", "process_ratings")
    def process_ratings(self, file_path: str) -> int:
        """Process title.ratings.tsv.gz file using streaming approach"""
        logger.info(f"Processing ratings file (streaming) - file_path: {file_path}")
        
        records_processed = 0
        batch_size = 100
        batch = []

        with self.Session() as session:
            # Clear existing data
            session.query(Rating).delete()
            session.commit()

            with gzip.open(file_path, "rt", encoding="utf-8") as f:
                # Skip header
                header = f.readline()
                
                for line_num, line in enumerate(f, 1):
                    try:
                        fields = line.strip().split('\t')
                        if len(fields) < 3:
                            continue
                        
                        rating = Rating(
                            tconst=fields[0],
                            average_rating=float(fields[1]) if fields[1] != '\\N' else None,
                            num_votes=int(fields[2]) if fields[2] != '\\N' and fields[2].isdigit() else None,
                        )
                        
                        if rating.average_rating is not None and rating.num_votes is not None:
                            batch.append(rating)
                        
                        if len(batch) >= batch_size:
                            session.add_all(batch)
                            session.commit()
                            records_processed += len(batch)
                            batch = []
                            
                            if records_processed % 1000 == 0:
                                logger.info(f"Ratings processing progress - records_processed: {records_processed}")
                    
                    except Exception as e:
                        logger.warning(f"Error processing line {line_num}: {e}")
                        continue

                # Process remaining batch
                if batch:
                    session.add_all(batch)
                    session.commit()
                    records_processed += len(batch)

        logger.info(f"Ratings processing completed - records_processed: {records_processed}")
        return records_processed

    @log_execution_time("etl.process_principals")
    @handle_exceptions("etl", "process_principals")
    def process_principals(self, file_path: str) -> int:
        """Process title.principals.tsv.gz file using streaming approach"""
        logger.info(f"Processing principals file (streaming) - file_path: {file_path}")
        
        records_processed = 0
        batch_size = 100
        batch = []

        with self.Session() as session:
            # Clear existing data
            session.query(Principal).delete()
            session.commit()

            with gzip.open(file_path, "rt", encoding="utf-8") as f:
                # Skip header
                header = f.readline()
                
                for line_num, line in enumerate(f, 1):
                    try:
                        fields = line.strip().split('\t')
                        if len(fields) < 6:
                            continue
                        
                        # Only process actors/actresses
                        category = fields[3] if fields[3] != '\\N' else ''
                        if category not in ['actor', 'actress']:
                            continue
                        
                        principal = Principal(
                            tconst=fields[0],
                            ordering=int(fields[1]) if fields[1] != '\\N' and fields[1].isdigit() else 1,
                            nconst=fields[2],
                            category=category,
                            job=fields[4] if fields[4] != '\\N' else None,
                            characters=fields[5] if fields[5] != '\\N' else None,
                        )
                        
                        batch.append(principal)
                        
                        if len(batch) >= batch_size:
                            session.add_all(batch)
                            session.commit()
                            records_processed += len(batch)
                            batch = []
                            
                            if records_processed % 1000 == 0:
                                logger.info(f"Principals processing progress - records_processed: {records_processed}")
                    
                    except Exception as e:
                        logger.warning(f"Error processing line {line_num}: {e}")
                        continue

                # Process remaining batch
                if batch:
                    session.add_all(batch)
                    session.commit()
                    records_processed += len(batch)

        logger.info(f"Principals processing completed - records_processed: {records_processed}")
        return records_processed

    @log_execution_time("etl.refresh_view")
    @handle_exceptions("etl", "refresh_materialized_view")
    def refresh_materialized_view(self):
        """Refresh the actor_ratings table with computed data"""
        logger.info("Refreshing actor ratings table")

        with self.Session() as session:
            # Clear existing actor ratings
            session.execute(text("DELETE FROM actor_ratings"))
            session.commit()

            # Compute new actor ratings from the base tables
            query = text("""
            INSERT INTO actor_ratings (primary_name, profession, score, number_of_titles, total_runtime_minutes)
            SELECT 
                p.primary_name,
                pr.category as profession,
                ROUND(AVG(r.average_rating)::numeric, 2) as score,
                COUNT(DISTINCT pr.tconst) as number_of_titles,
                SUM(COALESCE(t.runtime_minutes, 0)) as total_runtime_minutes
            FROM people p
            JOIN principals pr ON p.nconst = pr.nconst
            JOIN titles t ON pr.tconst = t.tconst
            JOIN ratings r ON t.tconst = r.tconst
            WHERE pr.category IN ('actor', 'actress')
            GROUP BY p.primary_name, pr.category
            ORDER BY score DESC
            """)
            
            session.execute(query)
            session.commit()

        logger.info("Actor ratings table refreshed successfully")

    def log_etl_run(
        self,
        status: str,
        started_at: datetime,
        finished_at: datetime = None,
        records_processed: int = 0,
        error_message: str = None,
    ) -> int:
        """Log ETL run to database using SQLAlchemy ORM"""

        with self.Session() as session:
            etl_run = ETLRun(
                status=status,
                started_at=started_at,
                finished_at=finished_at,
                records_processed=records_processed,
                error_message=error_message,
            )

            # Calculate duration if both timestamps are available
            if started_at and finished_at:
                duration = finished_at - started_at
                etl_run.duration_seconds = int(duration.total_seconds())

            session.add(etl_run)
            session.commit()

            return etl_run.id

    @log_execution_time("etl.full_pipeline")

    def run_full_pipeline(self) -> Dict[str, any]:
        """Run the complete ETL pipeline"""
        started_at = datetime.now()
        total_records = 0

        logger.info("Starting full ETL pipeline")

        try:
            # Process files in order
            file_processors = {
                "name_basics": self.process_people,
                "title_basics": self.process_titles,
                "title_ratings": self.process_ratings,
                "title_principals": self.process_principals,
            }

            for file_key, processor_func in file_processors.items():
                logger.info(f"Processing file - file_key: {file_key}")

                # Download file
                file_path = self.download_file(file_key)

                # Process file
                records = processor_func(file_path)
                total_records += records

                logger.info(f"File processing completed - file_key: {file_key}, records_processed: {records}")

            # Refresh materialized view
            self.refresh_materialized_view()

            finished_at = datetime.now()
            duration_seconds = (finished_at - started_at).total_seconds()

            # Log successful run
            run_id = self.log_etl_run("completed", started_at, finished_at, total_records)

            logger.info(f"ETL pipeline completed successfully - run_id: {run_id}, duration_seconds: {duration_seconds}, total_records: {total_records}")

            return {
                "success": True,
                "run_id": run_id,
                "duration_seconds": duration_seconds,
                "records_processed": total_records,
                "started_at": started_at,
                "finished_at": finished_at,
            }

        except Exception as e:
            finished_at = datetime.now()
            duration_seconds = (finished_at - started_at).total_seconds()

            # Log failed run
            run_id = self.log_etl_run("failed", started_at, finished_at, total_records, str(e))

            logger.error(f"ETL pipeline failed - run_id: {run_id}, error: {str(e)}, duration_seconds: {duration_seconds}, records_processed: {total_records}")

            return {
                "success": False,
                "run_id": run_id,
                "error": str(e),
                "duration_seconds": duration_seconds,
                "records_processed": total_records,
                "started_at": started_at,
                "finished_at": finished_at,
            }


def main():
    """Main ETL entry point"""
    etl = IMDbETL()
    result = etl.run_full_pipeline()

    if result["success"]:
        print(f"ETL completed successfully in {result['duration_seconds']:.2f} seconds")
        print(f"Processed {result['records_processed']} records")
    else:
        print(f"ETL failed: {result['error']}")
        exit(1)


if __name__ == "__main__":
    main()
    