"""
Simplified ETL pipeline for IMDb data processing using SQLAlchemy ORM
"""

import gzip
import os
from datetime import datetime
from typing import Dict

import pandas as pd
import requests
from sqlalchemy.orm import sessionmaker

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
        self.engine = get_engine()
        self.Session = sessionmaker(bind=self.engine)

        # Create data directory
        os.makedirs(self.data_dir, exist_ok=True)

        logger.info(f"ETL initialized - data_dir: {self.data_dir}")

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
        """Process name.basics.tsv.gz file using SQLAlchemy ORM"""
        logger.info(f"Processing people file - file_path: {file_path}")

        # Read and clean data
        with gzip.open(file_path, "rt", encoding="utf-8") as f:
            df = pd.read_csv(f, sep="\t", na_values="\\N")

        # Clean data
        df = df.dropna(subset=["primaryName"])
        df["birthYear"] = pd.to_numeric(df["birthYear"], errors="coerce")
        df["deathYear"] = pd.to_numeric(df["deathYear"], errors="coerce")

        # Filter for actors/actresses
        df_actors = df[df["primaryProfession"].str.contains("actor|actress", na=False, case=False)]

        logger.info(f"People data loaded - total_records: {len(df)}, actor_records: {len(df_actors)}")

        # Insert data using ORM
        records_processed = 0
        chunk_size = self.settings.chunk_size

        with self.Session() as session:
            # Clear existing data
            session.query(Person).delete()
            session.commit()

            for i in range(0, len(df_actors), chunk_size):
                chunk = df_actors.iloc[i : i + chunk_size]

                # Create Person objects
                people = []
                for _, row in chunk.iterrows():
                    person = Person(
                        nconst=row["nconst"],
                        primary_name=row["primaryName"],
                        birth_year=int(row["birthYear"]) if pd.notna(row["birthYear"]) else None,
                        death_year=int(row["deathYear"]) if pd.notna(row["deathYear"]) else None,
                        primary_profession=row["primaryProfession"],
                        known_for_titles=row["knownForTitles"],
                    )
                    people.append(person)

                # Bulk insert
                session.add_all(people)
                session.commit()
                records_processed += len(people)

                if records_processed % 10000 == 0:
                    logger.info(f"People processing progress - records_processed: {records_processed}")

        logger.info(f"People processing completed - records_processed: {records_processed}")
        return records_processed

    @log_execution_time("etl.process_titles")
    @handle_exceptions("etl", "process_titles")

    def process_titles(self, file_path: str) -> int:
        """Process title.basics.tsv.gz file using SQLAlchemy ORM"""
        logger.info(f"Processing titles file - file_path: {file_path}")

        with gzip.open(file_path, "rt", encoding="utf-8") as f:
            df = pd.read_csv(f, sep="\t", na_values="\\N")

        # Clean data
        df = df.dropna(subset=["primaryTitle"])
        df["startYear"] = pd.to_numeric(df["startYear"], errors="coerce")
        df["endYear"] = pd.to_numeric(df["endYear"], errors="coerce")
        df["runtimeMinutes"] = pd.to_numeric(df["runtimeMinutes"], errors="coerce")

        logger.info(f"Titles data loaded - total_records: {len(df)}")

        records_processed = 0
        chunk_size = self.settings.chunk_size

        with self.Session() as session:
            # Clear existing data
            session.query(Title).delete()
            session.commit()

            for i in range(0, len(df), chunk_size):
                chunk = df.iloc[i : i + chunk_size]

                # Create Title objects
                titles = []
                for _, row in chunk.iterrows():
                    title = Title(
                        tconst=row["tconst"],
                        title_type=row["titleType"],
                        primary_title=row["primaryTitle"],
                        original_title=row["originalTitle"],
                        is_adult=row["isAdult"] == "1",
                        start_year=int(row["startYear"]) if pd.notna(row["startYear"]) else None,
                        end_year=int(row["endYear"]) if pd.notna(row["endYear"]) else None,
                        runtime_minutes=int(row["runtimeMinutes"]) if pd.notna(row["runtimeMinutes"]) else None,
                        genres=row["genres"],
                    )
                    titles.append(title)

                # Bulk insert
                session.add_all(titles)
                session.commit()
                records_processed += len(titles)

                if records_processed % 10000 == 0:
                    logger.info(f"Titles processing progress - records_processed: {records_processed}")

        logger.info(f"Titles processing completed - records_processed: {records_processed}")
        return records_processed

    @log_execution_time("etl.process_ratings")
    @handle_exceptions("etl", "process_ratings")

    def process_ratings(self, file_path: str) -> int:
        """Process title.ratings.tsv.gz file using SQLAlchemy ORM"""
        logger.info(f"Processing ratings file - file_path: {file_path}")

        with gzip.open(file_path, "rt", encoding="utf-8") as f:
            df = pd.read_csv(f, sep="\t", na_values="\\N")

        df["averageRating"] = pd.to_numeric(df["averageRating"], errors="coerce")
        df["numVotes"] = pd.to_numeric(df["numVotes"], errors="coerce")
        df = df.dropna()

        logger.info(f"Ratings data loaded - total_records: {len(df)}")

        records_processed = 0
        chunk_size = self.settings.chunk_size

        with self.Session() as session:
            # Clear existing data
            session.query(Rating).delete()
            session.commit()

            for i in range(0, len(df), chunk_size):
                chunk = df.iloc[i : i + chunk_size]

                # Create Rating objects
                ratings = []
                for _, row in chunk.iterrows():
                    rating = Rating(
                        tconst=row["tconst"], average_rating=float(row["averageRating"]), num_votes=int(row["numVotes"])
                    )
                    ratings.append(rating)

                # Bulk insert
                session.add_all(ratings)
                session.commit()
                records_processed += len(ratings)

                if records_processed % 10000 == 0:
                    logger.info(f"Ratings processing progress - records_processed: {records_processed}")

        logger.info(f"Ratings processing completed - records_processed: {records_processed}")
        return records_processed

    @log_execution_time("etl.process_principals")
    @handle_exceptions("etl", "process_principals")

    def process_principals(self, file_path: str) -> int:
        """Process title.principals.tsv.gz file using SQLAlchemy ORM"""
        logger.info(f"Processing principals file - file_path: {file_path}")

        with gzip.open(file_path, "rt", encoding="utf-8") as f:
            df = pd.read_csv(f, sep="\t", na_values="\\N")

        # Filter for actors/actresses only
        df = df[df["category"].isin(self.settings.target_professions)].copy()
        df["ordering"] = pd.to_numeric(df["ordering"], errors="coerce")
        df = df.dropna(subset=["tconst", "nconst", "category"])

        logger.info(f"Principals data loaded - total_records: {len(df)}")

        records_processed = 0
        chunk_size = self.settings.chunk_size

        with self.Session() as session:
            # Clear existing data
            session.query(Principal).delete()
            session.commit()

            for i in range(0, len(df), chunk_size):
                chunk = df.iloc[i : i + chunk_size]

                # Create Principal objects
                principals = []
                for _, row in chunk.iterrows():
                    principal = Principal(
                        tconst=row["tconst"],
                        ordering=int(row["ordering"]) if pd.notna(row["ordering"]) else 1,
                        nconst=row["nconst"],
                        category=row["category"],
                        job=row["job"] if pd.notna(row["job"]) else None,
                        characters=row["characters"] if pd.notna(row["characters"]) else None,
                    )
                    principals.append(principal)

                # Bulk insert
                session.add_all(principals)
                session.commit()
                records_processed += len(principals)

                if records_processed % 10000 == 0:
                    logger.info(f"Principals processing progress - records_processed: {records_processed}")

        logger.info(f"Principals processing completed - records_processed: {records_processed}")
        return records_processed

    @log_execution_time("etl.refresh_view")
    @handle_exceptions("etl", "refresh_materialized_view")

    def refresh_materialized_view(self):
        """Refresh the actor_ratings table with computed data"""
        logger.info("Refreshing actor ratings table")

        with self.Session() as session:
            # Clear existing actor ratings
            session.query(ActorRating).delete()
            session.commit()

            # Compute new actor ratings from the base tables
            from sqlalchemy import text
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
    