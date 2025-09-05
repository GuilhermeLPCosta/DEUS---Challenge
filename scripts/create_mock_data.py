#!/usr/bin/env python3
"""
Create mock data for testing the IMDb API
"""

import os
import sys
from datetime import datetime, timedelta
from random import choice, randint, uniform

# Add the project root to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from sqlalchemy.orm import sessionmaker

from app.config.settings import get_settings
from app.database.connection import get_engine
from app.database.models import ActorRating, ETLRun, Person, Principal, Rating, Title

# Sample data
ACTORS = [
    "Leonardo DiCaprio",
    "Robert De Niro",
    "Tom Hanks",
    "Brad Pitt",
    "Denzel Washington",
    "Morgan Freeman",
    "Christian Bale",
    "Matthew McConaughey",
    "Ryan Gosling",
    "Jake Gyllenhaal",
    "Oscar Isaac",
    "Michael Fassbender",
    "Joaquin Phoenix",
    "Adam Driver",
    "Mahershala Ali",
]

ACTRESSES = [
    "Meryl Streep",
    "Scarlett Johansson",
    "Natalie Portman",
    "Cate Blanchett",
    "Jennifer Lawrence",
    "Emma Stone",
    "Charlize Theron",
    "Amy Adams",
    "Viola Davis",
    "Saoirse Ronan",
    "Lupita Nyong'o",
    "Brie Larson",
    "Margot Robbie",
    "Tilda Swinton",
    "Frances McDormand",
]

ACTOR_NAMES = ACTORS + ACTRESSES

MOVIE_TITLES = [
    "The Shawshank Redemption",
    "The Godfather",
    "The Dark Knight",
    "Pulp Fiction",
    "Forrest Gump",
    "Inception",
    "The Matrix",
    "Goodfellas",
    "The Silence of the Lambs",
    "Saving Private Ryan",
    "Schindler's List",
    "The Departed",
    "Fight Club",
    "The Lord of the Rings",
    "Star Wars",
    "Casablanca",
    "Citizen Kane",
    "Vertigo",
    "Psycho",
    "Sunset Boulevard",
    "Apocalypse Now",
    "Taxi Driver",
    "Raging Bull",
    "The Wizard of Oz",
    "Gone with the Wind",
    "Lawrence of Arabia",
    "Singin' in the Rain",
    "It's a Wonderful Life",
    "Some Like It Hot",
    "North by Northwest",
]

GENRES = [
    "Action",
    "Adventure",
    "Comedy",
    "Crime",
    "Drama",
    "Fantasy",
    "Horror",
    "Mystery",
    "Romance",
    "Sci-Fi",
    "Thriller",
    "Western",
]


def create_mock_data():
    """Create comprehensive mock data for testing"""
    settings = get_settings()
    engine = get_engine()
    Session = sessionmaker(bind=engine)

    print("Creating mock data...")

    with Session() as session:
        # Clear existing data
        print("Clearing existing data...")
        session.query(Principal).delete()
        session.query(Rating).delete()
        session.query(Title).delete()
        session.query(Person).delete()
        session.query(ActorRating).delete()
        session.query(ETLRun).delete()
        session.commit()

        # Create people (actors/actresses)
        print("Creating people...")
        people = []
        for i, name in enumerate(ACTOR_NAMES):
            profession = "actor" if name in ACTORS else "actress"
            person = Person(
                nconst=f"nm{1000000 + i:07d}",
                primary_name=name,
                birth_year=randint(1950, 1995),
                death_year=None if randint(0, 10) > 1 else randint(2000, 2023),
                primary_profession=profession,
                known_for_titles=f"tt{randint(1000000, 9999999):07d}",
            )
            people.append(person)

        session.add_all(people)
        session.commit()
        print(f"Created {len(people)} people")

        # Create titles (movies)
        print("Creating titles...")
        titles = []
        for i, title in enumerate(MOVIE_TITLES):
            movie = Title(
                tconst=f"tt{2000000 + i:07d}",
                title_type="movie",
                primary_title=title,
                original_title=title,
                is_adult=False,
                start_year=randint(1970, 2023),
                end_year=None,
                runtime_minutes=randint(90, 180),
                genres=",".join([choice(GENRES) for _ in range(randint(1, 3))]),
            )
            titles.append(movie)

        session.add_all(titles)
        session.commit()
        print(f"Created {len(titles)} titles")

        # Create ratings
        print("Creating ratings...")
        ratings = []
        for title in titles:
            rating = Rating(
                tconst=title.tconst,
                average_rating=round(uniform(6.0, 9.5), 1),
                num_votes=randint(10000, 2000000),
            )
            ratings.append(rating)

        session.add_all(ratings)
        session.commit()
        print(f"Created {len(ratings)} ratings")

        # Create principals (actor-movie relationships)
        print("Creating principals...")
        principals = []
        for person in people:
            # Each actor appears in 3-8 movies
            num_movies = randint(3, 8)
            selected_titles = [choice(titles) for _ in range(num_movies)]

            for j, title in enumerate(selected_titles):
                principal = Principal(
                    tconst=title.tconst,
                    ordering=j + 1,
                    nconst=person.nconst,
                    category=person.primary_profession,
                    job=None,
                    characters=f'["Character {j+1}"]',
                )
                principals.append(principal)

        session.add_all(principals)
        session.commit()
        print(f"Created {len(principals)} principals")

        # Create actor ratings (computed from the relationships)
        print("Creating actor ratings...")
        actor_ratings = []

        for person in people:
            # Get all movies this person appeared in
            person_principals = [p for p in principals if p.nconst == person.nconst]
            person_titles = [p.tconst for p in person_principals]
            person_ratings = [r for r in ratings if r.tconst in person_titles]

            if person_ratings:
                # Calculate weighted average rating
                total_weighted_rating = sum(r.average_rating * r.num_votes for r in person_ratings)
                total_votes = sum(r.num_votes for r in person_ratings)
                avg_rating = total_weighted_rating / total_votes if total_votes > 0 else 0

                # Calculate total runtime
                person_title_objects = [t for t in titles if t.tconst in person_titles]
                total_runtime = sum(t.runtime_minutes or 0 for t in person_title_objects)

                actor_rating = ActorRating(
                    primary_name=person.primary_name,
                    profession=person.primary_profession,
                    score=round(avg_rating, 2),
                    number_of_titles=len(person_ratings),
                    total_runtime_minutes=total_runtime,
                )
                actor_ratings.append(actor_rating)

        session.add_all(actor_ratings)
        session.commit()
        print(f"Created {len(actor_ratings)} actor ratings")

        # Create ETL run record
        print("Creating ETL run record...")
        etl_run = ETLRun(
            status="completed",
            started_at=datetime.now() - timedelta(minutes=30),
            finished_at=datetime.now() - timedelta(minutes=5),
            records_processed=len(people) + len(titles) + len(ratings) + len(principals),
            duration_seconds=1500,  # 25 minutes
            error_message=None,
        )
        session.add(etl_run)
        session.commit()
        print("Created ETL run record")

        print("\n✅ Mock data creation completed successfully!")
        print(f"📊 Summary:")
        print(f"   - {len(people)} actors/actresses")
        print(f"   - {len(titles)} movies")
        print(f"   - {len(ratings)} ratings")
        print(f"   - {len(principals)} actor-movie relationships")
        print(f"   - {len(actor_ratings)} computed actor ratings")
        print(f"   - 1 ETL run record")


if __name__ == "__main__":
    create_mock_data()