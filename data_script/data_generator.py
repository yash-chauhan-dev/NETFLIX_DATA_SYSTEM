import random
import faker
import psycopg2
import logging
import sys
import time
from datetime import datetime, timedelta

# Initialize Faker
fake = faker.Faker()

# Configure Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", handlers=[
    logging.FileHandler("data_generation.log"),
    logging.StreamHandler(sys.stdout)
])

# Database Connection (Update with actual credentials)
try:
    conn = psycopg2.connect(
        dbname="netflix_streaming",
        user="user",
        password="password",
        host="localhost",
        port="5432"
    )
    cursor = conn.cursor()
    logging.info("Database connection established successfully.")
except Exception as e:
    logging.error(f"Database connection failed: {e}")
    sys.exit(1)

# Get user input for number of records
num_users = int(input("Enter the number of users to generate: "))
num_movies_shows = int(input("Enter the number of movies/shows to generate: "))


def show_progress(current, total, task_name):
    percent = int((current / total) * 100)
    bar = "#" * (percent // 2) + "-" * (50 - (percent // 2))
    sys.stdout.write(f"\r{task_name}: [{bar}] {percent}%")
    sys.stdout.flush()


# Insert Subscription Plans and Commit
subscription_plans = [
    ("Basic", 9.99, 1),
    ("Standard", 15.99, 2),
    ("Premium", 19.99, 4)
]
cursor.executemany(
    "INSERT INTO subscription_plans (plan_name, price, max_devices) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING", subscription_plans)
conn.commit()
logging.info("Subscription plans inserted and committed.")

# Insert Genres and Commit
genres = ["Action", "Comedy", "Drama", "Horror", "Sci-Fi",
          "Thriller", "Romance", "Documentary", "Adventure"]
cursor.executemany("INSERT INTO genres (genre_name) VALUES (%s) ON CONFLICT DO NOTHING", [
                   (g,) for g in genres])
conn.commit()
logging.info("Genres inserted and committed.")

# Fetch Genre IDs
cursor.execute("SELECT genre_id, genre_name FROM genres")
genre_dict = {name: gid for gid, name in cursor.fetchall()}

# Fetch Subscription Plan IDs
cursor.execute("SELECT plan_id FROM subscription_plans")
plan_ids = [row[0] for row in cursor.fetchall()]

# Generate Users & Profiles
user_ids = []
profile_ids = []
for i in range(num_users):
    email = fake.unique.email()
    password_hash = fake.sha256()

    cursor.execute(
        "INSERT INTO users (email, password_hash) VALUES (%s, %s) RETURNING user_id", (email, password_hash))
    user_id = cursor.fetchone()[0]
    user_ids.append(user_id)

    for _ in range(random.randint(1, 3)):
        name = fake.first_name()
        age = random.randint(5, 70)
        cursor.execute(
            "INSERT INTO profiles (user_id, name, age) VALUES (%s, %s, %s) RETURNING profile_id", (user_id, name, age))
        profile_ids.append(cursor.fetchone()[0])

    show_progress(i + 1, num_users, "Generating Users")

conn.commit()
logging.info("Users and profiles inserted and committed.")

# Generate Subscriptions
for i, user_id in enumerate(user_ids):
    plan_id = random.choice(plan_ids)  # ✅ Ensure valid plan_id
    start_date = fake.date_between(start_date="-1y", end_date="today")
    end_date = start_date + timedelta(days=30 * random.randint(1, 12))
    status = "active" if end_date > datetime.today().date() else "expired"

    cursor.execute("INSERT INTO subscriptions (user_id, plan_id, start_date, end_date, status) VALUES (%s, %s, %s, %s, %s)",
                   (user_id, plan_id, start_date, end_date, status))

    show_progress(i + 1, len(user_ids), "Generating Subscriptions")

conn.commit()
logging.info("Subscriptions inserted and committed.")

# Generate Movies & TV Shows
content_ids = []
for i in range(num_movies_shows):
    title = fake.sentence(nb_words=3)
    content_type = random.choice(["movie", "tv_show"])
    release_year = random.randint(1950, 2024)
    language = fake.language_name()

    if content_type == "movie":
        duration = random.randint(60, 180)
        total_seasons = None
    else:
        duration = None
        total_seasons = random.randint(1, 10)

    cursor.execute("INSERT INTO movies_shows (title, content_type, release_year, language, duration, total_seasons) VALUES (%s, %s, %s, %s, %s, %s) RETURNING content_id",
                   (title, content_type, release_year, language, duration, total_seasons))
    content_id = cursor.fetchone()[0]
    content_ids.append(content_id)

    for _ in range(random.randint(1, 3)):
        genre_id = random.choice(list(genre_dict.values()))
        cursor.execute(
            "INSERT INTO content_genres (content_id, genre_id) VALUES (%s, %s) ON CONFLICT DO NOTHING", (content_id, genre_id))

    if content_type == "tv_show":
        for season in range(1, total_seasons + 1):
            for episode in range(1, random.randint(5, 15)):
                episode_title = fake.sentence(nb_words=4)
                episode_duration = random.randint(20, 60)
                cursor.execute("INSERT INTO episodes (content_id, season_number, episode_number, title, duration) VALUES (%s, %s, %s, %s, %s)",
                               (content_id, season, episode, episode_title, episode_duration))

    show_progress(i + 1, num_movies_shows, "Generating Movies & Shows")

conn.commit()
logging.info("Movies and TV shows inserted and committed.")

# Generate Watch History & Ratings
for i, profile_id in enumerate(profile_ids):
    watched_content = random.sample(content_ids, random.randint(3, 10))
    for content_id in watched_content:
        last_watched = fake.date_time_between(start_date="-1y", end_date="now")
        progress = random.randint(0, 100)

        cursor.execute("INSERT INTO watch_history (profile_id, content_id, last_watched, progress_percentage) VALUES (%s, %s, %s, %s)",
                       (profile_id, content_id, last_watched, progress))

        if random.random() > 0.5:
            rating = random.randint(1, 5)
            review = fake.text(
                max_nb_chars=200) if random.random() > 0.7 else None
            cursor.execute("INSERT INTO ratings (profile_id, content_id, rating, review) VALUES (%s, %s, %s, %s)",
                           (profile_id, content_id, rating, review))

    show_progress(i + 1, len(profile_ids),
                  "Generating Watch History & Ratings")

conn.commit()
logging.info("Watch history and ratings inserted and committed.")

# Close Connection
cursor.close()
conn.close()
logging.info("Data generation completed successfully!")
print("\nData generation completed successfully!")
