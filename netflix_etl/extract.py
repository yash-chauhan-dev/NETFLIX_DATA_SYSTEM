import logging
import sys

# Configure Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", handlers=[
    logging.FileHandler("etl.log"),
    logging.StreamHandler(sys.stdout)
])


def extract(spark, postgres_config):

    try:

        logging.info("Extracting data...")

        watch_history_df = spark.read \
            .format("jdbc") \
            .option("url", postgres_config["POSTGRES_URL"]) \
            .option("user", postgres_config["POSTGRES_USER"]) \
            .option("password", postgres_config["POSTGRES_PASSWORD"]) \
            .option("dbtable", "watch_history") \
            .load()
        logging.info("watch_history dataframe created successfully.")

        movie_shows_df = spark.read \
            .format("jdbc") \
            .option("url", postgres_config["POSTGRES_URL"]) \
            .option("user", postgres_config["POSTGRES_USER"]) \
            .option("password", postgres_config["POSTGRES_PASSWORD"]) \
            .option("dbtable", "movies_shows") \
            .load()
        logging.info("movie_shows dataframe created successfully.")

        episodes_df = spark.read \
            .format("jdbc") \
            .option("url", postgres_config["POSTGRES_URL"]) \
            .option("user", postgres_config["POSTGRES_USER"]) \
            .option("password", postgres_config["POSTGRES_PASSWORD"]) \
            .option("dbtable", "episodes") \
            .load()
        logging.info("episodes dataframe created successfully.")

        content_genres_df = spark.read \
            .format("jdbc") \
            .option("url", postgres_config["POSTGRES_URL"]) \
            .option("user", postgres_config["POSTGRES_USER"]) \
            .option("password", postgres_config["POSTGRES_PASSWORD"]) \
            .option("dbtable", "content_genres") \
            .load()
        logging.info("content_genres dataframe created successfully.")

        subscriptions_df = spark.read \
            .format("jdbc") \
            .option("url", postgres_config["POSTGRES_URL"]) \
            .option("user", postgres_config["POSTGRES_USER"]) \
            .option("password", postgres_config["POSTGRES_PASSWORD"]) \
            .option("dbtable", "subscriptions") \
            .load()
        logging.info("subscriptions dataframe created successfully.")

        genres_df = spark.read \
            .format("jdbc") \
            .option("url", postgres_config["POSTGRES_URL"]) \
            .option("user", postgres_config["POSTGRES_USER"]) \
            .option("password", postgres_config["POSTGRES_PASSWORD"]) \
            .option("dbtable", "genres") \
            .load()
        logging.info("Genres dataframe created successfully.")

        return {
            "watch_history_df": watch_history_df,
            "movie_shows_df": movie_shows_df,
            "episodes_df": episodes_df,
            "content_genres_df": content_genres_df,
            "subscriptions_df": subscriptions_df,
            "genres_df": genres_df
        }

    except Exception as e:
        logging.error(f"Data extraction failed: {e}")
        sys.exit(1)
