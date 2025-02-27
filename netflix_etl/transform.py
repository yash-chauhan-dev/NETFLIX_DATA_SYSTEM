from pyspark.sql.functions import col, countDistinct, date_format, sum, current_date, count, concat, year, weekofyear
import logging
import sys
import traceback

# Configure Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", handlers=[
    logging.FileHandler("etl.log"),
    logging.StreamHandler(sys.stdout)
])


def transform(
        watch_history_df,
        movie_shows_df,
        episodes_df,
        content_genres_df,
        subscriptions_df,
        genres_df
):

    try:

        logging.info("Transforming data...")

        # Convert timestamp to date format
        watch_history_df = watch_history_df \
            .withColumn("watch_date", date_format(col("last_watched"), "yyyy-MM-dd"))

        # Daily Active Users (DAU)
        dau_df = watch_history_df \
            .groupBy("watch_date") \
            .agg(countDistinct("profile_id").alias("active_users"))

        logging.info("Data transformation for DAU successful.")

        # Weekly Active Users (WAU)
        wau_df = watch_history_df \
            .withColumn("week", concat(year(col("last_watched")), weekofyear(col("last_watched")))) \
            .groupBy("week") \
            .agg(countDistinct("profile_id").alias("active_users"))

        logging.info("Data transformation for WAU successful.")

        # Monthly Active Users (MAU)
        mau_df = watch_history_df \
            .withColumn("month", date_format(col("last_watched"), "yyyy-MM")) \
            .groupBy("month") \
            .agg(countDistinct("profile_id").alias("active_users"))

        logging.info("Data transformation for MAU successful.")

        # Total Watch Time
        total_content = movie_shows_df \
            .select("content_id", "duration") \
            .union(episodes_df.select("content_id", "duration"))

        content_durations = total_content \
            .groupBy("content_id") \
            .agg(sum("duration").alias("duration"))

        total_watch_time_df = watch_history_df \
            .join(
                content_durations,
                watch_history_df["content_id"] == content_durations["content_id"],
                "inner") \
            .groupBy(watch_history_df["profile_id"]) \
            .agg(
                sum(
                    content_durations["duration"] *
                    (watch_history_df["progress_percentage"]/100)
                ).alias("total_watch_time")
            )

        logging.info("Data transformation for TOTAL_WATCH_TIME successful.")

        # Most-Watched Movies & TV Shows
        most_watched_movies_df = watch_history_df \
            .groupBy("content_id") \
            .agg(count("*").alias("watch_count"))

        logging.info("Data transformation for MOST_WATCHED_MOVIE successful.")

        # most-watched genres
        most_watched_genres_df = watch_history_df\
            .join(content_genres_df,
                  watch_history_df["content_id"] == content_genres_df["content_id"],
                  "inner") \
            .groupBy(content_genres_df["genre_id"]) \
            .agg(count(watch_history_df["content_id"]).alias("watch_count")) \
            .join(genres_df,
                  content_genres_df["genre_id"] == genres_df["genre_id"],
                  "inner") \
            .select(genres_df["genre_name"], col("watch_count"))

        logging.info("Data transformation for MOST_WATCHED_GENRES successful.")

        # Churn Rate
        total_users = subscriptions_df.select("user_id").distinct().count()
        churned_user = subscriptions_df \
            .where(col("end_date") >= (current_date() - 30)) \
            .select("user_id") \
            .distinct() \
            .count()

        churn_rate = (churned_user / total_users)

        logging.info(f"Churn Rate: {churn_rate:.2%}")

        # Trending Content
        last_7_days_watch_history_df = watch_history_df \
            .filter(
                col("last_watched") > (current_date() - 7)
            )

        trending_content_df = last_7_days_watch_history_df \
            .join(movie_shows_df,
                  last_7_days_watch_history_df["content_id"] == movie_shows_df["content_id"],
                  "inner") \
            .groupBy(movie_shows_df["title"]) \
            .agg(
                count(
                    last_7_days_watch_history_df["profile_id"]
                ).alias("watch_count")
            )

        logging.info("Data transformation successful.")

        return {
            "dau_df": dau_df,
            "wau_df": wau_df,
            "mau_df": mau_df,
            "total_watch_time_df": total_watch_time_df,
            "most_watched_movies_df": most_watched_movies_df,
            "most_watched_genres_df": most_watched_genres_df,
            "trending_content_df": trending_content_df
        }

    except Exception as e:
        logging.error(f"Data transformation failed: {traceback.format_exc()}")
        sys.exit(1)
