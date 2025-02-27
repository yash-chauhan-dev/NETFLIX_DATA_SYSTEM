from netflix_etl.config import get_postgres_config, get_spark_session
from netflix_etl.extract import extract
from netflix_etl.transform import transform
from netflix_etl.load import load
import sys

if "/opt/spark/app" not in sys.path:
    sys.path.append("/opt/spark/app")

spark = get_spark_session()
postgres_config = get_postgres_config()

dataframe_collection = extract(spark, postgres_config)

transformed_df_collection = transform(
    dataframe_collection["watch_history_df"],
    dataframe_collection["movie_shows_df"],
    dataframe_collection["episodes_df"],
    dataframe_collection["content_genres_df"],
    dataframe_collection["subscriptions_df"],
    dataframe_collection["genres_df"]
)

load(transformed_df_collection["dau_df"], "dau_table", postgres_config)
load(transformed_df_collection["wau_df"], "wau_table", postgres_config)
load(transformed_df_collection["mau_df"], "mau_table", postgres_config)
load(transformed_df_collection["total_watch_time_df"],
     "watch_time_table", postgres_config)
load(transformed_df_collection["most_watched_movies_df"],
     "most_watched_movies_table", postgres_config)
load(transformed_df_collection["most_watched_genres_df"],
     "most_watched_genres_table", postgres_config)
load(transformed_df_collection["trending_content_df"],
     "trending_content_table", postgres_config)
