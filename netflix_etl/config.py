from pyspark.sql import SparkSession
import logging
import sys

# Configure Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", handlers=[
    logging.FileHandler("etl.log"),
    logging.StreamHandler(sys.stdout)
])

# PostgreSQL Credentials
SOURCE_POSTGRES_URL = "jdbc:postgresql://localhost:5432/netflix_streaming"
DESTINATION_POSTGRES_URL = "jdbc:postgresql://localhost:5432/netflix_analytics"
POSTGRES_USER = "user"
POSTGRES_PASSWORD = "password"


def get_postgres_config():

    configuration = {
        "POSTGRES_URL": "jdbc:postgresql://postgres-db:5432/netflix_streaming",
        "POSTGRES_USER": "user",
        "POSTGRES_PASSWORD": "password",
    }

    return configuration


def get_spark_session():

    spark = SparkSession.builder \
        .appName("Netflix Analytics ETL") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
        .getOrCreate()

    logging.info("Spark session created successfully.")

    return spark
