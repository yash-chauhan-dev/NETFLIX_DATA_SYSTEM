import logging
import sys

# Configure Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", handlers=[
    logging.FileHandler("etl.log"),
    logging.StreamHandler(sys.stdout)
])


def load(df, table_name, postgres_config):

    try:
        df.write \
            .format("jdbc") \
            .option("url", postgres_config["POSTGRES_URL"]) \
            .option("user", postgres_config["POSTGRES_USER"]) \
            .option("password", postgres_config["POSTGRES_PASSWORD"]) \
            .option("dbtable", table_name) \
            .mode("overwrite") \
            .save()
        logging.info(f"Data loaded to {table_name} successfully.")

    except Exception as e:
        logging.error(f"Data loading failed: {table_name} : {e}")
        sys.exit(1)
