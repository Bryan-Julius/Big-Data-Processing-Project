import logging
from pyspark.sql import SparkSession

def create_spark_session():
    """Initializes a local Spark Session for Data Lakehouse processing."""
    logging.info("Initializing local Spark Session...")

    # configure Spark to use 8GB of RAM for local processing
    # and set the default compression for our output Parquet files.
    spark = SparkSession.builder \
        .appName("HurricaneDataLake") \
        .config("spark.driver.memory", "8g") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()

    logging.info("Spark Session successfully created.")
    return spark