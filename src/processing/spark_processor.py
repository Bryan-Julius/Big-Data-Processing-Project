import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, trim

def create_spark_session():
    """Initializes a local Spark Session for Data Lakehouse processing."""
    logging.info("Initializing local Spark Session...")
    return SparkSession.builder \
        .appName("HurricaneDataLake") \
        .config("spark.driver.memory", "8g") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()

def process_hurdat_data(spark, input_path, output_path):
    """Processes structured HURDAT2 text data into a defined schema and Parquet files."""
    logging.info("Starting Spark transformation on HURDAT2 data...")

    try:
        #  Read raw CSV-like text
        df = spark.read.text(input_path)

        #  Filter out header rows (Tracking rows always start with an 8-digit date YYYYMMDD)
        tracking_df = df.filter(col("value").rlike("^[0-9]{8}"))

        # Clean and Structure
        # split the single comma-separated string into actual, usable columns
        split_cols = split(tracking_df['value'], ',')

        structured_df = tracking_df \
            .withColumn("date", trim(split_cols.getItem(0))) \
            .withColumn("time", trim(split_cols.getItem(1))) \
            .withColumn("status", trim(split_cols.getItem(3))) \
            .withColumn("latitude", trim(split_cols.getItem(4))) \
            .withColumn("longitude", trim(split_cols.getItem(5))) \
            .withColumn("max_wind_knots", trim(split_cols.getItem(6)).cast("integer")) \
            .drop("value") # Drop the messy original column

        # Save to Parquet
        target_path = os.path.join(output_path, "hurdat_features.parquet")
        structured_df.write.mode("overwrite").parquet(target_path)

        logging.info(f"Successfully processed HURDAT2 data and saved Parquet to: {target_path}")
        return True

    except Exception as e:
        logging.error(f"Spark processing failed: {e}")
        return False




def query_data_lake(spark, processed_path):
    """Demonstrates the Query Layer using Spark SQL on the Parquet files."""
    logging.info("Executing Spark SQL validation query...")
    try:
        # Load the Parquet files we just created
        parquet_file = os.path.join(processed_path, "hurdat_features.parquet")
        df = spark.read.parquet(parquet_file)

        # Create a temporary SQL table in memory
        df.createOrReplaceTempView("hurricane_data")

        # Run an analytical query (e.g., finding the strongest winds by storm status)
        query = """
            SELECT status, COUNT(*) as record_count, MAX(max_wind_knots) as max_wind 
            FROM hurricane_data 
            WHERE status != '' 
            GROUP BY status 
            ORDER BY max_wind DESC
        """
        result = spark.sql(query)

        logging.info("Query Successful! Showing top results:")
        result.show(truncate=False)
        return True
    except Exception as e:
        logging.error(f"Query Layer failed: {e}")
        return False