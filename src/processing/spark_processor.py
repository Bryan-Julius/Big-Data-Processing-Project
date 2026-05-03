import os
import re
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, split, trim
from pyspark.sql.types import StructType, StructField, StringType, FloatType, Row
from processing.nc_processor import extract_features
import sys

def process_data(hurdat_file, raw_dir, processed_dir):
    spark = SparkSession.builder \
        .appName("HurricaneIntensityPipeline") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()

    # Process Hurdat2 (Native JVM DataFrames)
    df = spark.read.text(hurdat_file)

    # Filter out header rows (Tracking rows always start with an 8-digit date YYYYMMDD)
    tracking_df = df.filter(col("value").rlike("^[0-9]{8}"))

    # Split the comma-separated string
    split_cols = split(tracking_df['value'], ',')

    hurdat_df = tracking_df \
        .withColumn("date", trim(split_cols.getItem(0))) \
        .withColumn("time", trim(split_cols.getItem(1))) \
        .withColumn("status", trim(split_cols.getItem(3))) \
        .withColumn("latitude", trim(split_cols.getItem(4))) \
        .withColumn("longitude", trim(split_cols.getItem(5))) \
        .withColumn("max_wind_knots", trim(split_cols.getItem(6)).cast("int")) \
        .drop("value")

    # Filter out missing data (-99) and empty status lines
    hurdat_df = hurdat_df.dropna(subset=["date", "latitude", "longitude", "max_wind_knots"]) \
        .filter(col("max_wind_knots") != -99) \
        .filter(col("status") != "")

    # Saffir-Simpson engineering
    hurdat_df = hurdat_df.withColumn(
        "category",
        expr("""
            CASE 
                WHEN max_wind_knots >= 137 THEN 'Cat_5'
                WHEN max_wind_knots >= 113 THEN 'Cat_4'
                WHEN max_wind_knots >= 96 THEN 'Cat_3'
                WHEN max_wind_knots >= 83 THEN 'Cat_2'
                WHEN max_wind_knots >= 64 THEN 'Cat_1'
                WHEN max_wind_knots >= 34 THEN 'TS'
                ELSE 'TD'
            END
        """)
    )

    # Clean Lat/Lon strings into Decimals for the pyproj math
    hurdat_df = hurdat_df.withColumn(
        "lat_decimal",
        expr("CASE WHEN latitude LIKE '%S' THEN -1 * cast(substring(latitude, 1, length(latitude)-1) as float) ELSE cast(substring(latitude, 1, length(latitude)-1) as float) END")
    ).withColumn(
        "lon_decimal",
        expr("CASE WHEN longitude LIKE '%W' THEN -1 * cast(substring(longitude, 1, length(longitude)-1) as float) ELSE cast(substring(longitude, 1, length(longitude)-1) as float) END")
    )


    # Temporal Join Metadata Prep
    nc_files = [f for f in os.listdir(raw_dir) if f.endswith('.nc')]
    goes_metadata = []

    for f in nc_files:
        # Extract the 's20232401200' part of the filename
        match = re.search(r'_s(\d{4})(\d{3})(\d{2})(\d{2})', f)
        if match:
            year, julian_day, hour, minute = match.groups()
            # Convert Julian Day to standard YYYYMMDD
            date_obj = datetime(int(year), 1, 1) + timedelta(days=int(julian_day) - 1)
            standard_date = date_obj.strftime('%Y%m%d')
            standard_time = f"{hour}{minute}"

            goes_metadata.append(Row(
                filename=f,
                file_path=os.path.join(raw_dir, f),
                goes_date=standard_date,
                goes_time=standard_time
            ))

    # Create a DataFrame of the images and their timestamps
    goes_meta_df = spark.createDataFrame(goes_metadata)


    # Spatial Temporal Join
    # Join HURDAT and GOES where the dates and times match exactly
    joined_df = goes_meta_df.join(
        hurdat_df,
        (goes_meta_df.goes_date == hurdat_df.date) & (goes_meta_df.goes_time == hurdat_df.time),
        "inner"
    )


    # Distributed Tensor Extraction
    # Now that they are joined, we pass the file path AND the exact coordinates to the worker nodes
    extracted_rdd = joined_df.rdd.map(
        lambda row: extract_features(
            file_path=row.file_path,
            lat_decimal=row.lat_decimal,
            lon_decimal=row.lon_decimal
        )
    )

    # Save the Final Data Lakehouse
    goes_schema = StructType([
        StructField("filename", StringType(), True),
        StructField("mean_radiance", FloatType(), True),
        StructField("max_radiance", FloatType(), True),
    ])

    final_goes_df = spark.createDataFrame(extracted_rdd, schema=goes_schema)

    # Write HURDAT partitions
    hurdat_df.write.mode("overwrite") \
        .partitionBy("status", "category") \
        .parquet(os.path.join(processed_dir, "hurdat_features.parquet"))

    # Write Cropped GOES features
    final_goes_df.write.mode("overwrite") \
        .parquet(os.path.join(processed_dir, "goes_features.parquet"))

    print("Pipeline Execution Complete. Parquet Lakehouse created.")

    # Handle Windows specific error
    sys.stderr = open(os.devnull, 'w')
    spark.stop()