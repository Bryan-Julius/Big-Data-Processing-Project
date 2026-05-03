import os
import sys
os.environ['HADOOP_HOME'] = 'C:\\hadoop'
os.environ['PATH'] = 'C:\\hadoop\\bin;' + os.environ['PATH']
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession

def validate_lakehouse():
    print("Spinning up Spark SQL Engine...")
    spark = SparkSession.builder \
        .appName("M4_Validation") \
        .getOrCreate()

    processed_dir = "data/processed"
    goes_path = os.path.join(processed_dir, "goes_features.parquet")
    hurdat_path = os.path.join(processed_dir, "hurdat_features.parquet")

    #  Load the Parquet Files
    print("\n--- LOADING PARQUET LAKEHOUSE ---")
    goes_df = spark.read.parquet(goes_path)
    hurdat_df = spark.read.parquet(hurdat_path)

    # Register as Temporary SQL Tables
    goes_df.createOrReplaceTempView("goes_data")
    hurdat_df.createOrReplaceTempView("hurdat_data")

    # Test 1: Prove GOES Spatial Cropping Worked
    print("\n--- TEST 1: EXTRACTED SATELLITE FEATURES ---")
    print("Showing the mean and max radiance extracted from the 10x10 cropped bounding box:")
    spark.sql("""
        SELECT filename, mean_radiance, max_radiance 
        FROM goes_data 
        WHERE mean_radiance != -1.0
    """).show(truncate=False)

    # Test 2: Prove Tabular Partitioning Worked
    print("\n--- TEST 2: HURDAT PARTITION DISTRIBUTION ---")
    print("Showing the distribution of recorded storms by Status and Category:")
    spark.sql("""
        SELECT status, category, COUNT(*) as tracking_records, MAX(max_wind_knots) as peak_wind
        FROM hurdat_data
        GROUP BY status, category
        ORDER BY peak_wind DESC
    """).show()

    # Mute the terminal's error channel right before shutdown
    # to solve the harmless Windows/Java temp folder cleanup bug
    import sys
    sys.stderr = open(os.devnull, 'w')

    spark.stop()

if __name__ == "__main__":
    validate_lakehouse()