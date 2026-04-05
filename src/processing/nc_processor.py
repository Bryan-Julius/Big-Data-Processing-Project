import os
import xarray as xr
import logging
from pyspark.sql.types import StructType, StructField, StringType, FloatType

def process_goes_data(spark, raw_dir, processed_dir):
    """Distributed processing of NetCDF satellite imagery using Spark RDDs."""
    logging.info("Starting distributed transformation of GOES-16 NetCDF imagery...")

    #  Gather all .nc files in the local HDFS ingestion zone
    nc_files = [os.path.join(raw_dir, f) for f in os.listdir(raw_dir) if f.endswith('.nc')]

    if not nc_files:
        logging.warning("No NetCDF files found to process.")
        return False


    # Distributed Scaling Architecture

    # Parallelize the file paths across the Spark Cluster
    # This allows for limitless horizontal scaling if deployed to the cloud
    paths_rdd = spark.sparkContext.parallelize(nc_files)

    def extract_tensors(file_path):
        """Worker function executed in parallel across the cluster."""
        try:
            # Engine 'netcdf4' is required for complex GOES-16 data
            ds = xr.open_dataset(file_path, engine='netcdf4')

            # Extract the 'CMI' (Radiance) tensor multi-dimensional array
            radiance = ds['CMI']

            # To avoid memory bottlenecks, we extract statistical summary tensors
            # to serve as ML features, rather than storing raw 2000x2000 arrays.
            mean_rad = float(radiance.mean().values)
            max_rad = float(radiance.max().values)

            filename = os.path.basename(file_path)
            ds.close()
            return (filename, mean_rad, max_rad, "VALID")
        except Exception as e:
            # Robust Error Handling: Catch corrupted files without crashing the cluster
            return (os.path.basename(file_path), 0.0, 0.0, f"ERROR: {str(e)}")

    #  Execute the distributed Map transformation
    processed_rdd = paths_rdd.map(extract_tensors)

    #  Enforce Schema and Convert back to DataFrame
    schema = StructType([
        StructField("filename", StringType(), True),
        StructField("mean_radiance", FloatType(), True),
        StructField("max_radiance", FloatType(), True),
        StructField("status", StringType(), True)
    ])
    goes_df = spark.createDataFrame(processed_rdd, schema)

    #  Filter out corrupted images identified by the worker nodes
    valid_goes_df = goes_df.filter(goes_df.status == "VALID").drop("status")

    #  Save the extracted image features to the Parquet Data Lake
    target_path = os.path.join(processed_dir, "goes_features.parquet")
    valid_goes_df.write.mode("overwrite").parquet(target_path)

    logging.info(f"Successfully extracted NetCDF tensors and saved Parquet to: {target_path}")
    return True