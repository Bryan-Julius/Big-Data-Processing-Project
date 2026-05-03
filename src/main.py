import os
import sys
os.environ['HADOOP_HOME'] = 'C:\\hadoop'
os.environ['PATH'] = 'C:\\hadoop\\bin;' + os.environ['PATH']
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

src_path = os.path.abspath("src")
sys.path.insert(0, src_path)
os.environ['PYTHONPATH'] = src_path

import yaml
import logging
from dotenv import load_dotenv

# Import custom modules
from fetch.fetch_hurdat import download_hurdat_data
from fetch.fetch_goes import download_sample_goes_data
from processing.spark_processor import process_data

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_config(config_path='config/settings.yaml'):
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def ensure_directories(config: dict):
    """Ensure storage directories exist."""
    os.makedirs(config['storage']['raw_data_path'], exist_ok=True)
    os.makedirs(config['storage']['processed_data_path'], exist_ok=True)

def main():
    logging.info("Starting Hurricane Big Data Pipeline...")

    # Load Configuration
    load_dotenv()
    config = load_config()
    ensure_directories(config)

    raw_path = config['storage']['raw_data_path']
    processed_path = config['storage']['processed_data_path']


    # Phase 1: Get data
    logging.info(" Phase 1: Get data")
    hurdat_cfg = config['sources']['hurdat']
    goes_cfg = config['sources']['goes_s3']

    download_hurdat_data(hurdat_cfg['url'], raw_path, hurdat_cfg['filename'])
    download_sample_goes_data(
        bucket_name=goes_cfg['bucket_name'],
        prefix=goes_cfg['product_prefix'],
        output_dir=raw_path,
        max_files=goes_cfg['max_files_to_download']
    )


    # Phase 2 & 3: Distributed Processing (Temporal Join & Spatial Cropping)
    logging.info(" Phase 2 & 3: Processing (SPARK Temporal Join & Spatial Cropping)")
    hurdat_input = os.path.join(raw_path, hurdat_cfg['filename'])

    # This single orchestrator function now spins up Spark, joins the tables,
    # distributes the pyproj math, saves the Parquet Lakehouse, and shuts Spark down.
    process_data(
        hurdat_file=hurdat_input,
        raw_dir=raw_path,
        processed_dir=processed_path
    )

    logging.info(" Pipeline Execution Complete. Data is processed and persisted in Data Lakehouse.")

if __name__ == "__main__":
    main()