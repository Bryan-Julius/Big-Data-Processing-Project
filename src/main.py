import os
import yaml
import logging
from dotenv import load_dotenv

# Import our custom modules
from fetch.fetch_hurdat import download_hurdat_data
from fetch.fetch_goes import download_sample_goes_data
from processing.spark_processor import create_spark_session, process_hurdat_data

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


    # First phase get data

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


    # Second phase spark processing and data models

    logging.info(" Phase 2: PROCESSING (SPARK)")
    spark = create_spark_session()

    hurdat_input = os.path.join(raw_path, hurdat_cfg['filename'])
    process_hurdat_data(spark, hurdat_input, processed_path)

    # Shut down the Spark cluster cleanly
    spark.stop()
    logging.info(" Pipeline Execution Complete. Data is processed and persisted in Data Lake.")

if __name__ == "__main__":
    main()