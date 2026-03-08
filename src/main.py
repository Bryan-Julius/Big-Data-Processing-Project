import os
import yaml
import logging
from dotenv import load_dotenv
from fetch.fetch_hurdat import download_hurdat_data
from fetch.fetch_goes import download_sample_goes_data

# Set up basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_config(config_path: str = 'config/settings.yaml'):
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def ensure_directories(config: dict):
    # Check for directories
    os.makedirs(config['storage']['raw_data_path'], exist_ok=True)
    os.makedirs(config['storage']['processed_data_path'], exist_ok=True)

def main():
    logging.info("Starting Hurricane Big Data Pipeline")

    # Load Environment Variables and Config
    load_dotenv()
    config = load_config()
    ensure_directories(config)

    raw_path = config['storage']['raw_data_path']

    #  Fetch HURDAT2 Data
    hurdat_cfg = config['sources']['hurdat']
    download_hurdat_data(hurdat_cfg['url'], raw_path, hurdat_cfg['filename'])

    # Fetch GOES-16 Satellite Data
    goes_cfg = config['sources']['goes_s3']
    download_sample_goes_data(
        bucket_name=goes_cfg['bucket_name'],
        prefix=goes_cfg['product_prefix'],
        output_dir=raw_path,
        max_files=goes_cfg['max_files_to_download']
    )

    logging.info("Pipeline Run Complete. Data is persisted in storage.")

if __name__ == "__main__":
    main()