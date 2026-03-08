import os
import requests
import logging

def download_hurdat_data(url: str, output_dir: str, filename: str) -> bool:

    # Downloads the HURDAT2 dataset to the raw storage path

    output_path = os.path.join(output_dir, filename)

    logging.info(f"Downloading HURDAT2 data from {url}...")
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        with open(output_path, 'wb') as f:
            f.write(response.content)

        logging.info(f"Successfully saved HURDAT2 to {output_path}")
        return True
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to download HURDAT data: {e}")
        return False