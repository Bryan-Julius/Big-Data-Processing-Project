import os
import requests
import logging
import time

def download_hurdat_data(url: str, output_dir: str, filename: str,retries: int = 3) -> bool:

    # Downloads the HURDAT2 dataset to the raw storage path

    output_path = os.path.join(output_dir, filename)
    logging.info(f"Downloading HURDAT2 data from {url}...")

    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()

            with open(output_path, 'wb') as f:
                f.write(response.content)

            logging.info(f"Successfully saved HURDAT2 to {output_path}")
            return True
        except requests.exceptions.RequestException as e:
            logging.warning(f"Attempt {attempt + 1} failed: {e}")
            if attempt < retries - 1:
                time.sleep(2) # Wait 2 seconds before retrying

    logging.error("All download attempts failed.")
    return False