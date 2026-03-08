import os
import boto3
from botocore import UNSIGNED
from botocore.config import Config
import logging

def download_sample_goes_data(bucket_name: str, prefix: str, output_dir: str, max_files: int = 2) -> bool:

    # Downloads sample GOES-16 satellite imagery from AWS S3 public bucket
    logging.info(f"Connecting to public S3 bucket: {bucket_name}...")

    try:
        # Use UNSIGNED config since NOAA buckets are public
        s3_client = boto3.client('s3', config=Config(signature_version=UNSIGNED))

        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        if 'Contents' not in response:
            logging.warning("No files found at the specified S3 prefix.")
            return False

        files_downloaded = 0
        for obj in response['Contents']:
            if files_downloaded >= max_files:
                break

            file_key = obj['Key']
            filename = os.path.basename(file_key)
            output_path = os.path.join(output_dir, filename)

            logging.info(f"Downloading {filename}...")
            s3_client.download_file(bucket_name, file_key, output_path)
            logging.info(f"Saved to {output_path}")

            files_downloaded += 1

        return True
    except Exception as e:
        logging.error(f"Failed to download GOES data from S3: {e}")
        return False