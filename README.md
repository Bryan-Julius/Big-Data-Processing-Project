# Scalable Atmospheric Data Pipeline for Global Tropical Cyclone Intensity Forecasting

## 1. Project Overview
This project focuses on building a resilient, distributed data pipeline to process high-volume NetCDF satellite imagery and structured HURDAT2 track data. By utilizing Apache Spark and Parquet encoding, the system normalizes disparate meteorological datasets into a unified Data Lake architecture optimized for high-throughput querying.

## 2. Current Status (Milestone 2: Initial Implementation)
This repository currently reflects the **M2 Proof of Concept**.
* **Data Acquisition (Working):** Multi-source ingestion is operational. The pipeline successfully connects to the NHC HTTP servers to retrieve structured HURDAT2 track data, and authenticates with NOAA's public AWS S3 bucket (`noaa-goes16`) to retrieve raw, unstructured GOES-16 satellite imagery (.nc format).
* **Storage (Working):** Raw data is persistently downloaded and stored in the local `data/raw/` directory. This serves as our simulated HDFS ingestion zone for M2.
* **Pipeline Structure (Working):** The codebase utilizes a modular design, separating ingestion logic from the main orchestrator, with centralized YAML configuration management.
* **In Progress (Targeting M3):** Apache Spark integration for distributed tensor extraction, Parquet file encoding, and HBase feature store population.

## 3. Project Structure
```text
hurricane_pipeline/
├── config/
│   ├── settings.yaml       # Central configuration (URLs, S3 buckets, limits)
│   └── .env.example        # Template for environment variables
├── data/
│   ├── raw/                # Persistent storage for downloaded .nc and .txt files
│   └── processed/          # Target for future Spark-processed Parquet files
├── src/
│   ├── fetch/              # Data acquisition modules
│   │   ├── fetch_goes.py   # S3 connection and download logic
│   │   └── fetch_hurdat.py # HTTP request logic
│   └── main.py             # Pipeline orchestrator
├── requirements.txt        # Python dependencies
└── README.md

```

## 4. Environment Setup
Prerequisites: Python 3.10 or 3.11 is highly recommended to ensure future compatibility with PySpark.

Clone the repository:

Bash
git clone https://github.com/Bryan-Julius/Big-Data-Processing-Project
cd hurricane_pipeline
Create and activate a virtual environment:

Windows (PowerShell):

PowerShell
py -3.11 -m venv venv
.\venv\Scripts\Activate.ps1

Mac/Linux:

Bash
python3.11 -m venv venv
source venv/bin/activate

Install dependencies:

Bash
pip install -r requirements.txt

Configure Environment Variables:
Copy the example environment file. (Note: Because NOAA S3 buckets and NHC servers are public, API keys are not strictly required to execute the M2 pipeline, but the .env file is required by the orchestrator).

Bash
cp config/.env.example .env
## 5. How to Run the Pipeline

To execute the data acquisition phase of the pipeline, run the main file from the root directory:

Bash python src/main.py

Expected Output: The terminal will display logging information detailing the connection and download progress. Upon completion, verify that the raw satellite data (.nc files) and track data (.txt files) have been successfully persisted in the data/raw/ directory.