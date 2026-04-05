# Scalable Atmospheric Data Pipeline for Global Tropical Hurricane Intensity Forecasting

## Project Overview
This project implements an end-to-end Big Data pipeline designed to acquire, process, and analyze meteorological data. It integrates unstructured NetCDF satellite imagery (NOAA GOES-16) with structured tabular tracking data (NHC HURDAT2) into a unified, distributed Data Lakehouse architecture utilizing Apache Spark and Parquet.

---

## 1. Pipeline Stages
The pipeline executes sequentially in three distinct phases without manual intervention:

* **Phase 1: Data Acquisition**
    * Connects to the National Hurricane Center (NHC) HTTP server to download historical HURDAT2 text data.
    * Authenticates anonymously with NOAA's public Amazon S3 bucket (`noaa-goes16`) via `boto3` to download Level-2 Cloud and Moisture Imagery (`.nc` NetCDF files).
    * Saves raw files locally to `data/raw/` (simulating an HDFS ingestion zone).
* **Phase 2: Distributed Processing (Apache Spark)**
    * **Tabular Data:** PySpark DataFrames read the raw HURDAT2 text, filter out metadata headers via Regex, cast numerical columns, and engineer a new `category` feature based on the Saffir-Simpson wind scale.
    * **Imagery Data:** Spark RDDs distribute the NetCDF file paths to parallel worker nodes. Workers utilize `xarray` to open the arrays, extract the `CMI` (Cloud and Moisture Imagery) tensor, and calculate statistical summaries.
    * **Storage:** Both streams are serialized with Snappy compression and written to `data/processed/` as Parquet files. The tabular data utilizes a multi-level partitioning strategy (`/status/category/`).
* **Phase 3: Query & Validation Layer**
    * Initializes a temporary Spark SQL view over the processed Parquet data to execute validation queries, proving data integrity and immediate queryability.

---

## 2. Project Structure
```text
hurricane_pipeline/
├── config/
│   └── settings.yaml              # Central configuration (URLs, S3 buckets, limits)
├── data/
│   ├── raw/                       # Simulated HDFS ingestion zone (.nc, .txt)
│   └── processed/                 # Parquet Data Lakehouse
│       ├── goes_features.parquet/ # Extracted satellite tensors
│       └── hurdat_features.parquet/# Partitioned tabular data (status/category)
├── src/
│   ├── fetch/                     # Data acquisition modules
│   │   ├── fetch_goes.py          # S3 connection and download logic
│   │   └── fetch_hurdat.py        # HTTP request and API retry logic
│   ├── processing/                # Distributed processing modules
│   │   ├── nc_processor.py        # xarray NetCDF extraction via Spark RDDs
│   │   └── spark_processor.py     # HURDAT2 cleaning and feature engineering
│   └── main.py                    # End-to-end pipeline orchestrator
├── .env                           # Environment variables
├── .gitignore                     # Git tracking exclusions
├── requirements.txt               # Python dependencies
└── README.md                      # Project documentation
```

---

## 3. Setup & Dependencies

### Prerequisites
* **Python:** Version 3.11 recommended.
* **Java:** Java 8 or 11 (required for Apache Spark JVM).
* **Windows Users (Hadoop Binaries):** To execute Spark file I/O locally on Windows, you must create a `C:\hadoop\bin` directory and place the compiled `winutils.exe` and `hadoop.dll` files inside it. The pipeline automatically sets the environment variables at runtime.

### Installation
1. Clone this repository to your local machine.
2. Create and activate a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```
3. Install the required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

**Required Libraries (`requirements.txt`):**
* `boto3`
* `requests`
* `python-dotenv`
* `pyyaml`
* `pyspark==3.5.0`
* `netCDF4==1.6.5`
* `xarray==2023.10.0`

---

## 4. Configuration
The pipeline relies on externalized configuration to avoid hardcoded paths.
* **`config/settings.yaml`:** Contains the URLs, S3 bucket names, prefix filters, and maximum download limits.
* **`.env`:** A root-level environment file (optional for public data, but supported for future secure AWS credentials).

Ensure `settings.yaml` is present in the `config/` directory before execution.

---

## 5. Execution Instructions
The pipeline is designed to run end-to-end from a single entry point.

1. Open your terminal.
2. Ensure you are in the root directory of the project.
3. Execute the main orchestrator:
   ```bash
   python src/main.py
   ```
4. Monitor the console logs for processing metrics and the final Spark SQL validation tables. Processed output will be available in `data/processed/`.

---

## 6. Data Dictionary (Final Schema)

### HURDAT2 Track Features (`hurdat_features.parquet`)
| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| `date` | String | Observation date in YYYYMMDD format. |
| `time` | String | Observation time in UTC (HHMM). |
| `latitude` | String | Latitudinal coordinate of the storm center. |
| `longitude` | String | Longitudinal coordinate of the storm center. |
| `max_wind_knots`| Integer | Maximum sustained surface wind speed (in knots). Explicitly cast to integer. Nulls and missing data placeholders (-99) are dropped. |
| `status` | String | NHC storm classification. *See Status Codes below.* (Partition Key 1) |
| `category` | String | Saffir-Simpson category (e.g., Cat_1, Cat_5) engineered from wind speed. (Partition Key 2) |

**Status Codes:** `HU` (Hurricane), `TS` (Tropical Storm), `TD` (Tropical Depression), `EX` (Extratropical Cyclone), `SD` (Subtropical Depression), `SS` (Subtropical Storm), `LO` (Low), `WV` (Tropical Wave), `DB` (Disturbance).

### GOES-16 Imagery Features (`goes_features.parquet`)
| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| `filename` | String | Original NetCDF source file name. |
| `mean_radiance` | Float | The mean value extracted from the Level-2 Cloud and Moisture Imagery (CMI) multi-dimensional tensor. |
| `max_radiance` | Float | The maximum value extracted from the CMI tensor. |