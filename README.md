# Scalable Atmospheric Data Pipeline for Global Tropical Hurricane Intensity Forecasting

## Project Overview
This project implements an end-to-end Big Data pipeline designed to acquire, process, align, and analyze multi-modal meteorological data. It integrates unstructured NetCDF satellite imagery (NOAA GOES-16) with structured tabular tracking data (NHC HURDAT2) into a unified, distributed Data Lakehouse architecture utilizing Apache Spark and Parquet.

A core feature of this architecture is its **spatiotemporal alignment engine**, which utilizes PySpark for strict temporal inner-joins and `pyproj` for dynamic spatial tensor cropping, vastly reducing memory overhead and optimizing feature extraction.

---

## 1. Pipeline Architecture
The pipeline executes sequentially without manual intervention:

* **Phase 1: Data Acquisition**
    * Connects to the National Hurricane Center (NHC) HTTP server to download historical HURDAT2 text data.
    * Authenticates anonymously with NOAA's public Amazon S3 bucket (`noaa-goes16`) via `boto3` to download Level-2 Cloud and Moisture Imagery (`.nc` NetCDF files).
* **Phase 2: Temporal Alignment (Spark DataFrames)**
    * Parses historical hurricane tracking data using Native JVM DataFrames.
    * Extracts metadata from satellite NetCDF filenames (Julian dates/times) and executes a strict Spark Inner Join against the HURDAT2 text records. This guarantees that expensive spatial math is only performed on satellite imagery that has a perfectly matching 6-hour tracking interval (00:00, 06:00, 12:00, 18:00 UTC).
* **Phase 3: Spatial Cropping & Distributed Processing (Spark RDDs)**
    * Spark distributes the validated file paths and hurricane coordinates to parallel worker nodes.
    * Workers utilize `pyproj` to translate standard Earth Latitude/Longitude into GOES-16 Geostationary camera radians.
    * `xarray` crops the massive 10,848 x 10,848 satellite array down to a highly targeted 10x10 degree bounding box directly over the storm's eye, extracting statistical features (mean and max radiance) while dropping 99% of useless ocean/land data.
* **Phase 4: The Parquet Data Lakehouse**
    * Both streams are serialized with Snappy compression and written to `data/processed/`.
    * The tabular data utilizes a multi-level distributed partitioning strategy (`/status/category/`).

---

## 2. Project Structure
```text
hurricane_pipeline/
├── config/
│   └── settings.yaml              # Central configuration (URLs, S3 buckets, limits)
├── data/
│   ├── raw/                       # Simulated HDFS ingestion zone (.nc, .txt)
│   └── processed/                 # Parquet Data Lakehouse
│       ├── goes_features.parquet/ # Extracted bounding-box satellite tensors
│       └── hurdat_features.parquet/# Partitioned tabular data (status/category)
├── docs/
│   └── validation.md              # Data quality metrics and edge-case documentation
├── src/
│   ├── fetch/                     # Data acquisition modules
│   │   ├── fetch_goes.py          # S3 connection and download logic
│   │   └── fetch_hurdat.py        # HTTP request and API retry logic
│   ├── processing/                # Distributed processing modules
│   │   ├── nc_processor.py        # xarray spatial cropping and pyproj math
│   │   └── spark_processor.py     # Temporal join and Spark orchestration
│   ├── main.py                    # End-to-end pipeline orchestrator
│   └── validate_m4.py             # Spark SQL Data Quality validation script
├── .gitignore                     # Git tracking exclusions
├── environment.yml                # Python/Conda environment dependencies
└── README.md                      # Project documentation
```

---

## 3. Setup & Dependencies

### Prerequisites
* **Python:** Version 3.11 recommended.
* **Conda** (Required): Miniconda or Anaconda must be installed to handle complex C-extensions for data processing libraries.
* **Java:** Java 8 or 11 (required for Apache Spark JVM).
* **Windows Users (Hadoop Binaries):** To execute Spark file I/O locally on Windows, you must create a `C:\hadoop\bin` directory and place the compiled `winutils.exe` and `hadoop.dll` files inside it. The pipeline automatically sets the environment variables at runtime.

### Installation
1. Clone this repository to your local machine.
2. Create and activate a conda environment in terminal (If Windows ensure Conda is initialized in powershell)
   ```bash
   conda env create -f environment.yml
   ```
3. Install the required dependencies:
   ```bash
   conda activate big-data-env
   ```

**Required Libraries (`environment.yml`):**
- python=3.11
- requests=2.31.0
- boto3=1.34.0
- pandas=2.2.0
- pyyaml=6.0.1
- python-dotenv=1.0.1
- pyspark=3.5.0
- netcdf4=1.6.5
- xarray=2023.10.0
- pyproj

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
3. Ensure Conda is active
4. ```bash
   conda activate big-data-env
   ```
5. Execute the main orchestrator to ingest data, perform the temporal join, crop the tensors, and build the Lakehouse:
   ```bash
   python src/main.py
   ```
6. Monitor the console logs for processing metrics and the final Spark SQL validation tables. Processed output will be available in `data/processed/`.



7. Once the pipeline completes, execute the validation script to spin up a Spark SQL engine, verify data quality, and prove the spatial math succeeded:
   ```bash
   python src/validate.py
   ```
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
| `mean_radiance` | Float | The mean radiance extracted strictly from a 10x10 degree bounding box around the storm's center. |
| `max_radiance` | Float | The maximum radiance extracted from the cropped bounding box. |