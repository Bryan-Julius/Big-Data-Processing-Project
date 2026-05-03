# M4 Pipeline Validation & Data Quality Report

This document serves as the validation proof for the Hurricane Intensity Data Lakehouse. The validation suite (`src/validate_m4.py`) utilizes Spark SQL to execute predicate pushdown queries against the compressed Parquet data, verifying spatial math, temporal alignment, and overall data integrity.

---

## 1. Data Quality Metrics

The pipeline successfully ingested, cleaned, and partitioned the historical tracking data and satellite imagery. A programmatic null-check verified that no malformed tracking rows bypassed the Phase 2 filtering logic.

* **Total Valid HURDAT2 Tracking Records:** 54,692
* **Total Successfully Cropped GOES Tensors:** 6
* **Data Integrity Check:** 0 Nulls detected in core tracking features.

```text
Checking for Nulls/Missing Data in HURDAT2 Lakehouse:
+----+--------+---------+--------------+
|date|latitude|longitude|max_wind_knots|
+----+--------+---------+--------------+
|   0|       0|        0|             0|
+----+--------+---------+--------------+
```

---

## 2. Sample Validations (Correctness Proof)

### Temporal Join & Spatial Cropping Proof
*Note: Although the pipeline ingested multiple raw NetCDF files (10-minute intervals), the Temporal Join correctly filtered the Data Lakehouse down to only 6 tensors. This proves the strict Inner Join successfully dropped satellite imagery (e.g., 12:10, 12:20) that did not have an exact 6-hour timestamp match (00:00, 06:00, 12:00, 18:00) in the HURDAT2 tracking database.*

The spatial cropping engine (`pyproj`) successfully extracted the arrays. As mathematically expected, the `max_radiance` (the hottest convective center of the storm) is significantly higher than the `mean_radiance` of the overall 10x10 degree bounding box.

```text
Showing successful extraction of mathematically valid radiance features (Mean < Max):
+----------------------------------------------------------------------------+-------------+------------+
|filename                                                                    |mean_radiance|max_radiance|
+----------------------------------------------------------------------------+-------------+------------+
|OR_ABI-L2-CMIPF-M6C01_G16_s20232401200206_e20232401209514_c20232401209592.nc|0.071680054  |0.6828565   |
|OR_ABI-L2-CMIPF-M6C01_G16_s20232401200206_e20232401209514_c20232401209592.nc|0.14397518   |0.7539675   |
|OR_ABI-L2-CMIPF-M6C01_G16_s20232401200206_e20232401209514_c20232401209592.nc|0.13557397   |0.8507928   |
|OR_ABI-L2-CMIPF-M6C02_G16_s20232401200206_e20232401209514_c20232401209566.nc|0.053001408  |0.6904755   |
|OR_ABI-L2-CMIPF-M6C02_G16_s20232401200206_e20232401209514_c20232401209566.nc|0.10563459   |0.77872944  |
|OR_ABI-L2-CMIPF-M6C02_G16_s20232401200206_e20232401209514_c20232401209566.nc|0.08869011   |0.8273008   |
+----------------------------------------------------------------------------+-------------+------------+
```

### Tabular Feature Engineering Proof
The pipeline successfully engineered Saffir-Simpson categories from raw wind knots.

```text
Showing Correct Saffir-Simpson Feature Engineering by Peak Wind:
+------+--------+---------+
|status|category|peak_wind|
+------+--------+---------+
|    HU|   Cat_5|      165|
|    HU|   Cat_4|      135|
|    HU|   Cat_3|      110|
|    EX|   Cat_3|      105|
|    HU|   Cat_2|       95|
|    EX|   Cat_2|       95|
+------+--------+---------+
```

---

## 3. Edge Case Handling

The pipeline was engineered to defensively handle three major Big Data edge cases:

* **Edge Case A: Missing or Corrupted Meteorological Data (-99)**
    * *The Problem:* The NHC HURDAT2 database uses `-99` as a placeholder integer when historical wind speed data was not recorded. If left in the dataset, this would severely skew machine learning averages or result in negative storm categories.
    * *The Solution:* Handled via Spark DataFrame filtering (`.filter(col("max_wind_knots") != -99)`) during Phase 2 ingestion.
* **Edge Case B: Spatial Out-of-Bounds (Storms off the map)**
    * *The Problem:* The GOES-16 satellite only covers a specific hemisphere. If a historical storm in the HURDAT2 database occurred over Europe, the `pyproj` transformation would crash because the Lat/Lon coordinates do not exist on the camera's tensor array.
    * *The Solution:* Handled via a `try/except` block inside the `nc_processor.py` extraction function. If the math fails, the worker node catches the exception gracefully, assigns `-1.0` as the feature values, and allows the pipeline to continue without crashing the Spark cluster.
* **Edge Case C: Temporal Mismatch**
    * *The Problem:* We have 50,000+ historical text records, but only a few downloaded sample satellite images.
    * *The Solution:* Handled via a Strict Inner Join in `spark_processor.py`. The system only passes records to the expensive spatial extraction algorithm if an exact `date` and `time` match exists between the `.nc` file metadata and the text record.

---

## 4. Performance Results

* **Query Engine:** Apache Spark SQL (In-Memory Parquet Predicate Pushdown)
* **Data Format:** Snappy-Compressed Parquet
* **Validation Suite Execution Time:** 18.42 seconds (Includes local JVM cold-start overhead)
* **Analysis:** Because the Data Lakehouse is physically partitioned on disk by `status` and `category`, aggregate Saffir-Simpson queries bypass full table scans, allowing rapid resolution times on the tabular data once the JVM initializes.