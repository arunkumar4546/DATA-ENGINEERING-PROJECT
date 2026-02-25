# End-to-End Data Engineering Project – Medallion Architecture

## Overview

This project implements an end-to-end Data Engineering pipeline using Databricks, PySpark, and Delta Lake based on the Medallion Architecture (Bronze, Silver, Gold).

The pipeline processes transportation data (Drivers, Vehicles, Routes, Trips) and transforms raw data into business-ready analytical tables.

---

## Architecture

**Medallion Architecture Flow**

Raw CSV Files
→ Bronze Layer (Raw Data Ingestion)
→ Silver Layer (Data Cleaning & Deduplication)
→ Gold Layer (Dimension & Fact Tables)
→ Business Analysis / Reporting

Architecture diagram is available in the `architecture/` folder.

---

## Technologies Used

* Databricks
* PySpark
* Delta Lake
* SQL
* Python
* GitHub

---

## Project Layers

### Bronze Layer

* Raw CSV ingestion
* Append-only loading
* Metadata columns:

  * ingestion_date
  * ingestion_time
  * source_file
* Audit logging for load tracking

---

### Silver Layer

* Data cleaning
* Data type casting
* Null handling
* Deduplication using window functions
* Hash key generation for change tracking

---

### Gold Layer

#### Dimension Tables

* dim_driver
* dim_vehicle
* dim_route

#### Fact Table

* fact_trip

Business-ready data for analytics.

---

## Business Use Cases

* Total revenue by route
* Number of trips per driver
* Distance travelled analysis
* Daily trip trends

SQL queries are available in the `sql/` folder.

---

## Project Structure

bronze/   → Bronze ingestion scripts
silver/   → Data cleaning and transformation
gold/     → Dimension and fac
