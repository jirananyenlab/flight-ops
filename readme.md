# Flight Data Engineering Pipeline

## Overview

This project demonstrates an end-to-end Data Engineering pipeline built using the Medallion Architecture (Bronze, Silver, Gold). The pipeline extracts real-time flight data from the OpenSky Network API, processes and transforms the data through multiple layers, and loads aggregated business-ready data into Snowflake for analytics.

The workflow is orchestrated and scheduled using Apache Airflow, enabling automated data ingestion and transformation at regular intervals.

---

## Architecture

```text
                +------------------+
                |  OpenSky API     |
                +--------+---------+
                         |
                         v
                +------------------+
                | Bronze Layer     |
                | Raw Data Storage |
                +--------+---------+
                         |
                         v
                +------------------+
                | Silver Layer     |
                | Data Cleaning &  |
                | Transformation   |
                +--------+---------+
                         |
                         v
                +------------------+
                | Gold Layer       |
                | KPI Aggregation  |
                +--------+---------+
                         |
                         v
                +------------------+
                | Snowflake DW     |
                +------------------+
```

---

## Technologies Used

* Python
* Apache Airflow
* Snowflake
* Pandas
* OpenSky Network API
* Docker
* SQL

---

## Data Pipeline

### Bronze Layer

The Bronze layer stores raw flight data retrieved directly from the OpenSky API without modifications.

Responsibilities:

* Extract flight data from OpenSky API
* Store raw datasets for traceability and reprocessing

---

### Silver Layer

The Silver layer cleans and standardizes the raw data.

Responsibilities:

* Select relevant columns required for analytics

Output:

* Clean and structured flight dataset ready for analytics

---

### Gold Layer

The Gold layer generates business-ready aggregated datasets.

Responsibilities:

* Aggregate flight statistics by country
* Calculate average velocity
* Count total flights
* Analyze on-ground 

Example KPIs:

* Total Flights
* Average Velocity
* Aircraft On Ground
* Flight Count by Country

---

## Airflow Workflow

The pipeline is orchestrated using Apache Airflow.

DAG Tasks:

```text
bronze_ingest
      ↓
silver_transform
      ↓
gold_aggregate
      ↓
load_to_snowflake
```

Features:

* Scheduled execution
* Task dependency management
* Retry handling
* Logging and monitoring
* XCom communication between tasks

---

## Snowflake Data Warehouse

The Gold layer output is loaded into Snowflake for reporting and analytical purposes.

Example table:

```sql
FLIGHT_KPI
```

Columns:

| Column         | Description                  |
| -------------- | ---------------------------- |
| WINDOW_START   | Aggregation timestamp        |
| ORIGIN_COUNTRY | Country name                 |
| TOTAL_FLIGHTS  | Number of flights            |
| AVG_VELOCITY   | Average flight velocity      |
| ON_GROUND      | Number of aircraft on ground |

---

## Project Structure

```text
project/
│
├── dags/
│   └── flight_pipeline.py
│
├── data/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── secrets/
│
├── requirements.txt
├── docker-compose.yml
└── README.md
```

---

## Key Learning Outcomes

* Building ETL pipelines using Python
* Implementing Medallion Architecture
* Workflow orchestration with Apache Airflow
* Data warehousing with Snowflake
* Data transformation using Pandas
* Automating scheduled data pipelines
* Managing data quality across multiple processing layers

---