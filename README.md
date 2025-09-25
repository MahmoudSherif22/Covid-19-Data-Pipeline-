# Covid-19-Data-Pipeline-
This project implements an ETL data pipeline for COVID-19 case data, orchestrated with Apache Airflow and containerized using Docker. The pipeline ingests raw CSV files, processes them with Apache Spark, stages them in PostgreSQL, and transforms them into a Star Schema Data Warehouse for analytics and reporting with Power BI.
📌 Project Workflow
<img width="600" height="400" alt="Screenshot (1045)" src="https://github.com/user-attachments/assets/d554470e-539b-45de-8984-8ce0cee9d01f" />

## Data Ingestion

Daily COVID-19 CSV files ingested using Spark.

## Staging Area

Raw data stored in a PostgreSQL staging database for cleansing and validation.

## Transformation & Star Schema

Spark transforms staging data into a Star Schema with fact and dimension tables (e.g., date_dim, loc_dim, fact_covid).

Handles Slowly Changing Dimensions (SCD Type 2) for location data.

## Data Warehouse

Transformed data loaded into a PostgreSQL Data Warehouse.

Supports efficient queries and BI dashboards.

## Orchestration

Workflow automated using Apache Airflow (DAGs for ingestion, staging, transformation, and loading).

## Visualization

Final Data Warehouse consumed in Power BI for interactive dashboards.

## Tech Stack

Apache Spark → Data processing & transformations

Apache Airflow → Orchestration & scheduling

PostgreSQL → Staging & Data Warehouse

Docker → Containerization

Power BI → Visualization

## Covid 19 Data 
https://www.kaggle.com/datasets?search=covid
