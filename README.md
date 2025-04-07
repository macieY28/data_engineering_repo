# data_engineering_repo

# Data Engineering Project: Medallion Architecture

## Overview

This project implements a data engineering pipeline using the **Medallion Architecture**, designed and developed on Databricks. The pipeline processes data from two APIs: **Open-Meteo** and **OpenSky API**, and organizes it into three layers: **Bronze**, **Silver**, and **Gold**. The entire workflow leverages the Delta Lake format for efficient data storage and processing.

---

## Table of Contents

1. [Project Architecture](#project-architecture)
2. [Data Sources](#data-sources)
3. [Pipeline Layers](#pipeline-layers)
   - [Bronze Layer](#bronze-layer)
   - [Silver Layer](#silver-layer)
   - [Gold Layer](#gold-layer)
4. [Delta Lake Advantages](#delta-lake-advantages)
5. [Configuration Files](#configuration-files)
6. [Usage](#usage)
7. [Future Enhancements](#future-enhancements)

---

## Project Architecture

The pipeline is structured as follows:

1. **Bronze Layer**: Raw data ingestion from APIs.
2. **Silver Layer**: Data cleaning, deduplication, and formatting.
3. **Gold Layer**: Aggregated and transformed data stored as Hive Metastore tables and views for analysis and reporting.

---

## Data Sources

### Open-Meteo API

- Provides daily and hourly weather data for specific locations.

### OpenSky API

- Provides flight departure and arrival data, including airport codes and timestamps.

---

## Pipeline Layers

### Bronze Layer

- **Purpose**: Ingest raw data from APIs.
- **Details**:
  - Four distinct streams of data:
    1. Hourly weather data
    2. Daily weather data
    3. Flight departure data
    4. Flight arrival data
  - Configures API endpoints and retrieves data.
  - Stores data in Delta Lake under `dbfs:/data_engineering_repo/cloudproject/data/bronze`.

### Silver Layer

- **Purpose**: Clean and standardize the data.
- **Transformations**:
  - Deduplication of records.
  - Removal of null values.
  - Formatting numeric columns to two decimal places.
- **Storage**: Processed data is saved in Delta Lake under `dbfs:/data_engineering_repo/cloudproject/data/silver`.

### Gold Layer

- **Purpose**: Provide curated and aggregated datasets for analytics.
- **Details**:
  - Hive Metastore tables are created for structured querying.
  - Views are built for dashboarding and reporting.
  - Example analysis: Impact of weather conditions on airport operations.
- **Storage**: Final datasets are stored in Delta Lake under `dbfs:/data_engineering_repo/cloudproject/data/gold`.

---

## Delta Lake Advantages

- **ACID Transactions**: Ensures data integrity across multiple operations.
- **Schema Enforcement**: Guarantees consistency in data structure.
- **Time Travel**: Enables querying previous versions of data.
- **Efficient Storage**: Optimized for both batch and streaming workloads.

---

## Configuration Files

The project uses two configuration files:

- **Locations Config**: Contains coordinates for specific locations.
- **Airports Config**: Contains ICAO codes for airports.

These files are loaded during the Bronze Layer's API setup to parameterize data retrieval.

---

## Usage

### Prerequisites

- Databricks workspace with Delta Lake enabled.
- Access to Open-Meteo and OpenSky APIs are free.
- Properly formatted configuration files.

### Steps to Run

1. Clone the repository to your Databricks workspace.
2. Run the Bronze Layer notebooks to ingest raw data.
3. Execute the Silver Layer notebooks to process and clean the data.
4. Generate curated datasets by running the Gold Layer notebooks.
5. Use Hive Metastore views for analytics and reporting.

---

## Future Enhancements

- Integration of additional APIs or data sources.
- Advanced analytics using machine learning models.
- Real-time dashboarding for operational insights.

---

This project is designed to provide a robust framework for data engineering workflows, emphasizing modularity and scalability. It could be also implemented on other spark-based data platform like Microsoft Fabrics or Azure Synapse Analytics. Contributions and suggestions are welcome!


*About author:
data engineer with 3 years of experience in Big Data projects
you can contact with me by mail: maciekcz95@wp.pl*

