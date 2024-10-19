# Medallion Fitness (Data Lakehouse Project)

## Overview

This project implements a robust, scalable data engineering pipeline for processing fitness-related data using a medallion architecture in Databricks. It showcases best practices in data engineering, including stream processing, data quality checks, and the creation of analytics-ready datasets.

## Architecture

The pipeline follows a three-layer medallion architecture:

1. **Bronze Layer**: Raw data ingestion from various sources (user registrations, gym logins, heart rate data, etc.)
2. **Silver Layer**: Data cleansing, transformation, and enrichment
3. **Gold Layer**: Creation of aggregated, analysis-ready datasets for data consumers

The project utilizes Apache Spark for distributed processing and Delta Lake for reliable data storage and ACID transactions.

## Requirements

1. Design and implement a lakehouse platform using the medallion architecture.
2. Collect and ingest data from source systems.
3. Prepare the following low-latency analytics-ready datasets for down-stream data consumers
   a. Workout BPM summary
   b. Gym summary
4. Have multiple environments.
5. Decouple data ingestion from data processing.
6. Support both batch and streaming workflows.
7. Automated Integration testing.
8. Automated deployment pipelines for test and prod environments.
9. Centralized metadata management and access control.
   

## Features

- Stream processing using Spark Structured Streaming
- Data quality checks and validation at each layer
- Scalable design to handle large volumes of fitness data
- Real-time processing of user activities and biometric data
- Creation of aggregated views for analytics and reporting

## Technology Stack

- Apache Spark
- Delta Lake
- Databricks
- Azure Data Lake Storage Gen2
- Python

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contact

For any questions or feedback, please contact Dharmik Naik at dharmiknaik@hotmail.com
