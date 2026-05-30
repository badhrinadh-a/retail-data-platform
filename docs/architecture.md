# Architecture Overview

This platform simulates a modern retail data stack running entirely locally using Docker Compose.

## Components

1. **Kafka & Zookeeper**: Provides the event streaming backbone. Real-time events from the fake data generator are pushed to topics.
2. **MinIO (S3-compatible)**: Serves as the Data Lake storage for Bronze, Silver, and Gold delta tables.
3. **Apache Spark (Master/Worker)**: Handles both streaming (Bronze ingestion) and batch (Silver/Gold transformations) workloads using PySpark and Delta Lake.
4. **Apache Airflow**: Orchestrates the batch pipelines scheduling, monitoring, and executing spark-submit commands.
5. **PostgreSQL**: Stores Airflow metadata and acts as a potential serving layer for BI.
6. **Jenkins**: Automates the CI/CD platform validating code, running pytest, and deploying updates to the local docker stack.
