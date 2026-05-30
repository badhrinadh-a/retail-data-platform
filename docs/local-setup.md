# Local Setup Guide

## Prerequisites
- Docker & Docker Compose
- Python 3.9+
- Make

## Getting Started

1. **Start the Infrastructure**
   ```bash
   make build
   make up
   ```
   This will start Kafka, Spark, MinIO, Postgres, Airflow, and Jenkins.

2. **Wait for Services**
   Wait approximately 2-3 minutes for Airflow to initialize its database and Kafka to start.

3. **Bootstrap Environment**
   ```bash
   ./scripts/setup_minio.sh
   ./scripts/bootstrap.sh
   ```

4. **Accessing UIs**
   - **Airflow**: http://localhost:8085 (admin/admin)
   - **Jenkins**: http://localhost:8082
   - **MinIO**: http://localhost:9001 (minioadmin/minioadmin)
   - **Spark UI**: http://localhost:8080

5. **Stopping The Stack**
   ```bash
   make down
   ```
