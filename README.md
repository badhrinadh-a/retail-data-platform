# Retail Data Platform

An enterprise-ready, locally-deployable data engineering platform designed to simulate a modern high-scale retail environment (like Metro Cash & Carry).

## Overview
This monorepo contains everything needed to spin up a fully functioning data infrastructure stack via Docker Compose, complete with real-time ingestion, bash processing, orchestration, and continuous integration.

**Tech Stack**:
- Apache Spark (PySpark) & Delta Lake
- Apache Kafka
- Apache Airflow
- MinIO (Local S3)
- PostgreSQL
- Jenkins

## Repository Structure
- `airflow/` - DAGs and custom operators
- `src/` - Core Python code including Kafka publishers and PySpark jobs
- `tests/` - Unit tests for the data application
- `docker/` - Custom image configurations
- `jenkins/` - CI/CD pipeline definitions
- `scripts/` - Shell helpers for initializing the environment
- `docs/` - Extensive documentation

## Getting Started

Check out [docs/local-setup.md](docs/local-setup.md) for full instructions.

Quick start:
```bash
make build
make up
./scripts/setup_minio.sh
./scripts/bootstrap.sh
python src/kafka/producer.py
```

## Architecture & Flows
- [Architecture Overview](docs/architecture.md)
- [Data Flow (Medallion)](docs/data-flow.md)
- [CI/CD Flow](docs/ci-cd-flow.md)
- [Branching Strategy](docs/branch-strategy.md)
