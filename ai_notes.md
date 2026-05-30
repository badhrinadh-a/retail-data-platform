This project is a locally-deployable, enterprise-ready Retail Data Platform designed to simulate modern, high-scale retail environments. By leveraging industry-standard tools, it replicates an end-to-end production data stack running on your local machine using Docker.

1. Architectural Blueprint
The project implements a classic Medallion Architecture using Delta Lake over S3-compatible object storage (MinIO), orchestrated by Apache Airflow, powered by Apache Spark, and backed by Apache Kafka for streaming events.

mermaid
graph TD
    %% Event Generation & Streaming
    subgraph Data Generation & Streaming
        Gen[Fake Data Producer <br/>src/kafka/producer.py] -->|Produce JSON Events| Kafka[Kafka Broker <br/>Topics: orders, customers, inventory, payments]
    end
    %% Ingestion & Storage Layers
    subgraph S3 Storage MinIO
        Bronze[(Bronze Bucket <br/>Raw Append-Only Delta)]
        Silver[(Silver Bucket <br/>Cleaned & Deduplicated Delta)]
        Gold[(Gold Bucket <br/>Aggregated Sales Delta)]
    end
    %% Processing & Compute
    subgraph Compute Engine
        SparkStream[Spark Streaming Ingestion]
        SparkBatch1[Spark Batch Transform]
        SparkBatch2[Spark Batch Aggregate]
    end
    %% Data Flows
    Kafka -->|Spark Structured Streaming| SparkStream
    SparkStream -->|Append raw partitioned by topic| Bronze
    
    Bronze -->|PySpark batch load & clean| SparkBatch1
    SparkBatch1 -->|Write clean orders| Silver
    
    Silver -->|PySpark batch aggregate| SparkBatch2
    SparkBatch2 -->|Write daily sales summary| Gold
    %% Orchestration
    subgraph Orchestration & CI/CD
        Airflow[Apache Airflow] -.->|Orchestrates| SparkStream
        Airflow -.->|Orchestrates| SparkBatch1
        Airflow -.->|Orchestrates| SparkBatch2
        Jenkins[Jenkins CI/CD] -.->|Lint, Test & Auto Deploy| docker-compose[Docker Stack]
    end
    classDef storage fill:#1e293b,stroke:#0f172a,stroke-width:2px,color:#fff;
    classDef compute fill:#1d4ed8,stroke:#1e40af,stroke-width:2px,color:#fff;
    classDef streaming fill:#b45309,stroke:#92400e,stroke-width:2px,color:#fff;
    classDef orchestrator fill:#047857,stroke:#065f46,stroke-width:2px,color:#fff;
    class Bronze,Silver,Gold storage;
    class SparkStream,SparkBatch1,SparkBatch2 compute;
    class Gen,Kafka streaming;
    class Airflow,Jenkins orchestrator;
2. What We Can Achieve & Learn Using This Project
Mastering the Medallion Architecture:
Bronze Layer (Raw): Append raw Kafka JSON payloads directly into MinIO S3 partitioning by topic, preserving the audit trail without modification.
Silver Layer (Cleaned & Refined): Parse raw JSON strings using schemas, deduplicate transactional entries (e.g. keeping the latest state of an order), enforce proper types, and write structured Delta files.
Gold Layer (Aggregated/Business): Group clean tables by core metrics (e.g. Total Revenue and Order Count per order status) to create ultra-fast reporting datasets ready for BI consumption.
Real-time Event Streaming: Experience PySpark Structured Streaming in action as it consumes real-time telemetry from multiple Kafka topics (orders, customers, inventory, payments) with robust checkpointing to guarantee once-and-only-once semantics.
ACID Transactions on Data Lakes: Delta Lake brings transactions to local S3 storage. You can perform time travel (querying older snapshots), ACID updates, and schema enforcement/evolution without setting up complex database clusters.
Production-Grade Orchestration: Utilize Airflow DAGs to coordinate workflows. Learn how to sequence stream activations, run data-cleaning pipelines, trigger mock Data Quality checks, and schedule gold reporting aggregates.
Enterprise CI/CD Pipelines: The containerized Jenkins server runs pipelines that automatically fetch your code, run formatting (black/isort), check code quality (flake8), execute unit tests (pytest), rebuild custom images, and safely hot-reload your local infrastructure.
3. How to Start the Project (Step-by-Step Guide)
You can launch the entire ecosystem locally using simple Terminal commands.

Step A: Boot the Infrastructure
First, build and run all services in the background using the provided Makefile helpers:

bash
# Build custom Airflow and Jenkins Docker images
make build
# Launch the docker-compose stack (Postgres, Kafka, Spark, MinIO, Airflow, Jenkins)
make up
Step B: Bootstrap Storage & Topics
Wait 2–3 minutes for all databases, Kafka, and Airflow systems to fully initialize. Then, execute the bootstrap scripts:

Initialize MinIO Buckets: Setup the MinIO command-line client (mc) and configure the bronze, silver, and gold S3 buckets:
bash
./scripts/setup_minio.sh
Create Kafka Topics: Auto-generate topics for our main telemetry:
bash
./scripts/bootstrap.sh
Step C: Start the Fake Data Generator
Kickstart the real-time simulation engine to stream telemetry into Kafka:

bash
python src/kafka/producer.py
(This starts continuously producing random, enterprise-like orders, inventory updates, and payments into the Kafka topics. Keep this running in its own terminal window or background).

Step D: Run and Monitor the Pipelines
Open your web browser to interact with the different control rooms of your platform:

Interface	URL	Credentials	Purpose
Apache Airflow	http://localhost:8085	admin / admin	Trigger the ingestion and batch-transformation DAGs.
Apache Spark	http://localhost:8080	No login needed	Monitor your Spark Master/Worker health and active job executions.
MinIO Storage	http://localhost:9001	minioadmin / minioadmin	Browse raw, cleaned, and aggregated Delta tables in S3.
Jenkins CI/CD	http://localhost:8082	No login needed	Review your automated testing, formatting, and deployment runs.
Go to the Airflow UI and turn on/unpause the streaming_ingestion DAG to ingest real-time Kafka traffic into Delta Bronze.
Trigger the batch_processing_pipeline DAG manually or let it run daily to transition the Bronze telemetry into Silver structures, execute quality gates, and output Gold reports!