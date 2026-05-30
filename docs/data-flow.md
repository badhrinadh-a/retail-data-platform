# Data Flow

The Retail Data Platform implements the classic Medallion architecture using Delta Lake.

## 1. Data Generation
`src/kafka/producer.py` continuously produces randomized JSON events containing orders, customers, inventory updates, and payments.

## 2. Bronze Zone (Raw)
`src/spark/streaming_ingestion.py` consumes the Kafka topics and appends them immediately to the MinIO `bronze` bucket. No schema enforcement or transformation is applied yet, storing the raw payload.

## 3. Silver Zone (Refined)
`src/spark/batch_transform.py`, orchestrated by Airflow via `batch_jobs_dag`, reads the bronze data, extracts the JSON schema, deduplicates records, and enforces data types before writing to the `silver` bucket.

## 4. Gold Zone (Aggregated)
`src/spark/batch_aggregate.py` reads clean silver data to produce daily reporting tables, such as total revenue and order status summaries, written to the `gold` bucket.
