"""
Kafka → Bronze Streaming Ingestion

Reads events from all Kafka topics in real-time and appends them to the
Bronze Delta Lake layer in MinIO, partitioned by topic. Uses structured
logging for production observability.
"""

import os

from src.shared.logging_config import get_logger
from src.shared.spark_session import get_spark_session

logger = get_logger(__name__)


def run_streaming_ingestion():
    spark = get_spark_session("BronzeStreamingIngestion")
    spark.sparkContext.setLogLevel("WARN")

    KAFKA_BROKER = os.getenv("KAFKA_BROKER_INTERNAL", "kafka:29092")
    TOPICS = ",".join(["orders", "customers", "inventory", "payments"])

    logger.info(
        "Starting streaming ingestion",
        extra={"broker": KAFKA_BROKER, "topics": TOPICS, "layer": "bronze"},
    )

    # Read from Kafka
    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", TOPICS)
        .option("startingOffsets", "earliest")
        .load()
    )

    # We cast value to string and add topic metadata
    parsed_df = df.selectExpr(
        "topic",
        "CAST(key AS STRING)",
        "CAST(value AS STRING)",
        "timestamp",
    )

    # Write stream to Delta Bronze layer (MinIO) partitioned by topic
    query = (
        parsed_df.writeStream.format("delta")
        .outputMode("append")
        .partitionBy("topic")
        .option(
            "checkpointLocation", "s3a://bronze/_checkpoints/kafka_ingestion"
        )
        .start("s3a://bronze/events")
    )

    logger.info("Streaming query started, awaiting termination")
    query.awaitTermination()


if __name__ == "__main__":
    run_streaming_ingestion()
