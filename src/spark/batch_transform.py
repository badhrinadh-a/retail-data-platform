"""
Bronze → Silver Batch Transform

Reads raw order events from the Bronze Delta layer, parses the JSON payload,
deduplicates using window functions (keeping the latest ingested record per
order_id), and upserts into the Silver orders table using Delta Lake MERGE.

This replaces the previous mode("overwrite") approach which could destroy
existing data on partial failures.
"""

from delta.tables import DeltaTable
from pyspark.sql.functions import col, from_json, row_number
from pyspark.sql.types import DoubleType, LongType, StringType, StructType
from pyspark.sql.window import Window

from src.shared.logging_config import get_logger
from src.shared.spark_session import get_spark_session

logger = get_logger(__name__)


# Reusable schema definition for order events
ORDER_SCHEMA = (
    StructType()
    .add("order_id", StringType())
    .add("customer_id", StringType())
    .add("amount", DoubleType())
    .add("status", StringType())
    .add("created_at", LongType())
)

SILVER_ORDERS_PATH = "s3a://silver/orders"


def parse_bronze_orders(spark):
    """Load and parse order events from the Bronze Delta layer."""
    logger.info("Loading bronze events and filtering for orders topic")
    bronze_df = (
        spark.read.format("delta")
        .load("s3a://bronze/events")
        .filter(col("topic") == "orders")
    )

    parsed_orders = (
        bronze_df.withColumn("data", from_json(col("value"), ORDER_SCHEMA))
        .select("data.*", col("timestamp").alias("ingested_at"))
        .filter(col("order_id").isNotNull())  # Drop malformed records
    )

    record_count = parsed_orders.count()
    logger.info(
        "Parsed bronze orders",
        extra={
            "record_count": record_count,
            "layer": "bronze_to_silver",
        },
    )
    return parsed_orders


def deduplicate_orders(parsed_orders):
    """Deduplicate orders by order_id.

    Keeps the most recently ingested record for each order_id.
    """
    dedup_window = Window.partitionBy("order_id").orderBy(col("ingested_at").desc())

    clean_orders = (
        parsed_orders.withColumn("_row_num", row_number().over(dedup_window))
        .filter(col("_row_num") == 1)
        .drop("_row_num")
    )

    dedup_count = clean_orders.count()
    logger.info(
        "Deduplicated orders",
        extra={
            "dedup_count": dedup_count,
            "layer": "silver",
        },
    )
    return clean_orders


def upsert_to_silver(spark, clean_orders):
    """
    Upsert clean orders into the Silver Delta table using MERGE.

    If the table doesn't exist yet (first run), creates it.
    On subsequent runs, merges by order_id — updating existing records
    and inserting new ones. This is safe against partial failures.
    """
    if DeltaTable.isDeltaTable(spark, SILVER_ORDERS_PATH):
        logger.info("Silver table exists — performing MERGE upsert")
        silver_table = DeltaTable.forPath(spark, SILVER_ORDERS_PATH)

        (
            silver_table.alias("target")
            .merge(
                clean_orders.alias("source"),
                "target.order_id = source.order_id",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        logger.info(
            "MERGE upsert completed successfully",
            extra={"layer": "silver"},
        )
    else:
        logger.info("Silver table does not exist — creating with initial write")
        (
            clean_orders.write.format("delta")
            .mode("overwrite")  # Safe here: first write only, table doesn't exist
            .save(SILVER_ORDERS_PATH)
        )
        logger.info("Initial Silver table created", extra={"layer": "silver"})


def run_batch_transform():
    """Main entry point for the Bronze → Silver transformation."""
    spark = get_spark_session("SilverBatchTransform")
    spark.sparkContext.setLogLevel("WARN")

    try:
        parsed_orders = parse_bronze_orders(spark)
        clean_orders = deduplicate_orders(parsed_orders)
        upsert_to_silver(spark, clean_orders)
        logger.info("Silver transformation completed successfully")
    except Exception:
        logger.exception("Silver transformation FAILED")
        raise


if __name__ == "__main__":
    run_batch_transform()
