"""
Silver → Gold Batch Aggregation

Reads clean order data from the Silver Delta layer, computes revenue and
order count aggregations grouped by status, and upserts into the Gold
summary table using Delta Lake MERGE.

This replaces the previous mode("overwrite") approach which could destroy
existing aggregation data on partial failures.
"""

import sys
import logging
from pyspark.sql.functions import sum as _sum, count, col, current_timestamp
from delta.tables import DeltaTable

from src.shared.spark_session import get_spark_session
from src.shared.logging_config import get_logger

logger = get_logger(__name__)

GOLD_SALES_SUMMARY_PATH = "s3a://gold/daily_sales_summary"


def compute_aggregations(spark):
    """Load Silver orders and compute revenue/count aggregations by status."""
    logger.info("Loading silver orders for aggregation")
    silver_orders = spark.read.format("delta").load("s3a://silver/orders")

    record_count = silver_orders.count()
    logger.info(
        "Silver orders loaded",
        extra={"record_count": record_count, "layer": "silver_to_gold"},
    )

    gold_agg = (
        silver_orders.groupBy("status")
        .agg(
            _sum("amount").alias("total_revenue"),
            count("order_id").alias("order_count"),
        )
        .withColumn("updated_at", current_timestamp())
    )

    return gold_agg


def upsert_to_gold(spark, gold_agg):
    """
    Upsert aggregated metrics into the Gold Delta table using MERGE.

    Merges by status — updates existing status rows with fresh metrics,
    inserts any new status values. Safe against partial failures.
    """
    if DeltaTable.isDeltaTable(spark, GOLD_SALES_SUMMARY_PATH):
        logger.info("Gold table exists — performing MERGE upsert")
        gold_table = DeltaTable.forPath(spark, GOLD_SALES_SUMMARY_PATH)

        (
            gold_table.alias("target")
            .merge(
                gold_agg.alias("source"),
                "target.status = source.status"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        logger.info("MERGE upsert completed successfully", extra={"layer": "gold"})
    else:
        logger.info("Gold table does not exist — creating with initial write")
        (
            gold_agg.write
            .format("delta")
            .mode("overwrite")  # Safe here: first write only, table doesn't exist
            .save(GOLD_SALES_SUMMARY_PATH)
        )
        logger.info("Initial Gold table created", extra={"layer": "gold"})


def run_batch_aggregate():
    """Main entry point for the Silver → Gold aggregation."""
    spark = get_spark_session("GoldBatchAggregate")
    spark.sparkContext.setLogLevel("WARN")

    try:
        gold_agg = compute_aggregations(spark)
        upsert_to_gold(spark, gold_agg)

        # Log summary for debugging (replaces gold_agg.show())
        summary_rows = gold_agg.collect()
        for row in summary_rows:
            logger.info(
                "Gold aggregation result",
                extra={
                    "status": row["status"],
                    "total_revenue": float(row["total_revenue"]),
                    "order_count": int(row["order_count"]),
                },
            )
        logger.info("Gold aggregation completed successfully")
    except Exception:
        logger.exception("Gold aggregation FAILED")
        raise


if __name__ == "__main__":
    run_batch_aggregate()
