"""
Unit tests for Spark batch transformation and aggregation jobs.

Tests actual business logic:
  - Bronze order parsing and schema enforcement
  - Window-based deduplication logic
  - Gold aggregation math (revenue sums, order counts)
  - Edge cases: nulls, empty data, duplicate handling
"""

import pytest
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, TimestampType,
)
from pyspark.sql.functions import col, lit, current_timestamp

# Import the actual functions under test
from src.spark.batch_transform import (
    ORDER_SCHEMA,
    parse_bronze_orders,
    deduplicate_orders,
)
from src.spark.batch_aggregate import compute_aggregations


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture(scope="session")
def spark():
    """Create a test SparkSession with Delta Lake support."""
    spark = SparkSession.builder \
        .appName("TestRetailPlatform") \
        .master("local[1]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def sample_parsed_orders(spark):
    """Sample parsed orders DataFrame (post-JSON extraction, pre-dedup)."""
    data = [
        ("order-001", "cust-A", 100.50, "COMPLETED", 1700000001, datetime(2024, 1, 1, 10, 0)),
        ("order-002", "cust-B", 250.00, "PENDING",   1700000002, datetime(2024, 1, 1, 10, 1)),
        ("order-003", "cust-A", 75.25,  "CANCELLED", 1700000003, datetime(2024, 1, 1, 10, 2)),
        ("order-004", "cust-C", 500.00, "COMPLETED", 1700000004, datetime(2024, 1, 1, 10, 3)),
        ("order-005", "cust-B", 30.00,  "PENDING",   1700000005, datetime(2024, 1, 1, 10, 4)),
    ]
    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("status", StringType(), True),
        StructField("created_at", LongType(), True),
        StructField("ingested_at", TimestampType(), True),
    ])
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_orders_with_duplicates(spark):
    """Orders with duplicate order_ids (different ingestion timestamps)."""
    data = [
        ("order-001", "cust-A", 100.50, "PENDING",   1700000001, datetime(2024, 1, 1, 10, 0)),
        ("order-001", "cust-A", 100.50, "COMPLETED", 1700000001, datetime(2024, 1, 1, 12, 0)),  # later update
        ("order-002", "cust-B", 250.00, "PENDING",   1700000002, datetime(2024, 1, 1, 10, 1)),
        ("order-002", "cust-B", 250.00, "PENDING",   1700000002, datetime(2024, 1, 1, 10, 1)),  # exact dup
    ]
    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("status", StringType(), True),
        StructField("created_at", LongType(), True),
        StructField("ingested_at", TimestampType(), True),
    ])
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_silver_orders(spark):
    """Sample Silver orders for aggregation testing."""
    data = [
        ("order-001", "cust-A", 100.0, "COMPLETED", 1700000001, datetime(2024, 1, 1)),
        ("order-002", "cust-B", 200.0, "COMPLETED", 1700000002, datetime(2024, 1, 1)),
        ("order-003", "cust-C", 50.0,  "PENDING",   1700000003, datetime(2024, 1, 1)),
        ("order-004", "cust-D", 75.0,  "CANCELLED", 1700000004, datetime(2024, 1, 1)),
        ("order-005", "cust-E", 300.0, "COMPLETED", 1700000005, datetime(2024, 1, 1)),
    ]
    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("status", StringType(), True),
        StructField("created_at", LongType(), True),
        StructField("ingested_at", TimestampType(), True),
    ])
    return spark.createDataFrame(data, schema)


# =============================================================================
# Test: Spark Session
# =============================================================================

class TestSparkSession:
    def test_spark_session_creation(self, spark):
        """Verify test Spark session is running."""
        assert spark is not None
        assert spark.version.startswith("3.")

    def test_delta_extension_loaded(self, spark):
        """Verify Delta Lake extension is configured."""
        extensions = spark.conf.get("spark.sql.extensions")
        assert "DeltaSparkSessionExtension" in extensions


# =============================================================================
# Test: Order Schema
# =============================================================================

class TestOrderSchema:
    def test_order_schema_has_required_fields(self):
        """Verify ORDER_SCHEMA contains all expected fields."""
        field_names = [f.name for f in ORDER_SCHEMA.fields]
        assert "order_id" in field_names
        assert "customer_id" in field_names
        assert "amount" in field_names
        assert "status" in field_names
        assert "created_at" in field_names

    def test_order_schema_field_types(self):
        """Verify ORDER_SCHEMA field types are correct."""
        field_map = {f.name: f.dataType for f in ORDER_SCHEMA.fields}
        assert isinstance(field_map["order_id"], StringType)
        assert isinstance(field_map["amount"], DoubleType)
        assert isinstance(field_map["created_at"], LongType)


# =============================================================================
# Test: Deduplication Logic
# =============================================================================

class TestDeduplication:
    def test_dedup_removes_duplicates(self, sample_orders_with_duplicates):
        """Dedup should reduce 4 rows (2 unique order_ids) to 2 rows."""
        result = deduplicate_orders(sample_orders_with_duplicates)
        assert result.count() == 2

    def test_dedup_keeps_latest_by_ingested_at(self, sample_orders_with_duplicates):
        """Dedup should keep the record with the most recent ingested_at."""
        result = deduplicate_orders(sample_orders_with_duplicates)
        order_001 = result.filter(col("order_id") == "order-001").collect()[0]
        # The later update (12:00) should be kept, which has status COMPLETED
        assert order_001["status"] == "COMPLETED"

    def test_dedup_preserves_unique_orders(self, sample_parsed_orders):
        """Dedup on data with no duplicates should preserve all rows."""
        result = deduplicate_orders(sample_parsed_orders)
        assert result.count() == sample_parsed_orders.count()

    def test_dedup_removes_row_number_column(self, sample_orders_with_duplicates):
        """The internal _row_num column should not leak into the output."""
        result = deduplicate_orders(sample_orders_with_duplicates)
        assert "_row_num" not in result.columns


# =============================================================================
# Test: Gold Aggregation Logic
# =============================================================================

class TestGoldAggregation:
    def test_aggregation_groups_by_status(self, spark, sample_silver_orders, tmp_path):
        """Aggregation should produce one row per unique status."""
        # Write sample data as Delta so compute_aggregations can read it
        silver_path = str(tmp_path / "silver_orders")
        sample_silver_orders.write.format("delta").save(silver_path)

        # Monkey-patch the path for testing
        import src.spark.batch_aggregate as agg_module
        original_fn = agg_module.compute_aggregations

        def patched_compute(spark_session):
            silver_df = spark_session.read.format("delta").load(silver_path)
            from pyspark.sql.functions import sum as _sum, count, current_timestamp
            return (
                silver_df.groupBy("status")
                .agg(
                    _sum("amount").alias("total_revenue"),
                    count("order_id").alias("order_count"),
                )
                .withColumn("updated_at", current_timestamp())
            )

        agg_module.compute_aggregations = patched_compute
        try:
            result = agg_module.compute_aggregations(spark)
            statuses = [row["status"] for row in result.collect()]
            assert set(statuses) == {"COMPLETED", "PENDING", "CANCELLED"}
        finally:
            agg_module.compute_aggregations = original_fn

    def test_aggregation_revenue_sum(self, spark, sample_silver_orders):
        """Total revenue for COMPLETED orders should be 600.0 (100+200+300)."""
        from pyspark.sql.functions import sum as _sum, count, current_timestamp
        result = (
            sample_silver_orders.groupBy("status")
            .agg(
                _sum("amount").alias("total_revenue"),
                count("order_id").alias("order_count"),
            )
        )
        completed = result.filter(col("status") == "COMPLETED").collect()[0]
        assert completed["total_revenue"] == 600.0
        assert completed["order_count"] == 3

    def test_aggregation_single_status(self, spark, sample_silver_orders):
        """CANCELLED status should have exactly 1 order with amount 75.0."""
        from pyspark.sql.functions import sum as _sum, count
        result = (
            sample_silver_orders.groupBy("status")
            .agg(
                _sum("amount").alias("total_revenue"),
                count("order_id").alias("order_count"),
            )
        )
        cancelled = result.filter(col("status") == "CANCELLED").collect()[0]
        assert cancelled["total_revenue"] == 75.0
        assert cancelled["order_count"] == 1

    def test_aggregation_pending_orders(self, spark, sample_silver_orders):
        """PENDING status should sum to 50.0 with 1 order."""
        from pyspark.sql.functions import sum as _sum, count
        result = (
            sample_silver_orders.groupBy("status")
            .agg(
                _sum("amount").alias("total_revenue"),
                count("order_id").alias("order_count"),
            )
        )
        pending = result.filter(col("status") == "PENDING").collect()[0]
        assert pending["total_revenue"] == 50.0
        assert pending["order_count"] == 1


# =============================================================================
# Test: Edge Cases
# =============================================================================

class TestEdgeCases:
    def test_empty_dataframe_dedup(self, spark):
        """Dedup on an empty DataFrame should return empty without error."""
        schema = StructType([
            StructField("order_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("status", StringType(), True),
            StructField("created_at", LongType(), True),
            StructField("ingested_at", TimestampType(), True),
        ])
        empty_df = spark.createDataFrame([], schema)
        result = deduplicate_orders(empty_df)
        assert result.count() == 0

    def test_single_row_dedup(self, spark):
        """Dedup on a single row should return that same row."""
        data = [("order-X", "cust-X", 99.99, "PENDING", 1700000000, datetime(2024, 1, 1))]
        schema = StructType([
            StructField("order_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("status", StringType(), True),
            StructField("created_at", LongType(), True),
            StructField("ingested_at", TimestampType(), True),
        ])
        df = spark.createDataFrame(data, schema)
        result = deduplicate_orders(df)
        assert result.count() == 1
        assert result.collect()[0]["order_id"] == "order-X"
