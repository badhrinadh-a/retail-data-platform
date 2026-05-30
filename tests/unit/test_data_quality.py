"""
Unit tests for the data quality validation module.

Tests each individual check function and the composite validation suites
against controlled DataFrames with known data, including edge cases.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, IntegerType,
)

from src.quality.data_quality import (
    check_not_empty,
    check_no_nulls,
    check_no_duplicates,
    check_value_range,
    check_expected_columns,
    check_allowed_values,
    check_row_count_consistency,
    QualityReport,
    QualityCheckResult,
)


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture(scope="session")
def spark():
    """Create a test SparkSession."""
    spark = SparkSession.builder \
        .appName("TestDataQuality") \
        .master("local[1]") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def orders_df(spark):
    """Clean orders DataFrame — no quality issues."""
    data = [
        ("order-001", "cust-A", 100.0, "COMPLETED", 1700000001),
        ("order-002", "cust-B", 250.0, "PENDING",   1700000002),
        ("order-003", "cust-C", 75.0,  "CANCELLED", 1700000003),
    ]
    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("status", StringType(), True),
        StructField("created_at", LongType(), True),
    ])
    return spark.createDataFrame(data, schema)


@pytest.fixture
def orders_with_nulls(spark):
    """Orders DataFrame with NULL values in critical columns."""
    data = [
        ("order-001", "cust-A", 100.0, "COMPLETED", 1700000001),
        (None,        "cust-B", 250.0, "PENDING",   1700000002),
        ("order-003", None,     75.0,  "CANCELLED", 1700000003),
    ]
    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("status", StringType(), True),
        StructField("created_at", LongType(), True),
    ])
    return spark.createDataFrame(data, schema)


@pytest.fixture
def orders_with_duplicates(spark):
    """Orders DataFrame with duplicate order_ids."""
    data = [
        ("order-001", "cust-A", 100.0, "COMPLETED", 1700000001),
        ("order-001", "cust-A", 100.0, "COMPLETED", 1700000001),  # duplicate
        ("order-002", "cust-B", 250.0, "PENDING",   1700000002),
    ]
    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("status", StringType(), True),
        StructField("created_at", LongType(), True),
    ])
    return spark.createDataFrame(data, schema)


@pytest.fixture
def empty_df(spark):
    """Empty DataFrame with order schema."""
    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("amount", DoubleType(), True),
    ])
    return spark.createDataFrame([], schema)


# =============================================================================
# Test: check_not_empty
# =============================================================================

class TestCheckNotEmpty:
    def test_nonempty_table_passes(self, orders_df):
        result = check_not_empty(orders_df, "orders")
        assert result.passed is True
        assert result.metric_value == 3.0

    def test_empty_table_fails(self, empty_df):
        result = check_not_empty(empty_df, "orders")
        assert result.passed is False
        assert result.metric_value == 0.0

    def test_min_rows_threshold(self, orders_df):
        result = check_not_empty(orders_df, "orders", min_rows=10)
        assert result.passed is False  # only 3 rows, need 10

    def test_exact_threshold(self, orders_df):
        result = check_not_empty(orders_df, "orders", min_rows=3)
        assert result.passed is True


# =============================================================================
# Test: check_no_nulls
# =============================================================================

class TestCheckNoNulls:
    def test_clean_data_passes(self, orders_df):
        results = check_no_nulls(orders_df, "orders", ["order_id", "customer_id"])
        assert all(r.passed for r in results)

    def test_nulls_detected(self, orders_with_nulls):
        results = check_no_nulls(orders_with_nulls, "orders", ["order_id", "customer_id"])
        result_map = {r.check_name: r for r in results}
        assert result_map["orders_order_id_no_nulls"].passed is False
        assert result_map["orders_customer_id_no_nulls"].passed is False

    def test_missing_column_fails(self, orders_df):
        results = check_no_nulls(orders_df, "orders", ["nonexistent_column"])
        assert len(results) == 1
        assert results[0].passed is False
        assert "does not exist" in results[0].message

    def test_null_percentage_calculated(self, orders_with_nulls):
        results = check_no_nulls(orders_with_nulls, "orders", ["order_id"])
        # 1 null out of 3 rows = 33.33%
        assert results[0].metric_value == pytest.approx(33.33, abs=0.1)


# =============================================================================
# Test: check_no_duplicates
# =============================================================================

class TestCheckNoDuplicates:
    def test_unique_data_passes(self, orders_df):
        result = check_no_duplicates(orders_df, "orders", ["order_id"])
        assert result.passed is True
        assert result.metric_value == 0.0

    def test_duplicates_detected(self, orders_with_duplicates):
        result = check_no_duplicates(orders_with_duplicates, "orders", ["order_id"])
        assert result.passed is False
        assert result.metric_value == 1.0  # 3 rows - 2 distinct = 1 dup


# =============================================================================
# Test: check_value_range
# =============================================================================

class TestCheckValueRange:
    def test_values_in_range_passes(self, orders_df):
        result = check_value_range(orders_df, "orders", "amount", min_value=0.0)
        assert result.passed is True

    def test_negative_values_fail(self, spark):
        data = [("order-001", -50.0), ("order-002", 100.0)]
        schema = StructType([
            StructField("order_id", StringType(), True),
            StructField("amount", DoubleType(), True),
        ])
        df = spark.createDataFrame(data, schema)
        result = check_value_range(df, "orders", "amount", min_value=0.0)
        assert result.passed is False
        assert result.metric_value == 1.0


# =============================================================================
# Test: check_expected_columns
# =============================================================================

class TestCheckExpectedColumns:
    def test_all_columns_present_passes(self, orders_df):
        result = check_expected_columns(orders_df, "orders", ["order_id", "amount", "status"])
        assert result.passed is True

    def test_missing_columns_fails(self, orders_df):
        result = check_expected_columns(
            orders_df, "orders", ["order_id", "nonexistent", "also_missing"]
        )
        assert result.passed is False
        assert result.metric_value == 2.0  # 2 missing columns


# =============================================================================
# Test: check_allowed_values
# =============================================================================

class TestCheckAllowedValues:
    def test_valid_values_pass(self, orders_df):
        result = check_allowed_values(
            orders_df, "orders", "status", ["COMPLETED", "PENDING", "CANCELLED"]
        )
        assert result.passed is True

    def test_invalid_values_fail(self, orders_df):
        result = check_allowed_values(
            orders_df, "orders", "status", ["COMPLETED"]  # Missing PENDING and CANCELLED
        )
        assert result.passed is False
        assert result.metric_value == 2.0  # 2 rows with invalid values


# =============================================================================
# Test: check_row_count_consistency
# =============================================================================

class TestCheckRowCountConsistency:
    def test_same_count_passes(self, orders_df):
        result = check_row_count_consistency(orders_df, orders_df, "source", "target")
        assert result.passed is True
        assert result.metric_value == 0.0

    def test_small_loss_within_threshold(self, spark, orders_df):
        # Target has 2 rows vs source 3 = 33% loss, threshold 50% => passes
        smaller = orders_df.limit(2)
        result = check_row_count_consistency(
            orders_df, smaller, "source", "target", max_loss_pct=50.0
        )
        assert result.passed is True

    def test_large_loss_exceeds_threshold(self, spark, orders_df):
        # Target has 1 row vs source 3 = 66% loss, threshold 5% => fails
        smaller = orders_df.limit(1)
        result = check_row_count_consistency(
            orders_df, smaller, "source", "target", max_loss_pct=5.0
        )
        assert result.passed is False

    def test_empty_source_is_warning(self, spark, empty_df, orders_df):
        result = check_row_count_consistency(empty_df, orders_df, "source", "target")
        assert result.passed is True
        assert result.severity == "WARN"


# =============================================================================
# Test: QualityReport
# =============================================================================

class TestQualityReport:
    def test_all_pass_report(self):
        report = QualityReport(layer="silver", table="orders")
        report.results = [
            QualityCheckResult("check_1", True, 0.0, 0.0, "ok"),
            QualityCheckResult("check_2", True, 3.0, 1.0, "ok"),
        ]
        assert report.passed is True
        assert report.summary["overall_status"] == "PASS"
        assert report.summary["failed"] == 0

    def test_error_failure_blocks_report(self):
        report = QualityReport(layer="silver", table="orders")
        report.results = [
            QualityCheckResult("check_1", True, 0.0, 0.0, "ok"),
            QualityCheckResult("check_2", False, 5.0, 0.0, "nulls found", severity="ERROR"),
        ]
        assert report.passed is False
        assert report.summary["overall_status"] == "FAIL"
        assert report.summary["failed"] == 1

    def test_warn_failure_does_not_block(self):
        report = QualityReport(layer="silver", table="orders")
        report.results = [
            QualityCheckResult("check_1", True, 0.0, 0.0, "ok"),
            QualityCheckResult("check_2", False, 5.0, 0.0, "minor issue", severity="WARN"),
        ]
        assert report.passed is True  # WARN failures don't block
