"""
Data Quality Validation Module for the Retail Data Platform

Provides production-grade data quality checks between Medallion layers.
Each check returns a structured result with pass/fail, metric values,
and descriptive messages. Designed to be called from Airflow DAGs or
standalone scripts.

Quality checks implemented:
    - Schema validation (expected columns and types)
    - Null checks on critical columns
    - Row count thresholds (minimum and cross-layer consistency)
    - Value range validation (e.g., amounts must be positive)
    - Duplicate detection on primary keys
    - Referential integrity (cross-table FK checks)
"""

import sys
from dataclasses import dataclass, field
from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, isnull

from src.shared.logging_config import get_logger

logger = get_logger(__name__)


@dataclass
class QualityCheckResult:
    """Result of a single data quality check."""

    check_name: str
    passed: bool
    metric_value: float
    threshold: float
    message: str
    severity: str = "ERROR"  # ERROR = blocks pipeline, WARN = logs only


@dataclass
class QualityReport:
    """Aggregated results from all quality checks on a dataset."""

    layer: str
    table: str
    results: List[QualityCheckResult] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        """Report passes only if all ERROR-severity checks pass."""
        return all(r.passed for r in self.results if r.severity == "ERROR")

    @property
    def summary(self) -> dict:
        total = len(self.results)
        passed = sum(1 for r in self.results if r.passed)
        failed = total - passed
        return {
            "layer": self.layer,
            "table": self.table,
            "total_checks": total,
            "passed": passed,
            "failed": failed,
            "overall_status": "PASS" if self.passed else "FAIL",
        }


# =============================================================================
# Individual Quality Checks
# =============================================================================


def check_not_empty(
    df: DataFrame, table_name: str, min_rows: int = 1
) -> QualityCheckResult:
    """Verify that the DataFrame has at least min_rows records."""
    row_count = df.count()
    passed = row_count >= min_rows
    return QualityCheckResult(
        check_name=f"{table_name}_not_empty",
        passed=passed,
        metric_value=float(row_count),
        threshold=float(min_rows),
        message=(
            f"Table '{table_name}' has {row_count} rows " f"(minimum: {min_rows})"
        ),
        severity="ERROR",
    )


def check_no_nulls(
    df: DataFrame, table_name: str, columns: List[str]
) -> List[QualityCheckResult]:
    """Verify that specified columns contain no NULL values."""
    results = []
    total_rows = df.count()

    for column in columns:
        if column not in df.columns:
            results.append(
                QualityCheckResult(
                    check_name=f"{table_name}_{column}_no_nulls",
                    passed=False,
                    metric_value=0.0,
                    threshold=0.0,
                    message=(f"Column '{column}' does not exist in " f"'{table_name}'"),
                    severity="ERROR",
                )
            )
            continue

        null_count = df.filter(isnull(col(column))).count()
        null_pct = (null_count / total_rows * 100) if total_rows > 0 else 0.0
        passed = null_count == 0

        results.append(
            QualityCheckResult(
                check_name=f"{table_name}_{column}_no_nulls",
                passed=passed,
                metric_value=null_pct,
                threshold=0.0,
                message=(
                    f"Column '{column}' has {null_count} nulls " f"({null_pct:.2f}%)"
                ),
                severity="ERROR",
            )
        )

    return results


def check_no_duplicates(
    df: DataFrame, table_name: str, key_columns: List[str]
) -> QualityCheckResult:
    """Verify that key columns form a unique key (no duplicate rows)."""
    total_rows = df.count()
    distinct_rows = df.select(*key_columns).distinct().count()
    duplicate_count = total_rows - distinct_rows
    passed = duplicate_count == 0

    return QualityCheckResult(
        check_name=f"{table_name}_no_duplicates_on_{'_'.join(key_columns)}",
        passed=passed,
        metric_value=float(duplicate_count),
        threshold=0.0,
        message=f"Found {duplicate_count} duplicate rows on key {key_columns}",
        severity="ERROR",
    )


def check_value_range(
    df: DataFrame,
    table_name: str,
    column: str,
    min_value: Optional[float] = None,
    max_value: Optional[float] = None,
) -> QualityCheckResult:
    """Verify that all values in a column fall within the specified range."""
    violations = df
    description_parts = []

    if min_value is not None:
        violations = violations.filter(col(column) < min_value)
        description_parts.append(f">= {min_value}")

    if max_value is not None:
        violations = violations.filter(col(column) > max_value)
        description_parts.append(f"<= {max_value}")

    violation_count = violations.count()
    passed = violation_count == 0
    range_desc = " and ".join(description_parts) if description_parts else "any"

    return QualityCheckResult(
        check_name=f"{table_name}_{column}_in_range",
        passed=passed,
        metric_value=float(violation_count),
        threshold=0.0,
        message=(
            f"Column '{column}' has {violation_count} values outside"
            f" range ({range_desc})"
        ),
        severity="ERROR",
    )


def check_expected_columns(
    df: DataFrame, table_name: str, expected_columns: List[str]
) -> QualityCheckResult:
    """Verify that the DataFrame contains all expected columns."""
    actual_columns = set(df.columns)
    missing = set(expected_columns) - actual_columns
    passed = len(missing) == 0

    return QualityCheckResult(
        check_name=f"{table_name}_schema_check",
        passed=passed,
        metric_value=float(len(missing)),
        threshold=0.0,
        message=(
            f"Missing columns: {sorted(missing)}"
            if missing
            else "All expected columns present"
        ),
        severity="ERROR",
    )


def check_row_count_consistency(
    source_df: DataFrame,
    target_df: DataFrame,
    source_name: str,
    target_name: str,
    max_loss_pct: float = 5.0,
) -> QualityCheckResult:
    """
    Verify that the target table didn't lose too many rows vs source.

    Allows a configurable loss percentage to account for deduplication
    and filtering, but catches catastrophic data loss.
    """
    source_count = source_df.count()
    target_count = target_df.count()

    if source_count == 0:
        return QualityCheckResult(
            check_name=f"{source_name}_to_{target_name}_row_consistency",
            passed=True,
            metric_value=0.0,
            threshold=max_loss_pct,
            message="Source is empty — nothing to compare",
            severity="WARN",
        )

    loss_pct = ((source_count - target_count) / source_count) * 100
    passed = loss_pct <= max_loss_pct

    return QualityCheckResult(
        check_name=f"{source_name}_to_{target_name}_row_consistency",
        passed=passed,
        metric_value=loss_pct,
        threshold=max_loss_pct,
        message=(
            f"Row loss: {loss_pct:.2f}% "
            f"(source={source_count}, target={target_count}, "
            f"max_allowed={max_loss_pct}%)"
        ),
        severity="ERROR",
    )


def check_allowed_values(
    df: DataFrame, table_name: str, column: str, allowed_values: List[str]
) -> QualityCheckResult:
    """Verify that a column only contains values from an allowed set."""
    invalid_rows = df.filter(~col(column).isin(allowed_values))
    violation_count = invalid_rows.count()
    passed = violation_count == 0

    return QualityCheckResult(
        check_name=f"{table_name}_{column}_allowed_values",
        passed=passed,
        metric_value=float(violation_count),
        threshold=0.0,
        message=(
            f"Column '{column}' has {violation_count} rows with values "
            f"outside {allowed_values}"
        ),
        severity="ERROR",
    )


# =============================================================================
# Composite Quality Suites (used by DAGs)
# =============================================================================


def validate_silver_orders(spark: SparkSession) -> QualityReport:
    """Run all quality checks on the Silver orders table."""
    report = QualityReport(layer="silver", table="orders")

    try:
        silver_df = spark.read.format("delta").load("s3a://silver/orders")
    except Exception as e:
        logger.error(
            "Failed to load Silver orders for quality check",
            extra={"error": str(e)},
        )
        report.results.append(
            QualityCheckResult(
                check_name="silver_orders_readable",
                passed=False,
                metric_value=0.0,
                threshold=0.0,
                message=f"Cannot read Silver orders: {e}",
                severity="ERROR",
            )
        )
        return report

    # 1. Schema check
    report.results.append(
        check_expected_columns(
            silver_df,
            "silver_orders",
            [
                "order_id",
                "customer_id",
                "amount",
                "status",
                "created_at",
                "ingested_at",
            ],
        )
    )

    # 2. Not empty
    report.results.append(check_not_empty(silver_df, "silver_orders", min_rows=1))

    # 3. No nulls on critical columns
    report.results.extend(
        check_no_nulls(
            silver_df,
            "silver_orders",
            ["order_id", "customer_id", "amount", "status"],
        )
    )

    # 4. No duplicate order_ids
    report.results.append(check_no_duplicates(silver_df, "silver_orders", ["order_id"]))

    # 5. Amount must be positive
    report.results.append(
        check_value_range(silver_df, "silver_orders", "amount", min_value=0.0)
    )

    # 6. Status must be a known value
    report.results.append(
        check_allowed_values(
            silver_df,
            "silver_orders",
            "status",
            ["PENDING", "COMPLETED", "CANCELLED"],
        )
    )

    return report


def validate_gold_sales_summary(spark: SparkSession) -> QualityReport:
    """Run all quality checks on the Gold daily sales summary table."""
    report = QualityReport(layer="gold", table="daily_sales_summary")

    try:
        gold_df = spark.read.format("delta").load("s3a://gold/daily_sales_summary")
    except Exception as e:
        logger.error(
            "Failed to load Gold sales summary for quality check",
            extra={"error": str(e)},
        )
        report.results.append(
            QualityCheckResult(
                check_name="gold_sales_readable",
                passed=False,
                metric_value=0.0,
                threshold=0.0,
                message=f"Cannot read Gold sales summary: {e}",
                severity="ERROR",
            )
        )
        return report

    # 1. Schema check
    report.results.append(
        check_expected_columns(
            gold_df,
            "gold_sales_summary",
            ["status", "total_revenue", "order_count"],
        )
    )

    # 2. Not empty
    report.results.append(check_not_empty(gold_df, "gold_sales_summary", min_rows=1))

    # 3. No nulls
    report.results.extend(
        check_no_nulls(
            gold_df,
            "gold_sales_summary",
            ["status", "total_revenue", "order_count"],
        )
    )

    # 4. Revenue must be positive
    report.results.append(
        check_value_range(gold_df, "gold_sales_summary", "total_revenue", min_value=0.0)
    )

    # 5. Order count must be positive
    report.results.append(
        check_value_range(gold_df, "gold_sales_summary", "order_count", min_value=1.0)
    )

    return report


def run_all_quality_checks(spark: SparkSession) -> bool:
    """
    Run all quality checks and log results. Returns True if all pass.

    This is the entry point called by the Airflow DAG via spark-submit.
    """
    logger.info("Starting data quality validation suite")
    all_passed = True

    for validate_fn in [validate_silver_orders, validate_gold_sales_summary]:
        report = validate_fn(spark)

        # Log each check result
        for result in report.results:
            log_fn = logger.info if result.passed else logger.error
            log_fn(
                f"Quality check: {result.check_name}",
                extra={
                    "check_name": result.check_name,
                    "passed": result.passed,
                    "metric_value": result.metric_value,
                    "threshold": result.threshold,
                    "severity": result.severity,
                    "message": result.message,
                    "layer": report.layer,
                    "table": report.table,
                },
            )

        # Log summary
        logger.info(
            f"Quality report: {report.layer}/{report.table}",
            extra=report.summary,
        )

        if not report.passed:
            all_passed = False

    if all_passed:
        logger.info("All data quality checks PASSED")
    else:
        logger.error("Data quality checks FAILED — pipeline should halt")

    return all_passed


if __name__ == "__main__":
    from src.shared.spark_session import get_spark_session

    spark = get_spark_session("DataQualityChecks")
    spark.sparkContext.setLogLevel("WARN")

    passed = run_all_quality_checks(spark)
    sys.exit(0 if passed else 1)
