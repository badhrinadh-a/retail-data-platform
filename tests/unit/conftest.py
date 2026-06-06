"""
Shared test fixtures for all unit tests.

A single session-scoped SparkSession with Delta Lake support avoids conflicts
when multiple test modules each try to create their own session.
"""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Create a shared test SparkSession with Delta Lake support."""
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName("TestRetailPlatform")
        .master("local[1]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "1")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    yield spark
    spark.stop()
