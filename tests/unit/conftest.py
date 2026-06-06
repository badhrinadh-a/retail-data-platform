"""
Shared test fixtures for all unit tests.

A single session-scoped SparkSession with Delta Lake support avoids conflicts
when multiple test modules each try to create their own session.
"""

import sys
import typing

# Workaround for Python 3.12+ compatibility with older PySpark versions (like 3.4.1)
# PySpark tries to import BinaryIO/TextIO from typing.io,
# which was removed in Python 3.12.
try:
    import typing.io
except ModuleNotFoundError:
    import types

    typing_io = types.ModuleType("typing.io")
    typing_io.BinaryIO = typing.BinaryIO
    typing_io.TextIO = typing.TextIO
    sys.modules["typing.io"] = typing_io

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
