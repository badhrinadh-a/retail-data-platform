import os

from pyspark.sql import SparkSession


def get_spark_session(app_name="RetailDataPlatform"):
    """
    Creates and returns a SparkSession configured with Delta Lake, AWS S3,
    and Kafka streaming packages.

    All credentials and endpoints are read from environment variables,
    falling back to development defaults if not set.
    """
    # MinIO / S3 configuration from environment
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    minio_access_key = os.getenv("MINIO_ROOT_USER", "minioadmin")
    minio_secret_key = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")

    packages = [
        "io.delta:delta-core_2.12:4.2.0",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:4.1.1",
    ]

    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(
            "spark.jars.packages",
            ",".join(packages),
        )
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config(
            "spark.hadoop.fs.s3a.access.key",
            minio_access_key,
        )
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            minio_secret_key,
        )
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )
