"""Création de la SparkSession locale (+ Delta Lake) et lecture/écriture des tables."""
import os
from pyspark.sql import SparkSession
from src.utils.config import LAKE_FORMAT


def get_spark(app_name: str = "fraud-lakehouse") -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "4g"))
        .config("spark.sql.shuffle.partitions", os.getenv("SHUFFLE_PARTITIONS", "16"))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
    )
    if LAKE_FORMAT == "delta":
        builder = (
            builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def write_table(df, path, mode="overwrite", partition_by=None):
    writer = df.write.format(LAKE_FORMAT).mode(mode)
    if LAKE_FORMAT == "delta" and mode == "overwrite":
        writer = writer.option("overwriteSchema", "true")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)


def read_table(spark, path):
    return spark.read.format(LAKE_FORMAT).load(path)
