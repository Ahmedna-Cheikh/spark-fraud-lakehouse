"""Couche BRONZE : CSV brut -> table Delta, sans aucune transformation (tout en texte).

Usage : python -m src.ingestion.bronze_ingest [--append]
"""
import argparse
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
from src.utils.config import RAW_PATH, BRONZE_PATH, COLUMNS
from src.utils.spark import get_spark, write_table, read_table

SCHEMA = StructType([StructField(c, StringType(), True) for c in COLUMNS])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--append", action="store_true", help="ajoute au lieu de remplacer la table")
    args = parser.parse_args()

    spark = get_spark("bronze-ingest")
    raw = spark.read.option("header", True).schema(SCHEMA).csv(RAW_PATH)
    bronze = (
        raw.withColumn("source_file", F.col("_metadata.file_path"))
        .withColumn("ingestion_ts", F.current_timestamp())
        .withColumn("batch_id", F.lit(datetime.now().strftime("%Y%m%d_%H%M%S")))
    )
    write_table(bronze, BRONZE_PATH, mode="append" if args.append else "overwrite")

    n_src = spark.read.option("header", True).csv(RAW_PATH).count()
    n_bronze = read_table(spark, BRONZE_PATH).count()
    print(f"Réconciliation : source={n_src:,} | bronze={n_bronze:,} -> {'OK' if n_src == n_bronze else 'ECART'}")
    spark.stop()


if __name__ == "__main__":
    main()
