"""Structured Streaming : data/raw_stream/ (CSV) -> table Delta Bronze (streaming), avec checkpoint.

Usage : python -m src.streaming.stream_bronze --once      (traite l'existant puis s'arrête)
        python -m src.streaming.stream_bronze             (tourne en continu, Ctrl+C pour arrêter)
"""
import argparse
import os
from pyspark.sql import functions as F
from src.ingestion.bronze_ingest import SCHEMA
from src.utils.config import RAW_STREAM_PATH, BRONZE_STREAM_PATH, CHECKPOINT_PATH
from src.utils.spark import get_spark


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()

    os.makedirs(RAW_STREAM_PATH, exist_ok=True)
    spark = get_spark("stream-bronze")
    stream = (
        spark.readStream.schema(SCHEMA).option("header", True).csv(RAW_STREAM_PATH)
        .withColumn("source_file", F.col("_metadata.file_path"))
        .withColumn("ingestion_ts", F.current_timestamp())
    )
    writer = (stream.writeStream.format("delta").outputMode("append")
              .option("checkpointLocation", CHECKPOINT_PATH))
    writer = writer.trigger(availableNow=True) if args.once else writer.trigger(processingTime="5 seconds")
    query = writer.start(BRONZE_STREAM_PATH)
    query.awaitTermination()
    n = spark.read.format("delta").load(BRONZE_STREAM_PATH).count()
    print(f"Table streaming Bronze : {n:,} lignes")
    spark.stop()


if __name__ == "__main__":
    main()
