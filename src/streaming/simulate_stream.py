"""Dépose de petits fichiers CSV dans data/raw_stream/ pour simuler un flux de transactions.

Usage : python -m src.streaming.simulate_stream [--batches 10] [--size 2000] [--pause 5]
"""
import argparse
import time
from src.ingestion.generate_transactions import build_transactions
from src.utils.config import RAW_STREAM_PATH
from src.utils.spark import get_spark


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--batches", type=int, default=10)
    parser.add_argument("--size", type=int, default=2000)
    parser.add_argument("--pause", type=float, default=5.0)
    args = parser.parse_args()

    spark = get_spark("simulate-stream")
    for i in range(args.batches):
        df = build_transactions(spark, args.size, seed=1000 + i, id_offset=50_000_000 + i * args.size)
        df.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{RAW_STREAM_PATH}/batch_{i:04d}")
        print(f"Lot {i + 1}/{args.batches} déposé ({args.size} transactions)")
        time.sleep(args.pause)
    spark.stop()


if __name__ == "__main__":
    main()
