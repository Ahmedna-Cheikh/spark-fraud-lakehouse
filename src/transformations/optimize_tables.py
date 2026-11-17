"""Optimisation Delta : OPTIMIZE / Z-ORDER, historique et time travel (Delta uniquement).

Usage : python -m src.transformations.optimize_tables
"""
from delta.tables import DeltaTable
from src.utils.config import BRONZE_PATH, SILVER_PATH, gold_path
from src.utils.spark import get_spark


def main():
    spark = get_spark("optimize-tables")

    bronze = DeltaTable.forPath(spark, BRONZE_PATH)
    bronze.optimize().executeCompaction()

    silver = DeltaTable.forPath(spark, SILVER_PATH)
    silver.optimize().executeZOrderBy("customer_id")

    scored = DeltaTable.forPath(spark, gold_path("scored_transactions"))
    scored.optimize().executeZOrderBy("customer_id")

    print("=== Historique Silver ===")
    silver.history(5).select("version", "timestamp", "operation").show(truncate=False)

    n_v0 = spark.read.format("delta").option("versionAsOf", 0).load(SILVER_PATH).count()
    print(f"Time travel : la version 0 de Silver contenait {n_v0:,} lignes")

    # Nettoyage des anciens fichiers (rétention par défaut : 7 jours)
    silver.vacuum()
    spark.stop()


if __name__ == "__main__":
    main()
