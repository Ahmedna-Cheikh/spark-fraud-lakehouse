"""Couche SILVER : typage, qualité (quarantaine), déduplication, nulls, règles de fraude R1-R4.

Usage : python -m src.transformations.silver_clean
"""
from pyspark.sql import functions as F
from src.transformations.rules import add_fraud_rules
from src.utils.config import BRONZE_PATH, SILVER_PATH, QUARANTINE_PATH
from src.utils.spark import get_spark, read_table, write_table


def main():
    spark = get_spark("silver-clean")
    bronze = read_table(spark, BRONZE_PATH)

    # 1) Typage correct
    typed = (
        bronze
        .withColumn("tx_id", F.col("tx_id").cast("long"))
        .withColumn("customer_id", F.col("customer_id").cast("long"))
        .withColumn("merchant_id", F.col("merchant_id").cast("long"))
        .withColumn("amount", F.col("amount").cast("double"))
        .withColumn("old_balance", F.col("old_balance").cast("double"))
        .withColumn("new_balance", F.col("new_balance").cast("double"))
        .withColumn("is_fraud", F.col("is_fraud").cast("int"))
        .withColumn("event_ts", F.to_timestamp("timestamp"))
        .drop("timestamp")
    )

    # 2) Contrôles qualité -> quarantaine
    reason = (
        F.when(F.col("tx_id").isNull(), "tx_id_null")
        .when(F.col("customer_id").isNull(), "customer_id_null")
        .when(F.col("amount").isNull(), "amount_null")
        .when(F.col("amount") <= 0, "amount_not_positive")
        .when(F.col("event_ts").isNull(), "timestamp_invalid")
    )
    typed = typed.withColumn("reject_reason", reason)
    rejected = typed.filter(F.col("reject_reason").isNotNull())
    valid = typed.filter(F.col("reject_reason").isNull()).drop("reject_reason")
    write_table(rejected, QUARANTINE_PATH)

    # 3) Déduplication + gestion des nulls (avec indicateur)
    clean = (
        valid.dropDuplicates(["tx_id"])
        .withColumn("balance_missing", (F.col("old_balance").isNull() | F.col("new_balance").isNull()))
        .fillna({"old_balance": 0.0, "new_balance": 0.0, "country": "UNK", "channel": "UNKNOWN", "tx_type": "UNKNOWN"})
        .withColumn("tx_date", F.to_date("event_ts"))
        .withColumn("tx_hour", F.hour("event_ts"))
        .withColumn("tx_month", F.date_format("event_ts", "yyyy-MM"))
    )

    # 4) Règles de fraude R1 à R4
    silver = add_fraud_rules(clean)
    write_table(silver.repartition("tx_month"), SILVER_PATH, partition_by=["tx_month"])

    n_bronze = read_table(spark, BRONZE_PATH).count()
    n_quar = read_table(spark, QUARANTINE_PATH).count()
    n_silver = read_table(spark, SILVER_PATH).count()
    print(f"Bronze={n_bronze:,} | Quarantaine={n_quar:,} | Doublons supprimés={n_bronze - n_quar - n_silver:,} | Silver={n_silver:,}")
    spark.stop()


if __name__ == "__main__":
    main()
