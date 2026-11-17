"""Couche GOLD : Fraud Score /100, alertes et KPI (global, pays, heure, clients).

Usage : python -m src.transformations.gold_kpis
"""
from pyspark.sql import functions as F
from src.transformations.rules import fraud_score_expr
from src.utils.config import SILVER_PATH, ALERT_THRESHOLD, gold_path
from src.utils.spark import get_spark, read_table, write_table


def rate(num, den):
    return F.round(F.when(den > 0, num / den * 100).otherwise(0.0), 3)


def main():
    spark = get_spark("gold-kpis")
    df = (
        read_table(spark, SILVER_PATH)
        .withColumn("fraud_score", fraud_score_expr())
        .withColumn("alert_flag", (F.col("fraud_score") >= ALERT_THRESHOLD).cast("int"))
    )

    scored = df.select("tx_id", "event_ts", "tx_date", "tx_hour", "customer_id", "merchant_id", "tx_type",
                       "channel", "country", "device_id", "amount", "old_balance", "new_balance",
                       "r1_high_amount", "r2_velocity", "r3_night", "r4_low_balance",
                       "fraud_score", "alert_flag", "is_fraud")
    write_table(scored, gold_path("scored_transactions"))
    scored = read_table(spark, gold_path("scored_transactions"))

    # KPI global (avec précision / rappel des règles par rapport au label simulé)
    g = scored.agg(
        F.count("*").alias("nb_transactions"),
        F.sum("is_fraud").alias("nb_fraudes"),
        F.sum("alert_flag").alias("nb_alertes"),
        F.sum(F.col("alert_flag") * F.col("is_fraud")).alias("alertes_vraies"),
    ).withColumn("fraud_rate_pct", rate(F.col("nb_fraudes"), F.col("nb_transactions"))) \
     .withColumn("precision_pct", rate(F.col("alertes_vraies"), F.col("nb_alertes"))) \
     .withColumn("recall_pct", rate(F.col("alertes_vraies"), F.col("nb_fraudes")))
    write_table(g, gold_path("kpi_global"))

    by_country = scored.groupBy("country").agg(
        F.count("*").alias("nb_transactions"), F.sum("is_fraud").alias("nb_fraudes"),
        F.sum("alert_flag").alias("nb_alertes")
    ).withColumn("fraud_rate_pct", rate(F.col("nb_fraudes"), F.col("nb_transactions")))
    write_table(by_country, gold_path("kpi_by_country"))

    by_hour = scored.groupBy("tx_hour").agg(
        F.count("*").alias("nb_transactions"), F.sum("is_fraud").alias("nb_fraudes"),
        F.sum("alert_flag").alias("nb_alertes")
    ).withColumn("fraud_rate_pct", rate(F.col("nb_fraudes"), F.col("nb_transactions")))
    write_table(by_hour, gold_path("kpi_by_hour"))

    by_customer = (
        scored.groupBy("customer_id").agg(
            F.count("*").alias("nb_transactions"), F.sum("alert_flag").alias("nb_alertes"),
            F.round(F.sum("fraud_score"), 1).alias("score_cumule"), F.max("fraud_score").alias("score_max"))
        .orderBy(F.desc("score_cumule")).limit(1000)
    )
    write_table(by_customer, gold_path("kpi_by_customer"))

    print("=== KPI global ===")
    read_table(spark, gold_path("kpi_global")).show(truncate=False)
    print("=== KPI par heure (pic nocturne attendu) ===")
    read_table(spark, gold_path("kpi_by_hour")).orderBy("tx_hour").show(24)
    print("=== Top 5 clients suspects ===")
    read_table(spark, gold_path("kpi_by_customer")).show(5)
    spark.stop()


if __name__ == "__main__":
    main()
