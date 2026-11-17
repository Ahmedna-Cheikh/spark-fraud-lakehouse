"""Règles de fraude R1 à R4 et score pondéré (réutilisés par Silver, Gold et ML)."""
from pyspark.sql import Window
from pyspark.sql import functions as F
from src.utils.config import (HIGH_AMOUNT, VELOCITY_WINDOW_SEC, VELOCITY_MIN_TX, NIGHT_START,
                              NIGHT_END, LOW_BALANCE, WEIGHTS)


def add_fraud_rules(df):
    """Ajoute tx_count_1h et les indicateurs r1..r4 (0/1). Colonnes requises :
    customer_id, event_ts, amount, old_balance, new_balance, tx_type, tx_id."""
    w = (Window.partitionBy("customer_id")
         .orderBy(F.col("event_ts").cast("long"))
         .rangeBetween(-VELOCITY_WINDOW_SEC, 0))
    return (
        df.withColumn("tx_count_1h", F.count("tx_id").over(w))
        .withColumn("r1_high_amount", (F.col("amount") >= HIGH_AMOUNT).cast("int"))
        .withColumn("r2_velocity", (F.col("tx_count_1h") >= VELOCITY_MIN_TX).cast("int"))
        .withColumn("r3_night", F.hour("event_ts").between(NIGHT_START, NIGHT_END).cast("int"))
        .withColumn("r4_low_balance", ((F.col("new_balance") < LOW_BALANCE) & (F.col("old_balance") > 0)
                                       & F.col("tx_type").isin("TRANSFER", "CASH_OUT")).cast("int"))
    )


def fraud_score_expr():
    """Score /100 = (R1*3 + R2*3 + R3*2 + R4*1) / 9 * 100."""
    total = sum(WEIGHTS.values())
    raw = (F.col("r1_high_amount") * WEIGHTS["r1"] + F.col("r2_velocity") * WEIGHTS["r2"]
           + F.col("r3_night") * WEIGHTS["r3"] + F.col("r4_low_balance") * WEIGHTS["r4"])
    return F.round(raw / total * 100, 1)
