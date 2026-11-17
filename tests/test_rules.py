from datetime import datetime
import pytest
from pyspark.sql import SparkSession
from src.transformations.rules import add_fraud_rules, fraud_score_expr


@pytest.fixture(scope="module")
def spark():
    s = (SparkSession.builder.master("local[1]").appName("tests")
         .config("spark.sql.session.timeZone", "UTC").config("spark.ui.enabled", "false").getOrCreate())
    yield s
    s.stop()


def test_rules_and_score(spark):
    rows = [
        # client 1 : 3 transactions en 10 minutes, de nuit, gros montant, compte vidé
        (1, 1, "TRANSFER", 60000.0, 60001.0, 1.0, datetime(2026, 3, 1, 2, 0, 0)),
        (2, 1, "TRANSFER", 60000.0, 60001.0, 1.0, datetime(2026, 3, 1, 2, 5, 0)),
        (3, 1, "TRANSFER", 60000.0, 60001.0, 1.0, datetime(2026, 3, 1, 2, 10, 0)),
        # client 2 : transaction normale en journée
        (4, 2, "PAYMENT", 500.0, 9000.0, 8500.0, datetime(2026, 3, 1, 14, 0, 0)),
    ]
    cols = ["tx_id", "customer_id", "tx_type", "amount", "old_balance", "new_balance", "event_ts"]
    df = add_fraud_rules(spark.createDataFrame(rows, cols)).withColumn("score", fraud_score_expr())
    out = {r["tx_id"]: r for r in df.collect()}

    assert out[1]["r1_high_amount"] == 1 and out[1]["r3_night"] == 1 and out[1]["r4_low_balance"] == 1
    assert out[1]["r2_velocity"] == 0           # 1re transaction : pas encore de rafale
    assert out[3]["r2_velocity"] == 1           # 3e transaction dans l'heure
    assert out[3]["score"] == 100.0
    assert out[4]["score"] == 0.0
