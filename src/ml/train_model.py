"""Modèle ML (GBT Spark MLlib) avec split chronologique, comparaison à la baseline par règles
et suivi MLflow (si disponible).

Usage : python -m src.ml.train_model
"""
import os
from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.sql import functions as F
from src.transformations.rules import fraud_score_expr
from src.utils.config import SILVER_PATH, MODEL_PATH, ALERT_THRESHOLD
from src.utils.spark import get_spark, read_table

SPLIT_DATE = "2026-05-01"   # train : avant ; test : à partir de cette date (anti-fuite)
NUMERIC = ["amount", "log_amount", "old_balance", "new_balance", "balance_drop_ratio", "tx_hour",
           "tx_count_1h", "is_foreign", "r1_high_amount", "r2_velocity", "r3_night", "r4_low_balance"]
CATEGORICAL = ["tx_type", "channel", "country"]


def prf(df, pred_col):
    r = df.agg(
        F.sum(F.when((F.col(pred_col) == 1) & (F.col("label") == 1), 1).otherwise(0)).alias("tp"),
        F.sum(F.when((F.col(pred_col) == 1) & (F.col("label") == 0), 1).otherwise(0)).alias("fp"),
        F.sum(F.when((F.col(pred_col) == 0) & (F.col("label") == 1), 1).otherwise(0)).alias("fn"),
    ).collect()[0]
    tp, fp, fn = r["tp"] or 0, r["fp"] or 0, r["fn"] or 0
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    return precision, recall, f1


def main():
    spark = get_spark("train-model")
    df = (
        read_table(spark, SILVER_PATH)
        .withColumn("log_amount", F.log1p("amount"))
        .withColumn("is_foreign", (F.col("country") != "MR").cast("int"))
        .withColumn("balance_drop_ratio", F.when(F.col("old_balance") > 0,
                    (F.col("old_balance") - F.col("new_balance")) / F.col("old_balance")).otherwise(0.0))
        .withColumn("label", F.col("is_fraud").cast("double"))
        .withColumn("weight", F.when(F.col("is_fraud") == 1, 20.0).otherwise(1.0))
        .withColumn("rule_alert", (fraud_score_expr() >= ALERT_THRESHOLD).cast("int"))
    )
    train = df.filter(F.col("tx_date") < F.lit(SPLIT_DATE).cast("date"))
    test = df.filter(F.col("tx_date") >= F.lit(SPLIT_DATE).cast("date"))

    indexers = [StringIndexer(inputCol=c, outputCol=c + "_idx", handleInvalid="keep") for c in CATEGORICAL]
    assembler = VectorAssembler(inputCols=NUMERIC + [c + "_idx" for c in CATEGORICAL], outputCol="features")
    gbt = GBTClassifier(labelCol="label", featuresCol="features", weightCol="weight",
                        maxIter=40, maxDepth=5, maxBins=32, seed=42)
    model = Pipeline(stages=indexers + [assembler, gbt]).fit(train)

    pred = model.transform(test).withColumn("ml_alert", F.col("prediction").cast("int")).cache()
    auc_pr = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderPR").evaluate(pred)
    auc_roc = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC").evaluate(pred)
    p_ml, r_ml, f_ml = prf(pred, "ml_alert")
    p_rb, r_rb, f_rb = prf(pred, "rule_alert")

    print("\n=== Résultats sur le jeu de test (mai-juin 2026) ===")
    print(f"Modèle GBT  : PR-AUC={auc_pr:.3f} ROC-AUC={auc_roc:.3f} | précision={p_ml:.3f} rappel={r_ml:.3f} F1={f_ml:.3f}")
    print(f"Règles R1-R4: précision={p_rb:.3f} rappel={r_rb:.3f} F1={f_rb:.3f}")

    model.write().overwrite().save(MODEL_PATH)
    print(f"Modèle sauvegardé dans {MODEL_PATH}")

    uri = os.getenv("MLFLOW_TRACKING_URI")
    if uri:
        try:
            import mlflow
            mlflow.set_tracking_uri(uri)
            mlflow.set_experiment("fraud-lakehouse")
            with mlflow.start_run(run_name="gbt_fraud"):
                mlflow.log_params({"model": "GBTClassifier", "maxIter": 40, "maxDepth": 5, "split_date": SPLIT_DATE})
                mlflow.log_metrics({"pr_auc": auc_pr, "roc_auc": auc_roc, "precision": p_ml, "recall": r_ml,
                                    "f1": f_ml, "rules_precision": p_rb, "rules_recall": r_rb})
            print(f"Run enregistré dans MLflow : {uri}")
        except Exception as exc:  # MLflow est optionnel
            print(f"MLflow indisponible ({exc}) : métriques non enregistrées")
    spark.stop()


if __name__ == "__main__":
    main()
