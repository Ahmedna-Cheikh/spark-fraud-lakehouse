"""Configuration centrale du projet (chemins, seuils, poids des règles)."""
import os
from pathlib import Path

ROOT = Path(os.getenv("PROJECT_ROOT", Path(__file__).resolve().parents[2]))
DATA = ROOT / "data"

# --- Chemins du Lakehouse -------------------------------------------------
RAW_PATH = str(DATA / "raw" / "transactions_csv")        # source brute (CSV)
RAW_STREAM_PATH = str(DATA / "raw_stream")               # dossier surveillé par le streaming
BRONZE_PATH = str(DATA / "delta" / "transactions")       # Bronze (Delta)
BRONZE_STREAM_PATH = str(DATA / "delta" / "transactions_stream")
SILVER_PATH = str(DATA / "silver" / "transactions")      # Silver (Delta)
QUARANTINE_PATH = str(DATA / "quarantine" / "transactions")
CHECKPOINT_PATH = str(DATA / "checkpoints" / "bronze_stream")
MODEL_PATH = str(ROOT / "models" / "gbt_fraud")


def gold_path(name: str) -> str:
    return str(DATA / "gold" / name)


# Format de stockage ("delta" par défaut ; "parquet" utile uniquement pour des tests sans JAR Delta)
LAKE_FORMAT = os.getenv("LAKE_FORMAT", "delta")

# --- Génération des données simulées ---------------------------------------
N_TRANSACTIONS = int(os.getenv("N_TRANSACTIONS", 2_000_000))
FRAUD_RATE = 0.01              # 1 % de fraudes injectées
DUPLICATE_RATE = 0.005         # 0,5 % de doublons
START_EPOCH = 1767225600       # 2026-01-01 00:00:00 UTC
SPAN_DAYS = 180
N_CUSTOMERS = 100_000

COLUMNS = ["tx_id", "timestamp", "customer_id", "merchant_id", "tx_type", "channel",
           "country", "device_id", "amount", "currency", "old_balance", "new_balance", "is_fraud"]

# --- Règles de fraude (Silver) et score (Gold) -----------------------------
HIGH_AMOUNT = 50_000           # R1 : montant élevé (MRU)
VELOCITY_WINDOW_SEC = 3600     # R2 : fenêtre de 1 heure
VELOCITY_MIN_TX = 3            # R2 : au moins 3 transactions dans la fenêtre
NIGHT_START, NIGHT_END = 1, 4  # R3 : heures nocturnes (1h à 4h59)
LOW_BALANCE = 10.0             # R4 : solde quasi nul après l'opération
WEIGHTS = {"r1": 3, "r2": 3, "r3": 2, "r4": 1}   # somme = 9
ALERT_THRESHOLD = 70           # alerte si score >= 70 / 100
