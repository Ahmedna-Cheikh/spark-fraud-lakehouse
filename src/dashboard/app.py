"""Dashboard Streamlit lisant les tables Gold (Delta) sans Spark, via delta-rs."""
from pathlib import Path
import pandas as pd
import streamlit as st
from deltalake import DeltaTable

ROOT = Path(__file__).resolve().parents[2]
GOLD = ROOT / "data" / "gold"


@st.cache_data
def load(name: str) -> pd.DataFrame:
    return DeltaTable(str(GOLD / name)).to_pandas()


st.set_page_config(page_title="Détection de fraude bancaire", layout="wide")
st.title("Détection de fraude bancaire - Lakehouse Spark & Delta Lake")

if not (GOLD / "kpi_global").exists():
    st.warning("Tables Gold introuvables. Lancez d'abord : make pipeline")
    st.stop()

g = load("kpi_global").iloc[0]
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Transactions", f"{int(g.nb_transactions):,}")
c2.metric("Taux de fraude", f"{g.fraud_rate_pct:.2f} %")
c3.metric("Alertes (score >= 70)", f"{int(g.nb_alertes):,}")
c4.metric("Précision des règles", f"{g.precision_pct:.1f} %")
c5.metric("Rappel des règles", f"{g.recall_pct:.1f} %")

left, right = st.columns(2)
with left:
    st.subheader("Fraudes par heure (pic nocturne)")
    h = load("kpi_by_hour").sort_values("tx_hour").set_index("tx_hour")
    st.bar_chart(h["nb_fraudes"])
with right:
    st.subheader("Taux de fraude par pays (%)")
    c = load("kpi_by_country").sort_values("fraud_rate_pct", ascending=False).set_index("country")
    st.bar_chart(c["fraud_rate_pct"])

st.subheader("Top clients suspects")
st.dataframe(load("kpi_by_customer").head(50), use_container_width=True)
