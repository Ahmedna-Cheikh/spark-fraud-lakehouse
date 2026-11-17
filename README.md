# Détection de fraude bancaire - Lakehouse Spark & Delta Lake (100 % local)

Pipeline Medallion (Bronze > Silver > Gold) sur 2 000 000 de transactions simulées,
règles de fraude R1-R4, Fraud Score /100, KPI, modèle ML, streaming et dashboard.

## Démarrage rapide
```bash
make build        # construit l'image Docker (Spark + Delta + Jupyter + MLflow), 5 à 10 min la 1re fois
make up           # démarre JupyterLab (8888) et MLflow (5000)
make pipeline     # generate -> bronze -> silver -> gold
make optimize     # OPTIMIZE / Z-ORDER / time travel
make train        # modèle ML + comparaison avec les règles
make dashboard    # http://localhost:8501
make test         # tests unitaires
make down
```
Sans `make` : `sudo dnf install make`, ou lancez les commandes
`docker compose -f docker/docker-compose.yml exec lab python -m src....` du Makefile.

## Structure
- `src/ingestion/` : génération des données (CSV) et couche Bronze
- `src/transformations/` : règles R1-R4, Silver, Gold, optimisation Delta
- `src/ml/` : entraînement GBT (MLlib) + MLflow
- `src/streaming/` : simulateur de flux + Structured Streaming vers Bronze
- `src/dashboard/` : dashboard Streamlit (lit les tables Gold)
- `data/` : raw, delta (Bronze), silver, gold, quarantine
