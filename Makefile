COMPOSE = docker compose -f docker/docker-compose.yml
EXEC = $(COMPOSE) exec lab

.PHONY: env build up down shell generate bronze silver gold optimize train test pipeline stream-sim stream dashboard mlflow-ui logs

env:
	printf "HOST_UID=$$(id -u)\nHOST_GID=$$(id -g)\nSPARK_DRIVER_MEMORY=4g\n" > docker/.env
	mkdir -p mlflow models data/raw data/delta data/silver data/gold data/quarantine data/raw_stream data/checkpoints

build: env
	$(COMPOSE) build

up: env
	$(COMPOSE) up -d lab mlflow
	echo "JupyterLab : http://localhost:8888   |   MLflow : http://localhost:5000   |   Spark UI : http://localhost:4040"

down:
	$(COMPOSE) --profile dashboard down

shell:
	$(EXEC) bash

generate:
	$(EXEC) python -m src.ingestion.generate_transactions

bronze:
	$(EXEC) python -m src.ingestion.bronze_ingest

silver:
	$(EXEC) python -m src.transformations.silver_clean

gold:
	$(EXEC) python -m src.transformations.gold_kpis

optimize:
	$(EXEC) python -m src.transformations.optimize_tables

train:
	$(EXEC) python -m src.ml.train_model

test:
	$(EXEC) python -m pytest -q

pipeline: generate bronze silver gold

stream-sim:
	$(EXEC) python -m src.streaming.simulate_stream

stream:
	$(EXEC) python -m src.streaming.stream_bronze --once

dashboard:
	$(COMPOSE) --profile dashboard up -d dashboard
	echo "Dashboard : http://localhost:8501"

logs:
	$(COMPOSE) logs -f lab
