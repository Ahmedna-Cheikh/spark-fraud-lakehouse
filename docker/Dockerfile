FROM python:3.11-slim-bookworm

ARG HOST_UID=1000
ARG HOST_GID=1000

# Java 17 (requis par Spark) + outils
RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless curl procps ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
RUN pip install --no-cache-dir "sqlalchemy<2.1"

# JARs Delta Lake placés directement dans Spark : aucun téléchargement au lancement
RUN SPARK_JARS=$(python -c "import pyspark,os;print(os.path.join(os.path.dirname(pyspark.__file__),'jars'))") \
    && curl -fsSL -o "$SPARK_JARS/delta-spark_2.12-3.2.1.jar" https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.1/delta-spark_2.12-3.2.1.jar \
    && curl -fsSL -o "$SPARK_JARS/delta-storage-3.2.1.jar" https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.1/delta-storage-3.2.1.jar

# Utilisateur avec le même UID/GID que votre session Fedora (fichiers non root sur le disque)
RUN groupadd -o -g ${HOST_GID} app && useradd -o -m -u ${HOST_UID} -g ${HOST_GID} -s /bin/bash app

ENV HOME=/home/app \
    PYTHONPATH=/workspace \
    PROJECT_ROOT=/workspace \
    PYSPARK_PYTHON=python
WORKDIR /workspace
USER app
