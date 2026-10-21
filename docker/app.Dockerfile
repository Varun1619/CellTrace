# One image for the generator, Spark streaming jobs, dbt, scripts, tests and the dashboard.
FROM python:3.11-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless procps curl \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Same UID as the Airflow image so both can write the shared lakehouse volume.
RUN useradd --uid 50000 --gid 0 --create-home --home-dir /home/celltrace celltrace \
    && mkdir -p /app /lakehouse && chown 50000:0 /app /lakehouse

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

USER 50000
ENV HOME=/home/celltrace

# Resolve the Delta and Kafka connector jars at build time so containers start offline.
COPY --chown=50000:0 config/ config/
COPY --chown=50000:0 streaming/__init__.py streaming/spark_session.py streaming/
RUN python -c "from streaming.spark_session import build_spark; build_spark('warmup').stop()"

# Preload the DuckDB delta extension for dbt, scripts and the dashboard.
RUN python -c "import duckdb; c = duckdb.connect(); c.install_extension('delta'); c.load_extension('delta')"

# The repo is bind-mounted over /app at runtime; this copy only seeds the build.
CMD ["python", "-c", "print('celltrace app image: pass a command')"]
