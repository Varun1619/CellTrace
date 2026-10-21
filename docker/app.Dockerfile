# One image for the generator, Spark streaming jobs, dbt, scripts, tests and the dashboard.
FROM python:3.11-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless procps curl \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Resolve the Delta and Kafka connector jars at build time so containers start offline.
COPY config/ config/
COPY streaming/__init__.py streaming/spark_session.py streaming/
RUN python -c "from streaming.spark_session import build_spark; build_spark('warmup').stop()"

# Preload the DuckDB delta extension for dbt, scripts and the dashboard.
RUN python -c "import duckdb; c = duckdb.connect(); c.install_extension('delta'); c.load_extension('delta')"

# The repo is bind-mounted over /app at runtime; this copy only seeds the build.
CMD ["python", "-c", "print('celltrace app image: pass a command')"]
