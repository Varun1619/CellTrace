FROM apache/airflow:2.10.5-python3.11

USER root
RUN mkdir -p /lakehouse && chown 50000:0 /lakehouse
USER airflow

# dbt and delta-rs live in their own virtualenv so their pins never fight Airflow's constraints.
COPY requirements-airflow.txt /tmp/requirements-airflow.txt
RUN python -m venv /opt/airflow/celltrace-venv \
    && /opt/airflow/celltrace-venv/bin/pip install --no-cache-dir -r /tmp/requirements-airflow.txt \
    && /opt/airflow/celltrace-venv/bin/python -c "import duckdb; duckdb.connect().install_extension('delta')"

# pytest and dotenv in Airflow's own environment, for the DAG tests.
RUN pip install --no-cache-dir pytest==8.3.4 python-dotenv==1.0.1
