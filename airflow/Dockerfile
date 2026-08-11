
FROM apache/airflow:2.10.3-python3.11
 
# Airflow images run as the 'airflow' user by default; pip installs as
# root are blocked, so switch briefly then switch back.
USER root
 
# Nothing extra needed at the OS level for psycopg2-binary (it's
# self-contained), but if you ever swap to plain psycopg2, uncomment:
# RUN apt-get update && apt-get install -y --no-install-recommends \
#     gcc libpq-dev && rm -rf /var/lib/apt/lists/*
 
USER airflow
 
# Copied in from the project root by the build context (see docker-compose.yml)
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
 