# Custom Airflow image

FROM apache/airflow:2.9.3

USER root
RUN apt-get update \
	&& apt-get install -y --no-install-recommends git postgresql-client \
	&& apt-get clean \
	&& rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir "dbt-postgres==1.8.*"
