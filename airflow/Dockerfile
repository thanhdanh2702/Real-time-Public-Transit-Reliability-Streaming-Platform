ARG AIRFLOW_IMAGE=apache/airflow:3.1.4-python3.12
FROM ${AIRFLOW_IMAGE}

ARG AIRFLOW_VERSION=3.1.4

COPY airflow/requirements.txt /tmp/transitpulse-airflow-requirements.txt

RUN python -m pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    -r /tmp/transitpulse-airflow-requirements.txt
