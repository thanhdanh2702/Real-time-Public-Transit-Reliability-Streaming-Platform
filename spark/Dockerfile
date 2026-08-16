ARG SPARK_IMAGE=apache/spark:4.2.0-scala2.13-java17-python3-ubuntu
FROM ${SPARK_IMAGE}

ENV PYTHONPATH=/opt/transitpulse:/opt/spark/python/lib/pyspark.zip:/opt/spark/python/lib/py4j-0.10.9.9-src.zip \
    SPARK_CONF_DIR=/opt/transitpulse/spark/conf

USER root

RUN python3 -m pip install --no-cache-dir \
    "pytest>=8,<10" \
    "pytest-cov>=6,<8"

RUN mkdir -p /opt/transitpulse/spark /opt/transitpulse/configs /opt/spark/checkpoints \
    && chown -R 185:0 /opt/transitpulse /opt/spark/checkpoints

COPY --chown=185:0 spark /opt/transitpulse/spark
COPY --chown=185:0 spark/conf /opt/spark/conf
COPY --chown=185:0 configs /opt/transitpulse/configs

USER 185
WORKDIR /opt/transitpulse
