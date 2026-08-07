ARG SPARK_IMAGE=apache/spark:4.2.0-scala2.13-java17-python3-ubuntu
FROM ${SPARK_IMAGE}

USER root

RUN mkdir -p /opt/transitpulse/spark /opt/transitpulse/configs /opt/spark/checkpoints \
    && chown -R 185:0 /opt/transitpulse /opt/spark/checkpoints

COPY --chown=185:0 spark /opt/transitpulse/spark
COPY --chown=185:0 configs /opt/transitpulse/configs

USER 185
WORKDIR /opt/transitpulse
