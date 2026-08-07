ARG PYTHON_IMAGE=python:3.12-slim
FROM ${PYTHON_IMAGE}

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN addgroup --system transitpulse && adduser --system --ingroup transitpulse transitpulse

COPY pyproject.toml README.md ./
COPY producer ./producer

RUN python -m pip install --upgrade pip && python -m pip install ".[producer]"

USER transitpulse

CMD ["python", "producer/src/main.py"]
