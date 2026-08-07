ARG PYTHON_IMAGE=python:3.12-slim
FROM ${PYTHON_IMAGE}

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    STREAMLIT_SERVER_ADDRESS=0.0.0.0 \
    STREAMLIT_SERVER_PORT=8501 \
    STREAMLIT_BROWSER_GATHER_USAGE_STATS=false

WORKDIR /app

RUN addgroup --system transitpulse && adduser --system --ingroup transitpulse transitpulse

COPY pyproject.toml README.md ./
COPY dashboard ./dashboard

RUN python -m pip install --upgrade pip && python -m pip install ".[dashboard]"

USER transitpulse

CMD ["streamlit", "run", "dashboard/app.py"]
