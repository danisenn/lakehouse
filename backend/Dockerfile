FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_NO_CACHE_DIR=off \
    PIP_DEFAULT_TIMEOUT=100 \
    ARTIFACT_DIR=/app/artifacts \
    CORS_ORIGINS=* \
    API_VERSION=v1

WORKDIR /app

# Optional system dependencies (adjust as needed for polars / parquet)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install Python deps
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy code and artifacts
COPY src /app/src
# Pre-create artifacts dir; can be mounted as a volume at runtime
RUN mkdir -p /app/artifacts

EXPOSE 8889

CMD ["uvicorn", "src.api.app:app", "--host", "0.0.0.0", "--port", "8889"]
