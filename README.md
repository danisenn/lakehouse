# AI-Powered Assistance System for Semantic Data Integration in Lakehouse Architectures

## Overview

This repository includes a unified assistant that evaluates datasets using:
- **Anomaly Detection** — Z-Score, IQR, Isolation Forest
- **Schema Recognition** — infers column names and data types
- **Semantic Field Mapping** — maps dataset columns to your reference fields

## Getting Started

### Prerequisites

- Docker & Docker Compose
- Node.js (for local frontend development)

### Environment Setup

Copy the example environment file and fill in your credentials:

```bash
cp .env.example .env
# Edit .env with your Dremio/MinIO credentials
```

> **Note:** If Dremio is on the **same server** as Docker, set `DREMIO_HOST=host.docker.internal`.
> If Dremio is on a **remote server**, use that server's IP. Do **not** use `localhost` inside Docker.

---

## Running with Docker

### Server / Production

```bash
docker compose -f docker-compose.server.yml up -d

# Services:
#   Backend:  http://localhost:8889
#   Frontend: http://localhost:8888
#   Ollama:   http://localhost:11434
```

### Local Development

Run the backend in Docker and the frontend locally for hot-reloading:

**1. Start Backend + Ollama:**

```bash
docker compose -f docker-compose.local.yml up
# Backend at http://localhost:8000
```

**2. Start Frontend:**

```bash
cd frontend
npm install
npm run dev
# Frontend at http://localhost:5173
```

---

## CLI Usage

### Quick Start

```bash
pip install -r backend/requirements.txt

python3 backend/scripts/lakehouse_assistant.py \
  --config backend/configs/assistant_example.yml \
  --refs label,title,text \
  --verbose
```

### CLI Options

```
python backend/scripts/lakehouse_assistant.py --help
```

Key flags:

| Flag | Description |
|------|-------------|
| `--root PATH` | Folder to scan for CSV/Parquet |
| `--refs a,b,c` | Reference fields for semantic mapping |
| `--refs-file PATH` | YAML/JSON containing `reference_fields: [...]` |
| `--synonyms-file PATH` | YAML/JSON dict of `{ field: [aliases...] }` |
| `--threshold FLOAT` | Mapping acceptance cutoff (default 0.7) |
| `--epsilon FLOAT` | Ambiguity window (default 0.05) |
| `--use-zscore/--use-iqr/--use-isoforest` | Toggle detectors (default: all on) |
| `--z-threshold FLOAT` | Z-Score threshold (default 3.0) |
| `--contamination FLOAT` | Isolation Forest contamination (default 0.01) |

### Outputs

- JSON report: `artifacts/assistant_report.json`
- Anomaly samples: `artifacts/anomalies/`

---

## Running Tests

```bash
cd backend

# All tests
python -m pytest -q tests/

# Semantic mapping only
python -m pytest -q tests/test_semantic_field_mapping.py
```

---

## Project Structure

```
lakehouse/
├── backend/           # FastAPI backend + analysis engine
│   ├── src/           # Source code (API, anomaly detection, schema, mapping)
│   ├── tests/         # Unit & integration tests
│   ├── scripts/       # CLI scripts & benchmarks
│   └── configs/       # Configuration files
├── frontend/          # React/Vite frontend
├── test/              # Benchmarking infrastructure
└── docker-compose.*.yml
```
