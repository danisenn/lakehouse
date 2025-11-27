# Lakehouse Assistant Backend

## Prerequisites
- Python 3.10+
- pip

## Installation

```bash
pip install -r requirements.txt
```

## Configuration

Ensure you have a `.env` file in the `backend` directory if needed for environment variables (e.g. `DREMIO_CONNECTION_URL`).

## Running the Server

From the `backend` directory:

```bash
uvicorn src.api.app:app --reload
```

The server will start at `http://localhost:8000`.
API Documentation is available at `http://localhost:8000/docs`.
