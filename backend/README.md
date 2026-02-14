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

## Running the Server

From the `backend` directory (important: you must be in the backend folder so that `src` is found):

```bash
cd backend
uvicorn src.api.app:app --reload
```

The server will start at `http://localhost:8000`.
API Documentation is available at `http://localhost:8000/docs`.

## Benchmarking (Thesis Experiments)

To reproduce the results for the thesis, use the included benchmarking scripts.

### 1. Semantic Mapping Benchmark
Evaluates the `SemanticFieldMapper` across different scenarios (Exact, Case, Synonym, Fuzzy, etc.).

```bash
# Run from repository root or backend folder
python3 backend/scripts/benchmark_mapping.py
```

### 2. Anomaly Detection Benchmark
Evaluates the detection rates (Precision/Recall) against synthetic ground truth.

**Step A: Generate Test Data**
```bash
# From repository root
python3 test/src/pipeline.py --format csv
```
This will create generated datasets and truth files in `test/data/generated`.

**Step B: Run Benchmark Score**
```bash
# From repository root or backend folder
python3 backend/scripts/benchmark_anomaly_detection.py
```
This compares the system's output against the generated ground truth and saves `benchmark_results.csv`.

### 3. Schema Recognition Benchmark
Evaluates type inference (String→Int/Float/Date/Boolean) and semantic pattern detection (Email/Phone/UUID).

```bash
# From repository root or backend folder
python3 backend/scripts/benchmark_schema_recognition.py
```
This tests the system's ability to automatically recognize data types and semantic patterns from raw string data.
