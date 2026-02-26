"""
Compute ground truth schema and semantic types from a CSV/Parquet/JSON file.

This script runs with the backend on sys.path only, avoiding the test/src vs
backend/src namespace collision.

Usage:
    python compute_ground_truth.py <path_to_csv_or_parquet>

Outputs JSON to stdout with keys: original_schema, expected_semantic_types
"""
import sys
import json
from pathlib import Path

# Only backend on sys.path
BACKEND_DIR = Path(__file__).resolve().parent.parent.parent / "backend"
sys.path.insert(0, str(BACKEND_DIR))

import polars as pl
from src.schema_recognition.inference.type_inference import refine_types
from src.schema_recognition.inference.semantic import SemanticTypeDetector


def main():
    if len(sys.argv) < 2:
        print(json.dumps({"error": "No input file provided"}))
        sys.exit(1)

    input_path = sys.argv[1]
    
    # Read data
    if input_path.endswith(".csv"):
        df = pl.read_csv(input_path, infer_schema_length=1000, truncate_ragged_lines=True, ignore_errors=True)
    elif input_path.endswith(".parquet"):
        df = pl.read_parquet(input_path)
    elif input_path.endswith(".json"):
        df = pl.read_json(input_path)
    else:
        # Try CSV
        df = pl.read_csv(input_path, infer_schema_length=1000, truncate_ragged_lines=True, ignore_errors=True)

    # Refine types
    df = refine_types(df)
    schema = {str(name): str(dtype) for name, dtype in df.schema.items()}

    # Detect semantic types
    detector = SemanticTypeDetector()
    semantic_types = detector.detect(df)

    result = {
        "original_schema": schema,
        "expected_semantic_types": semantic_types,
    }

    print(json.dumps(result))


if __name__ == "__main__":
    main()
