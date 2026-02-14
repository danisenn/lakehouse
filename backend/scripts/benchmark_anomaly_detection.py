#!/usr/bin/env python3
import sys
import os
import json
import pandas as pd
import polars as pl
from pathlib import Path
from collections import defaultdict
import time

# Add backend root to path to import src
BACKEND_ROOT = Path(__file__).resolve().parents[2] / "backend"
sys.path.append(str(BACKEND_ROOT))

# Also add the project root to find test/data
PROJECT_ROOT = Path(__file__).resolve().parents[2]

try:
    from src.anomaly_detection.utils import detect_anomalies
except ImportError:
    # Try alternate path if running from backend dir
    sys.path.append(str(Path.cwd()))
    from src.anomaly_detection.utils import detect_anomalies

def calculate_metrics(detected_rows, truth_rows, total_rows):
    detected_set = set(detected_rows)
    truth_set = set(truth_rows)
    
    tp = len(detected_set.intersection(truth_set))
    fp = len(detected_set - truth_set)
    fn = len(truth_set - detected_set)
    tn = total_rows - len(truth_set) - fp
    
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0.0
    
    return {
        "TP": tp, "FP": fp, "FN": fn,
        "Precision": precision, "Recall": recall, "F1": f1
    }

def run_benchmark(data_dir):
    results = []
    
    print(f"Scanning for benchmarks in {data_dir}...")
    
    for schema_dir in data_dir.iterdir():
        if not schema_dir.is_dir():
            continue
            
        print(f"\nSchema: {schema_dir.name}")
        
        # files look like {scenario}.csv and {scenario}_truth.json
        # find all json files to drive the process
        truth_files = list(schema_dir.glob("*_truth.json"))
        
        for truth_file in truth_files:
            scenario_name = truth_file.stem.replace("_truth", "")
            csv_file = schema_dir / f"{scenario_name}.csv"
            
            if not csv_file.exists():
                print(f"  Skipping {scenario_name}: CSV not found")
                continue
                
            print(f"  Running Scenario: {scenario_name}")
            
            # Load Data
            df_pl = pl.read_csv(str(csv_file))
            
            # Add Row ID to track indices across filtering
            df_pl = df_pl.with_row_index("_row_id")
            
            with open(truth_file) as f:
                truth_meta = json.load(f)
            
            # Aggregate Truth
            all_truth_indices = set()
            for error_type, details in truth_meta.items():
                if "rows" in details:
                    all_truth_indices.update(details["rows"])
            
            # Run Detection - Union of all methods
            detected_indices = set()
            
            # 1. Z-Score / IQR (Numeric)
            # We treat every numeric column as a candidate for outliers
            numeric_cols = df_pl.select(pl.col(pl.Int64, pl.Float64)).columns
            # exclude _row_id if it got picked up (UInt32 usually)
            numeric_cols = [c for c in numeric_cols if c != "_row_id"]

            for col in numeric_cols:
                # Z-Score
                try:
                    anoms_z = detect_anomalies(df_pl, method="zscore", columns=[col], threshold=3.0)
                    detected_indices.update(anoms_z["_row_id"].to_list())
                except Exception:
                    pass
                
                # IQR
                try:
                    anoms_iqr = detect_anomalies(df_pl, method="iqr", columns=[col])
                    detected_indices.update(anoms_iqr["_row_id"].to_list())
                except Exception:
                    pass

            # 2. Isolation Forest (Multivariate)
            if len(numeric_cols) > 1:
                try:
                    anoms_if = detect_anomalies(
                        df_pl, 
                        method="isolation_forest", 
                        columns=numeric_cols,
                        contamination=0.05 # Aggressive for testing
                    )
                    detected_indices.update(anoms_if["_row_id"].to_list())
                except Exception:
                    pass

            # Calculate Metrics
            metrics = calculate_metrics(detected_indices, all_truth_indices, len(df_pl))
            
            print(f"    Results: Precision={metrics['Precision']:.2f}, Recall={metrics['Recall']:.2f}, F1={metrics['F1']:.2f}")
            results.append({
                "schema": schema_dir.name,
                "scenario": scenario_name,
                **metrics
            })

    # Summary Table
    if results:
        df_res = pd.DataFrame(results)
        print("\n\n=== Final Benchmark Report ===")
        print(df_res.to_markdown(index=False))
        
        # Save to file
        df_res.to_csv(data_dir / "benchmark_results.csv", index=False)
        print(f"\nSaved results to {data_dir / 'benchmark_results.csv'}")
    else:
        print("\nNo results found.")

if __name__ == "__main__":
    DATA_DIR = Path("test/data/generated")
    if not DATA_DIR.exists():
        # Fallback for running from backend root
        DATA_DIR = PROJECT_ROOT / "test/data/generated"
    
    run_benchmark(DATA_DIR)
