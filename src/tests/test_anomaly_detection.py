#!/usr/bin/env python3
"""
Test script to run anomaly detection on CSV files in the data folder.

It scans a root directory (default: ./data) for *.csv files, loads each file with Polars,
selects numeric columns, and runs three detectors:
- Z-Score (per numeric column)
- IQR (per numeric column)
- Isolation Forest (across all numeric columns)

Outputs a console summary and can optionally save anomaly rows as CSVs.

Usage examples:
  python scripts/test_anomaly_detection.py
  python scripts/test_anomaly_detection.py --root data/archive --max-rows 50000 --save --out-dir artifacts/anomalies
  python scripts/test_anomaly_detection.py --z-threshold 2.5 --contamination 0.02 --verbose

Requirements at runtime:
  - polars, numpy, scikit-learn

Note: The script adjusts sys.path to import from the local 'src' package.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
import time
from typing import Dict, List, Optional

# Ensure we can import from 'src' when script is run directly
PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import polars as pl

from src.anomaly_detection.utils import (
    detect_anomalies,
    select_numeric_columns,
)


def find_csv_files(root: Path) -> List[Path]:
    return sorted(p for p in root.rglob("*.csv") if p.is_file())


def safe_read_csv(path: Path, max_rows: Optional[int]) -> pl.DataFrame:
    read_kwargs = dict(ignore_errors=True)
    if max_rows and max_rows > 0:
        read_kwargs["n_rows"] = int(max_rows)
    # Low-memory friendly options
    read_kwargs["rechunk"] = True
    return pl.read_csv(path, **read_kwargs)


def save_df(df: pl.DataFrame, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    # Polars write_csv expects a path-like str
    df.write_csv(str(out_path))


def pretty_num(n: int) -> str:
    return f"{n:,}".replace(",", "_")


def run_detectors_on_df(
    df: pl.DataFrame,
    file_name: str,
    z_threshold: float,
    contamination: float,
    n_estimators: int,
    random_state: int,
    verbose: bool = False,
) -> Dict:
    report: Dict = {
        "file": file_name,
        "rows": df.height,
        "cols": df.width,
        "numeric_columns": [],
        "results": {
            "zscore": {},
            "iqr": {},
            "isolation_forest": {},
        },
        "timing_sec": {},
    }

    numeric_cols = select_numeric_columns(df)
    report["numeric_columns"] = numeric_cols

    # Per-column rule-based detectors
    for method in ("zscore", "iqr"):
        method_results = {}
        for col in numeric_cols:
            t0 = time.time()
            try:
                if method == "zscore":
                    anomalies = detect_anomalies(df, method="zscore", columns=[col], threshold=z_threshold)
                else:
                    anomalies = detect_anomalies(df, method="iqr", columns=[col])
                count = anomalies.height
                sample_preview = anomalies.head(3).to_dicts()
                method_results[col] = {
                    "count": int(count),
                    "preview": sample_preview,
                }
                if verbose:
                    print(f"  [{method}] {col}: {count} anomalies")
            except Exception as e:
                method_results[col] = {"error": str(e)}
            finally:
                report["timing_sec"].setdefault(method, 0.0)
                report["timing_sec"][method] += (time.time() - t0)
        report["results"][method] = method_results

    # Isolation Forest across all numeric columns (if any)
    if numeric_cols:
        t0 = time.time()
        try:
            anomalies_if = detect_anomalies(
                df,
                method="isolation_forest",
                columns=numeric_cols,
                contamination=contamination,
                n_estimators=n_estimators,
                random_state=random_state,
            )
            count_if = anomalies_if.height
            report["results"]["isolation_forest"] = {
                "columns": numeric_cols,
                "count": int(count_if),
                "preview": anomalies_if.head(5).to_dicts(),
            }
            if verbose:
                print(f"  [isolation_forest] cols={len(numeric_cols)} -> {count_if} anomalies")
        except Exception as e:
            report["results"]["isolation_forest"] = {"error": str(e), "columns": numeric_cols}
        finally:
            report["timing_sec"]["isolation_forest"] = (time.time() - t0)
    else:
        report["results"]["isolation_forest"] = {"skipped": True, "reason": "no numeric columns"}

    return report


def main():
    parser = argparse.ArgumentParser(description="Test anomaly detection on CSV files in a data folder.")
    parser.add_argument("--root", type=str, default=str(PROJECT_ROOT / "data"), help="Root folder to scan for CSV files (recursive). Default: ./data")
    parser.add_argument("--max-rows", type=int, default=0, help="If >0, limit the number of rows read from each CSV.")
    parser.add_argument("--z-threshold", type=float, default=3.0, help="Z-score threshold for per-column detection.")
    parser.add_argument("--contamination", type=float, default=0.01, help="Expected proportion of anomalies for Isolation Forest.")
    parser.add_argument("--n-estimators", type=int, default=100, help="Number of trees for Isolation Forest.")
    parser.add_argument("--random-state", type=int, default=42, help="Random seed for Isolation Forest.")
    parser.add_argument("--save", action="store_true", help="If set, save anomalies to CSV files in --out-dir.")
    parser.add_argument("--out-dir", type=str, default=str(PROJECT_ROOT / "artifacts" / "anomalies"), help="Output directory for saved anomaly CSVs.")
    parser.add_argument("--json-report", type=str, default="", help="Optional path to write a JSON summary report.")
    parser.add_argument("--verbose", action="store_true", help="Verbose per-file output.")

    args = parser.parse_args()

    root = Path(args.root).resolve()
    out_dir = Path(args.out_dir).resolve()

    if not root.exists():
        print(f"Root folder not found: {root}")
        sys.exit(1)

    csv_files = find_csv_files(root)
    if not csv_files:
        print(f"No CSV files found under: {root}")
        sys.exit(0)

    print(f"Scanning {len(csv_files)} CSV file(s) under {root}\n")

    all_reports: List[Dict] = []

    for path in csv_files:
        rel = path.relative_to(PROJECT_ROOT)
        print(f"File: {rel}")
        try:
            df = safe_read_csv(path, args.max_rows)
        except Exception as e:
            print(f"  Error reading CSV: {e}")
            all_reports.append({"file": str(rel), "error": str(e)})
            continue

        if df.is_empty():
            print("  Skipped: empty DataFrame")
            all_reports.append({"file": str(rel), "rows": 0, "cols": 0, "skipped": "empty"})
            continue

        report = run_detectors_on_df(
            df=df,
            file_name=str(rel),
            z_threshold=args.z_threshold,
            contamination=args.contamination,
            n_estimators=args.n_estimators,
            random_state=args.random_state,
            verbose=args.verbose,
        )

        # Optional saving of anomalies
        if args.save:
            numeric_cols = report.get("numeric_columns", [])
            # Save per-column rule-based results
            for method in ("zscore", "iqr"):
                percol = report["results"].get(method, {})
                for col, res in percol.items():
                    if isinstance(res, dict) and res.get("count", 0) > 0:
                        anomalies = detect_anomalies(
                            df, method=method, columns=[col], threshold=args.z_threshold
                        ) if method == "zscore" else detect_anomalies(df, method=method, columns=[col])
                        out_path = out_dir / method / rel.parent / f"{path.stem}__{method}__{col}.csv"
                        save_df(anomalies, out_path)
            # Save isolation forest results if any
            if numeric_cols:
                try:
                    anomalies_if = detect_anomalies(
                        df,
                        method="isolation_forest",
                        columns=numeric_cols,
                        contamination=args.contamination,
                        n_estimators=args.n_estimators,
                        random_state=args.random_state,
                    )
                    if anomalies_if.height > 0:
                        out_path = out_dir / "isolation_forest" / rel.parent / f"{path.stem}__isolation_forest.csv"
                        save_df(anomalies_if, out_path)
                except Exception:
                    # Already reported in report; ignore for saving
                    pass

        # Console summary
        num_cols = len(report.get("numeric_columns", []))
        z_total = sum((v.get("count", 0) for v in report["results"].get("zscore", {}).values() if isinstance(v, dict)))
        iqr_total = sum((v.get("count", 0) for v in report["results"].get("iqr", {}).values() if isinstance(v, dict)))
        if_res = report["results"].get("isolation_forest", {})
        if_count = int(if_res.get("count", 0)) if isinstance(if_res, dict) else 0
        print(
            f"  Summary: rows={pretty_num(df.height)}, cols={df.width}, numeric_cols={num_cols}, "
            f"zscore_total={z_total}, iqr_total={iqr_total}, isolation_forest={if_count}"
        )

        all_reports.append(report)

    # Optional JSON report
    if args.json_report:
        json_path = Path(args.json_report)
        json_path.parent.mkdir(parents=True, exist_ok=True)
        with json_path.open("w", encoding="utf-8") as f:
            json.dump(all_reports, f, indent=2)
        print(f"\nWrote JSON report: {json_path}")

    print("\nDone.")


if __name__ == "__main__":
    main()
