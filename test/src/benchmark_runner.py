"""
End-to-End Benchmark Runner

Orchestrates quality variant generation, backend analysis, and performance
evaluation for anomaly detection, schema recognition, and semantic mapping.
"""
import os
import sys
import json
import argparse
import subprocess
from pathlib import Path
from typing import Dict, List, Any, Set

# Add roots to sys.path for direct imports
ROOT_DIR = Path(__file__).resolve().parent.parent.parent
test_src_path = ROOT_DIR / "test" / "src"
backend_src_path = ROOT_DIR / "backend"

# Insert to front of path
for p in [backend_src_path, test_src_path]:
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

# Direct imports from their respective roots
from connection.dremio_api import DremioAPI
from src.assistant.runner import run_assistant, MappingConfig, AnomalyConfig
from src.assistant.datasource import LakehouseSQLDataSource
from src.connection.connection import get_connection


# ── Helpers ──────────────────────────────────────────────────────────────

def run_generation(table_name: str, source_schema: str, target_schema: str, limit: int = None):
    """Run the batch_quality_generator script."""
    print(f"\n--- Phase 1: Generating Quality Variants for {table_name} ---")
    
    generator_path = test_src_path / "batch_quality_generator.py"
    
    cmd = [
        sys.executable,
        str(generator_path),
        "--table", table_name,
        "--schema", source_schema,
        "--target-schema", target_schema
    ]
    if limit:
        cmd.extend(["--limit", str(limit)])
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Generation failed:\n{result.stderr}")
        return False
    print(result.stdout)
    return True


def load_ground_truth(table_name: str, scenario: str) -> Dict[str, Any]:
    """Load the ground truth JSON for a specific variant."""
    truth_dir = ROOT_DIR / "test" / "data" / "benchmarks"
    clean_name = table_name.replace(".csv", "").replace(".json", "").replace(".parquet", "")
    truth_file = truth_dir / f"{clean_name}_{scenario}_truth.json"
    
    if not truth_file.exists():
        print(f"Warning: Ground truth file not found: {truth_file}")
        return None
    
    with open(truth_file, "r") as f:
        return json.load(f)


def run_analysis(target_schema: str, table_name: str, reference_fields: List[str] = None):
    """Run backend assistant analysis on a promoted dataset."""
    print(f"Analyzing {target_schema}.{table_name}...")
    conn = get_connection()
    
    # Build fully-quoted SQL path
    schema_parts = target_schema.split(".")
    full_path = ".".join(f'"{p}"' for p in schema_parts + [table_name])
    
    source = LakehouseSQLDataSource(
        connection_uri=conn,
        query=f"SELECT * FROM {full_path}",
        name=table_name
    )

    # Use original column names as reference_fields for semantic mapping evaluation
    if reference_fields is None or len(reference_fields) == 0:
        reference_fields = ["_benchmark_placeholder"]

    mapping_cfg = MappingConfig(reference_fields=reference_fields)
    anomaly_cfg = AnomalyConfig(
        use_iqr=True,
        use_zscore=True,
        use_isolation_forest=True,
        use_missing_values=True
    )

    report = run_assistant(source, mapping_cfg, anomaly_cfg)
    return report


# ── Evaluation Functions ─────────────────────────────────────────────────

TYPE_EQUIVALENCES = {
    # Map similar types to allow fuzzy matching
    "Int64": {"Int64", "Int32", "Int16", "Int8", "UInt64", "UInt32"},
    "Float64": {"Float64", "Float32"},
    "Utf8": {"Utf8", "String", "LargeUtf8"},
    "Boolean": {"Boolean"},
    "Date": {"Date", "Datetime"},
}


def _types_match(expected: str, inferred: str) -> bool:
    """Check if two Polars type strings are equivalent."""
    if expected == inferred:
        return True
    # Check equivalence classes
    for _base, equivalents in TYPE_EQUIVALENCES.items():
        if expected in equivalents and inferred in equivalents:
            return True
    return False


def evaluate_anomalies(detected_rows: Dict[str, List[int]], ground_truth: Dict[str, Any]) -> Dict[str, Any]:
    """Compare detected anomaly indices with ground truth."""
    expected_rows = set()
    degradations = ground_truth.get("degradation_metadata", {})
    for task, meta in degradations.items():
        rows = meta.get("rows")
        if rows is None:
            rows = meta.get("affected_rows", [])
        expected_rows.update(rows)
    
    all_detected = set()
    for method, rows in detected_rows.items():
        all_detected.update(rows)
    
    tp = len(all_detected.intersection(expected_rows))
    fp = len(all_detected - expected_rows)
    fn = len(expected_rows - all_detected)
    
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0
    f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0
    
    return {
        "tp": tp, "fp": fp, "fn": fn,
        "precision": precision, "recall": recall, "f1": f1,
        "total_expected": len(expected_rows),
        "total_detected": len(all_detected)
    }


def evaluate_schema(inferred_schema: Dict[str, str], ground_truth: Dict[str, Any]) -> Dict[str, Any]:
    """
    Evaluate schema recognition accuracy.
    
    Compares inferred column types against the original types from the clean data.
    """
    original_schema = ground_truth.get("original_schema", {})
    if not original_schema:
        return {"accuracy": None, "detail": "No original_schema in ground truth"}
    
    total = 0
    correct = 0
    mismatches = {}
    
    for col, expected_type in original_schema.items():
        inferred_type = inferred_schema.get(col, "missing")
        total += 1
        if _types_match(expected_type, inferred_type):
            correct += 1
        else:
            mismatches[col] = {"expected": expected_type, "inferred": inferred_type}
    
    accuracy = correct / total if total > 0 else 0
    return {
        "accuracy": accuracy,
        "correct": correct,
        "total": total,
        "mismatches": mismatches
    }


def evaluate_semantic_types(detected_types: Dict[str, str], ground_truth: Dict[str, Any]) -> Dict[str, Any]:
    """
    Evaluate semantic type detection (Email, URL, Price, etc.) using P/R/F1.
    
    Ground truth: expected semantic type assignments from clean data.
    Predicted: semantic types detected by SemanticTypeDetector on degraded data.
    """
    expected_types = ground_truth.get("expected_semantic_types", {})
    
    # If no semantic types are expected (e.g., all columns are plain text),
    # consider it a perfect score if none were detected
    if not expected_types:
        if not detected_types:
            return {"precision": 1.0, "recall": 1.0, "f1": 1.0, "detail": "No semantic types expected or detected"}
        else:
            return {"precision": 0.0, "recall": 1.0, "f1": 0.0, "detail": "False positives detected but none expected"}
    
    # True positive = column detected with correct type
    # False positive = column detected but wrong type or not in expected
    # False negative = column expected but not detected
    tp = 0
    fp = 0
    details = {}
    
    for col, detected_type in detected_types.items():
        expected = expected_types.get(col)
        if expected and expected == detected_type:
            tp += 1
        else:
            fp += 1
            details[col] = {"detected": detected_type, "expected": expected or "none"}
    
    fn = sum(1 for col in expected_types if col not in detected_types)
    
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0
    f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0
    
    return {
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "tp": tp, "fp": fp, "fn": fn,
        "mismatches": details
    }


def evaluate_mapping(mapping_result: Dict, ground_truth: Dict[str, Any]) -> Dict[str, Any]:
    """
    Evaluate semantic field mapping accuracy.
    
    Uses the original column names as reference fields. A correct mapping is
    when a column maps to itself (identity mapping test).
    """
    columns = ground_truth.get("columns", [])
    if not columns:
        return {"accuracy": None, "detail": "No columns in ground truth"}
    
    mappings = mapping_result.get("mapping", {})
    unmapped = mapping_result.get("unmapped", [])
    ambiguous = mapping_result.get("ambiguous", {})
    
    total = len(columns)
    correct = 0
    mismatches = {}
    
    for col in columns:
        if col in mappings:
            target = mappings[col].get("target", "")
            if target == col:
                correct += 1
            else:
                mismatches[col] = {"mapped_to": target}
        elif col in ambiguous:
            # Ambiguous mapping — check if correct target is among candidates
            candidates = [c.get("target", "") for c in ambiguous[col]]
            if col in candidates:
                correct += 0.5  # Partial credit for ambiguous but present
            mismatches[col] = {"ambiguous_candidates": candidates}
        else:
            mismatches[col] = {"status": "unmapped"}
    
    accuracy = correct / total if total > 0 else 0
    return {
        "accuracy": accuracy,
        "correct": correct,
        "total": total,
        "unmapped": len(unmapped),
        "ambiguous": len(ambiguous),
        "mismatches": mismatches
    }


# ── Main Pipeline ────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="End-to-End Benchmarking Runner")
    parser.add_argument("--table", help="Table name to benchmark")
    parser.add_argument("--all", action="store_true", help="Benchmark all tables in the schema")
    parser.add_argument("--schema", default="lakehouse.datalake.raw", help="Source schema")
    parser.add_argument("--target-schema", default="lakehouse.datalake.benchmark_test", help="Target benchmark schema")
    parser.add_argument("--limit", type=int, help="Row limit for testing")
    parser.add_argument("--skip-generation", action="store_true", help="Skip variant generation")
    
    args = parser.parse_args()
    
    if not args.table and not args.all:
        parser.error("Either --table or --all must be specified")
        
    if args.all:
        print(f"\nDiscovering tables in {args.schema}...")
        try:
            from lakehouse_loader import list_tables
            tables = list_tables(args.schema)
            if not tables:
                print(f"No tables found in {args.schema}")
                return
            print(f"Found {len(tables)} tables: {tables}")
        except Exception as e:
            print(f"Could not list tables from {args.schema}: {e}")
            return
    else:
        tables = [args.table]

    for table_name in tables:
        print(f"\n{'='*80}")
        print(f" BENCHMARKING TABLE: {table_name}")
        print(f"{'='*80}")
        
        try:
            process_table(table_name, args)
        except Exception as e:
            print(f"Error processing {table_name}: {e}")
            import traceback
            traceback.print_exc()

def process_table(table_name: str, args):
    
    clean_name = table_name.replace(".csv", "").replace(".json", "").replace(".parquet", "")
    scenarios = ["high_quality", "medium_quality", "low_quality"]
    
    if not args.skip_generation:
        # Phase 0: Cleanup existing benchmark datasets in Dremio
        print(f"\n--- Phase 0: Cleaning up existing benchmark datasets ---")
        api = DremioAPI()
        if api.login():
            ext = ""
            if table_name.endswith(".csv"): ext = ".csv"
            elif table_name.endswith(".parquet"): ext = ".parquet"
            elif table_name.endswith(".json"): ext = ".json"
            
            target_schema_parts = args.target_schema.split(".")
            for scenario in scenarios:
                variant_table = f"{clean_name}_{scenario}{ext}"
                path = target_schema_parts + [variant_table]
                api.delete_dataset(path)

        # 1. Generate Variants
        if not run_generation(table_name, args.schema, args.target_schema, args.limit):
            return
    else:
        print("\n--- Skipping generation (--skip-generation) ---")

    # 2. Benchmark each variant
    results = {}
    
    print("\n--- Phase 2: Running Analysis & Evaluation ---")
    
    # Detect extension from source table
    ext = ""
    if table_name.endswith(".csv"): ext = ".csv"
    elif table_name.endswith(".parquet"): ext = ".parquet"
    elif table_name.endswith(".json"): ext = ".json"

    for scenario in scenarios:
        variant_table = f"{clean_name}_{scenario}{ext}"
        print(f"\nEvaluating variant: {variant_table}")
        
        # Load truth
        truth = load_ground_truth(table_name, scenario)
        if not truth:
            continue
        
        # Use original column names as reference fields for mapping evaluation
        reference_fields = truth.get("columns", [])
        
        # Run backend analysis
        report = run_analysis(args.target_schema, variant_table, reference_fields=reference_fields)
        if not report.datasets:
            print(f"Error: No analysis results for {variant_table}")
            continue
            
        ds_report = report.datasets[0]
        print(f"    - Rows: {ds_report.rows}, Cols: {ds_report.cols}")
        print(f"    - Detections: {ds_report.anomalies}")
        print(f"    - Schema: {ds_report.schema}")
        print(f"    - Semantic Types: {ds_report.semantic_types}")
        
        # Evaluate all three dimensions
        anomaly_metrics = evaluate_anomalies(ds_report.anomaly_rows, truth)
        schema_metrics = evaluate_schema(ds_report.schema, truth)
        semantic_metrics = evaluate_semantic_types(ds_report.semantic_types, truth)
        # DatasetReport stores mapping/ambiguous/unmapped as separate fields
        full_mapping_result = {
            "mapping": ds_report.mapping,
            "ambiguous": ds_report.ambiguous,
            "unmapped": ds_report.unmapped,
        }
        mapping_metrics = evaluate_mapping(full_mapping_result, truth)
        
        results[scenario] = {
            "anomaly": anomaly_metrics,
            "schema": schema_metrics,
            "semantic": semantic_metrics,
            "mapping": mapping_metrics,
        }
        
    # 3. Print Final Report
    print("\n" + "="*110)
    print(f"BENCHMARK REPORT: {table_name}")
    print("="*110)
    
    # Header
    print(f"{'Scenario':<20} | {'Anom P':<8} | {'Anom R':<8} | {'Anom F1':<8} | {'Schema':<8} | {'Sem F1':<8} | {'Map Acc':<8}")
    print("-" * 110)
    
    for scenario, data in results.items():
        a = data["anomaly"]
        s = data["schema"]
        sem = data["semantic"]
        m = data["mapping"]
        
        schema_acc = f"{s['accuracy']:.2%}" if s.get("accuracy") is not None else "N/A"
        sem_f1 = f"{sem['f1']:.2%}" if sem.get("f1") is not None else "N/A"
        map_acc = f"{m['accuracy']:.2%}" if m.get("accuracy") is not None else "N/A"
        
        print(f"{scenario:<20} | {a['precision']:<8.2%} | {a['recall']:<8.2%} | {a['f1']:<8.2%} | {schema_acc:<8} | {sem_f1:<8} | {map_acc:<8}")
    
    print("="*110)
    
    # Detailed breakdown
    for scenario, data in results.items():
        print(f"\n--- {scenario} Details ---")
        
        # Schema mismatches
        if data["schema"].get("mismatches"):
            print(f"  Schema mismatches ({len(data['schema']['mismatches'])}):")
            for col, info in data["schema"]["mismatches"].items():
                print(f"    {col}: expected={info['expected']}, inferred={info['inferred']}")
        
        # Semantic type mismatches
        if data["semantic"].get("mismatches"):
            print(f"  Semantic type mismatches ({len(data['semantic']['mismatches'])}):")
            for col, info in data["semantic"]["mismatches"].items():
                print(f"    {col}: detected={info['detected']}, expected={info['expected']}")
        
        # Mapping mismatches (show first 5)
        if data["mapping"].get("mismatches"):
            mismatches = data["mapping"]["mismatches"]
            print(f"  Mapping mismatches ({len(mismatches)}):")
            for col, info in list(mismatches.items())[:5]:
                print(f"    {col}: {info}")
            if len(mismatches) > 5:
                print(f"    ... and {len(mismatches) - 5} more")
    
    # Save full results JSON
    results_path = ROOT_DIR / "test" / "data" / "benchmarks" / f"{clean_name}_benchmark_results.json"
    with open(results_path, "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\nFull results saved to: {results_path}")


if __name__ == "__main__":
    main()
