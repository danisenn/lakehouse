#!/usr/bin/env python3
import sys
import time
import pandas as pd
import polars as pl
import numpy as np
from pathlib import Path
from collections import defaultdict

# Add backend root to path
BACKEND_ROOT = Path(__file__).resolve().parents[2] / "backend"
sys.path.append(str(BACKEND_ROOT))

# Also add current dir for local imports if needed
sys.path.append(str(Path.cwd()))

try:
    from src.semantic_field_mapping import SemanticFieldMapper
except ImportError:
    print("Error: Could not import SemanticFieldMapper. Check python path.")
    sys.exit(1)

# Configuration
REFERENCE_FIELDS = ["email", "phone", "first_name", "last_name", "address", "city", "zip_code", "country", "birth_date"]

# Synonym Dictionary (Simulated Domain Knowledge)
SYNONYMS = {
    "email": ["mail", "e-mail", "email_addr", "contact_mail"],
    "phone": ["telephone", "mobile", "cell", "contact_number"],
    "zip_code": ["postal_code", "zip", "postcode"],
    "birth_date": ["dob", "date_of_birth", "birthday"]
}

SCENARIOS = {
    "exact": lambda x: x,
    "case_upper": lambda x: x.upper(),
    "case_mixed": lambda x: x.title(),
    "synonym": lambda x: np.random.choice(SYNONYMS.get(x, [x])),
    "prefix": lambda x: f"user_{x}",
    "suffix": lambda x: f"{x}_val",
    "fuzzy_typo": lambda x: x[:-1] + "x" if len(x) > 3 else x, # simple typo
}

def generate_test_df(columns):
    # Create empty DF with columns
    data = {c: ["dummy"] for c in columns}
    return pl.DataFrame(data)

def run_benchmark():
    results = []
    
    print(f"Benchmarking SemanticFieldMapper...")
    print(f"Reference Fields: {REFERENCE_FIELDS}")
    
    # Initialize Mapper
    mapper = SemanticFieldMapper(
        reference_fields=REFERENCE_FIELDS,
        synonyms=SYNONYMS,
        threshold=0.7 # Thesis default
    )
    
    for scenario_name, transform_func in SCENARIOS.items():
        print(f"\nScenario: {scenario_name}")
        
        # Generate Test Case
        # We take the reference fields and apply the transformation to create "Source Columns"
        # The "Ground Truth" is that transformed_col maps back to original_col
        
        source_cols = []
        ground_truth = {}
        
        for ref in REFERENCE_FIELDS:
            # Skip synonym if not available
            if scenario_name == 'synonym' and ref not in SYNONYMS:
                continue
                
            src_col = transform_func(ref)
            source_cols.append(src_col)
            ground_truth[src_col] = ref
            
        # Create DataFrame
        df = generate_test_df(source_cols)
        
        # Run Mapping
        start_time = time.time()
        result = mapper.map_columns(df)
        duration = time.time() - start_time
        
        # Evaluate
        tp, fp, fn = 0, 0, 0
        ambiguous_count = len(result.get("ambiguous", {}))
        
        # 1. Check Matches
        mapped_dict = result.get("mapping", {})
        
        for src, mapping_info in mapped_dict.items():
            predicted_target = mapping_info["target"]
            # Correct?
            if src in ground_truth and ground_truth[src] == predicted_target:
                tp += 1
            else:
                fp += 1
        
        # 2. Check Misses (False Negatives)
        # Anything in ground truth that was NOT mapped is a FN
        # (unless it was flagged as ambiguous, which is a partial success or separate category)
        for src in ground_truth:
            if src not in mapped_dict:
                fn += 1
                
        # Calculate Metrics
        precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
        f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0.0
        
        print(f"  TP={tp}, FP={fp}, FN={fn}, Ambiguous={ambiguous_count}")
        print(f"  Precision={precision:.2f}, Recall={recall:.2f}, F1={f1:.2f}")
        print(f"  Avg Latency per Col: {(duration / len(source_cols) * 1000):.2f} ms")
        
        results.append({
            "Scenario": scenario_name,
            "Precision": precision,
            "Recall": recall,
            "F1": f1,
            "AvgLatencyMs": (duration / len(source_cols) * 1000),
            "Ambiguous": ambiguous_count
        })

    # Summary
    df_res = pd.DataFrame(results)
    print("\n\n=== Mapping Benchmark Results ===")
    print(df_res.to_markdown(index=False))
    
    # Save
    out_path = Path("test/data/generated/benchmark_mapping.csv")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df_res.to_csv(out_path, index=False)
    print(f"\nSaved to {out_path}")

if __name__ == "__main__":
    run_benchmark()
