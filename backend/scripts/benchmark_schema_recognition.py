#!/usr/bin/env python3
"""
Benchmark for Schema Recognition: Type Inference + Semantic Pattern Detection

Tests the system's ability to:
1. Infer correct data types (Boolean, Int, Float, Date, DateTime) from string columns
2. Detect semantic patterns (Email, Phone, UUID, URL, etc.)
"""

import sys
from pathlib import Path
import polars as pl
import pandas as pd
from typing import Dict, List

# Add backend root to path
BACKEND_ROOT = Path(__file__).resolve().parents[2] / "backend"
sys.path.append(str(BACKEND_ROOT))

try:
    from src.schema_recognition.inference.type_inference import refine_types
    from src.schema_recognition.inference.semantic import SemanticTypeDetector
except ImportError as e:
    print(f"Error: Could not import schema recognition modules: {e}")
    sys.exit(1)

def create_test_data():
    """
    Create a DataFrame with all string columns, but values that should be recognized as specific types.
    """
    data = {
        # Should be detected as Boolean
        "is_active": ["true", "false", "true", "true", "false"],
        "verified": ["1", "0", "1", "0", "1"],
        
        # Should be detected as Integer
        "age": ["25", "30", "45", "22", "35"],
        "count": ["100", "200", "150", "75", "300"],
        
        # Should be detected as Float
        "price": ["19.99", "29.50", "15.75", "99.99", "5.25"],
        "rating": ["4.5", "3.2", "5.0", "4.0", "3.8"],
        
        # Should be detected as Date
        "birth_date": ["1990-05-15", "1985-12-01", "2000-03-22", "1978-08-30", "1995-11-10"],
        
        # Should be detected as DateTime
        "created_at": [
            "2023-01-15 10:30:00",
            "2023-02-20 14:45:30",
            "2023-03-10 09:15:00",
            "2023-04-05 16:20:45",
            "2023-05-12 11:00:00"
        ],
        
        # Semantic patterns
        "email": ["john@example.com", "jane@test.org", "bob@company.co", "alice@mail.net", "charlie@domain.io"],
        "phone": ["+1-555-123-4567", "+1-555-987-6543", "+1-555-246-8135", "+1-555-369-2580", "+1-555-147-2589"],
        "website": ["https://example.com", "http://test.org", "https://company.co", "https://domain.io", "http://site.net"],
        "user_id": [
            "550e8400-e29b-41d4-a716-446655440000",
            "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
            "f47ac10b-58cc-4372-a567-0e02b2c3d479",
            "7c9e6679-7425-40de-944b-e07fc1f90ae7",
            "3b241101-e2bb-4255-8caf-4136c566a964"
        ],
        
        # Should stay as String (no clear pattern)
        "description": ["Product A", "Product B", "Product C", "Product D", "Product E"],
        "category": ["Electronics", "Clothing", "Food", "Books", "Toys"]
    }
    
    # Create DataFrame with all string types
    df = pl.DataFrame(data).with_columns(pl.all().cast(pl.Utf8))
    
    return df

def get_ground_truth():
    """
    Define the expected types for each column.
    """
    type_ground_truth = {
        "is_active": "Boolean",
        "verified": "Boolean",
        "age": "Int64",
        "count": "Int64",
        "price": "Float64",
        "rating": "Float64",
        "birth_date": "Date",
        "created_at": "Datetime", 
        "email": "Utf8",  # Type stays string, but semantic type is detected
        "phone": "Utf8",
        "website": "Utf8",
        "user_id": "Utf8",
        "description": "Utf8",
        "category": "Utf8"
    }
    
    semantic_ground_truth = {
        "email": "Email",
        "phone": "Phone",
        "website": "URL",
        "user_id": "UUID"
    }
    
    return type_ground_truth, semantic_ground_truth
    
def run_benchmark():
    print("=" * 60)
    print("Schema Recognition Benchmark")
    print("=" * 60)
    
    # Create test data
    df_original = create_test_data()
    type_gt, semantic_gt = get_ground_truth()
    
    print(f"\n📊 Test Data: {df_original.height} rows, {len(df_original.columns)} columns")
    print(f"   All columns initially stored as: Utf8 (String)")
    
    # ============================================
    # Part 1: Type Inference
    # ============================================
    print("\n" + "=" * 60)
    print("Part 1: Type Inference (String → Proper Types)")
    print("=" * 60)
    
    df_inferred = refine_types(df_original)
    
    # Evaluate
    type_results = []
    for col in df_original.columns:
        expected = type_gt.get(col, "Utf8")
        actual = str(df_inferred[col].dtype)
        
        # Normalize dtype names (Polars uses different names sometimes)
        if "Datetime" in actual:
            actual = "Datetime"
        
        correct = expected == actual
        
        type_results.append({
            "Column": col,
            "Expected": expected,
            "Inferred": actual,
            "Correct": "✅" if correct else "❌"
        })
        
        print(f"  {col:20s} | Expected: {expected:10s} | Got: {actual:10s} | {type_results[-1]['Correct']}")
    
    # Calculate accuracy
    correct_count = sum(1 for r in type_results if r["Correct"] == "✅")
    type_accuracy = correct_count / len(type_results)
    
    print(f"\n🎯 Type Inference Accuracy: {type_accuracy:.1%} ({correct_count}/{len(type_results)})")
    
    # ============================================
    # Part 2: Semantic Pattern Detection
    # ============================================
    print("\n" + "=" * 60)
    print("Part 2: Semantic Pattern Detection")
    print("=" * 60)
    
    detector = SemanticTypeDetector()
    detected_patterns = detector.detect(df_original)
    
    # Evaluate
    semantic_results = []
    all_semantic_cols = set(semantic_gt.keys())
    
    for col in all_semantic_cols:
        expected = semantic_gt.get(col)
        detected = detected_patterns.get(col)
        
        correct = expected == detected
        
        semantic_results.append({
            "Column": col,
            "Expected": expected,
            "Detected": detected or "(None)",
            "Correct": "✅" if correct else "❌"
        })
        
        print(f"  {col:20s} | Expected: {expected:15s} | Detected: {str(detected or '(None)'):15s} | {semantic_results[-1]['Correct']}")
    
    # Calculate metrics
    tp = sum(1 for r in semantic_results if r["Detected"] != "(None)" and r["Correct"] == "✅")
    fp = sum(1 for col in detected_patterns if col not in semantic_gt)
    fn = sum(1 for r in semantic_results if r["Detected"] == "(None)")
    
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0.0
    
    print(f"\n🎯 Semantic Detection Performance:")
    print(f"   Precision: {precision:.1%}")
    print(f"   Recall:    {recall:.1%}")
    print(f"   F1-Score:  {f1:.1%}")
    
    # ============================================
    # Summary Report
    # ============================================
    print("\n" + "=" * 60)
    print("Summary Report")
    print("=" * 60)
    
    summary = {
        "Component": ["Type Inference", "Semantic Detection"],
        "Accuracy/F1": [f"{type_accuracy:.1%}", f"{f1:.1%}"],
        "Correct/TP": [f"{correct_count}/{len(type_results)}", f"{tp}/{len(semantic_gt)}"],
        "Notes": [
            f"{len(type_results) - correct_count} type mismatches",
            f"FP={fp}, FN={fn}"
        ]
    }
    
    df_summary = pd.DataFrame(summary)
    print(df_summary.to_markdown(index=False))
    
    # Save results
    out_dir = Path("test/data/generated")
    out_dir.mkdir(parents=True, exist_ok=True)
    
    df_summary.to_csv(out_dir / "benchmark_schema_recognition.csv", index=False)
    print(f"\n💾 Saved results to {out_dir / 'benchmark_schema_recognition.csv'}")

if __name__ == "__main__":
    run_benchmark()
