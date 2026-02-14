import os
import json
import shutil
from pathlib import Path
import yaml
import pandas as pd
import numpy as np
import random
import argparse
from datetime import datetime, timedelta
import sys
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Ensure src modules can be imported
# Add the parent directory of 'src' (which is 'lakehouse/test') to sys.path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from src.storage.upload import upload_df_to_minio
from src.generators.combined_scenarios import apply_combined_scenario

def load_config(path):
    with open(path, 'r') as f:
        return yaml.safe_load(f)

def load_data_from_file(file_path):
    """Load data from CSV or Parquet file."""
    if file_path.endswith('.csv'):
        return pd.read_csv(file_path)
    elif file_path.endswith('.parquet'):
        return pd.read_parquet(file_path)
    else:
        raise ValueError(f"Unsupported file format: {file_path}. Use .csv or .parquet")

def generate_base_data(schema, n_rows=1000):
    data = {}
    for col, dtype in schema['columns'].items():
        if dtype == 'int':
            data[col] = [random.randint(1, 1000) for _ in range(n_rows)]
        elif dtype == 'float':
            data[col] = [random.uniform(10.0, 1000.0) for _ in range(n_rows)]
        elif dtype == 'string':
            data[col] = [random.choice(['A', 'B', 'C', 'D']) for _ in range(n_rows)]
        elif dtype == 'datetime':
            base_date = datetime.now()
            data[col] = [base_date - timedelta(days=random.randint(0, 365)) for _ in range(n_rows)]
        else:
            data[col] = [None] * n_rows
    return pd.DataFrame(data)

def enforce_schema(df, schema):
    """Enforce the types specified in the YAML schema."""
    if 'columns' not in schema:
        return df
    
    print(f"  Enforcing schema types...")
    for col, dtype in schema['columns'].items():
        if col not in df.columns:
            continue
            
        try:
            if dtype == 'int':
                # Remove everything except digits, dots and minus
                if df[col].dtype == 'object':
                    df[col] = df[col].astype(str).str.replace(r'[^\d.-]', '', regex=True)
                df[col] = pd.to_numeric(df[col], errors='coerce')
                # Use nullable Int64 (capital I) to support NA in Integer columns in Parquet
                df[col] = df[col].round().astype('Int64')
            elif dtype == 'float':
                if df[col].dtype == 'object':
                    # Remove currency symbols, commas, etc.
                    df[col] = df[col].astype(str).str.replace(r'[^\d,-]', '', regex=True)
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('float64')
            elif dtype == 'datetime':
                df[col] = pd.to_datetime(df[col], errors='coerce')
            elif dtype == 'string':
                # Ensure it's treated as a string, but preserve proper NULLs
                # We use object type for strings in Pandas to support None/NaN
                df[col] = df[col].astype(str).replace(['nan', 'None', 'NaT', 'None'], np.nan)
        except Exception as e:
            print(f"    Warning: Could not enforce type {dtype} for column {col}: {e}")
            
    print(f"    Final Types after enforcement:\n{df.dtypes}")
    return df

def main():
    parser = argparse.ArgumentParser(description="Data Generation Pipeline")
    parser.add_argument("--schema", type=str, help="Specific schema name to process (e.g., amazon_sale)")
    parser.add_argument("--format", type=str, choices=["csv", "delta", "both"], default="both", 
                        help="Upload format: csv, delta, or both (default: both)")
    parser.add_argument("--skip-upload", action="store_true",
                        help="Skip MinIO upload, only generate local files for benchmarking")
    parser.add_argument("--lakehouse-mode", action="store_true",
                        help="Use lakehouse loader (skip local CSV saving, only save ground truth JSON)")
    args = parser.parse_args()

    # Determine base directory (lakehouse/test)
    # __file__ is test/src/pipeline.py -> parent=src -> parent=test
    base_test_dir = Path(__file__).resolve().parent.parent
    config_dir = base_test_dir / 'configs'

    # Load scenarios
    scenarios_path = config_dir / 'quality_scenarios.yaml'
    try:
        if scenarios_path.exists():
            scenarios = load_config(str(scenarios_path))
        else:
            print(f"Warning: {scenarios_path} not found. No scenarios will be applied.")
            scenarios = {}
    except Exception as e:
         print(f"Error loading scenarios: {e}")
         scenarios = {}

    # Find all schema files
    if args.schema:
        schema_files = [f"{args.schema}_schema.yaml"]
        # Verify file exists
        if not (config_dir / schema_files[0]).exists():
            print(f"Error: Schema file {schema_files[0]} not found in {config_dir}/")
            return
    else:
        # Check if dir exists first
        if not config_dir.exists():
             print(f"Error: Config directory not found at {config_dir}")
             return
        schema_files = [f for f in os.listdir(config_dir) if f.endswith('_schema.yaml')]
    
    if not schema_files:
        print(f"No *_schema.yaml files found in {config_dir}/")
        return

    bucket_name = "datalake"

    for schema_file in schema_files:
        schema_name = schema_file.replace('_schema.yaml', '')
        print(f"\nProcessing schema: {schema_name} ({schema_file})")
        
        try:
            schema = load_config(str(config_dir / schema_file))
        except Exception as e:
            print(f"Error loading schema {schema_file}: {e}")
            continue

        # Generate base data
        if 'source_file' in schema and schema['source_file']:
            print(f"  Loading data from {schema['source_file']}...")
            try:
                df_base = load_data_from_file(schema['source_file'])
                print(f"  Loaded {len(df_base)} rows from file")
            except Exception as e:
                print(f"  Error loading file {schema['source_file']}: {e}")
                continue
        else:
            print(f"  Generating base data for {schema_name}...")
            try:
                df_base = generate_base_data(schema)
            except Exception as e:
                print(f"  Error generating data for {schema_name}: {e}")
                continue
        
        # Enforce types from schema
        df_base = enforce_schema(df_base, schema)
        
        # No longer uploading base data as it's considered a test/reference upload
        pass

        # Apply and upload scenarios
        if 'scenarios' in scenarios:
            for scenario_name, config in scenarios['scenarios'].items():
                print(f"  Applying scenario: {scenario_name}")
                try:
                    df_scenario, meta = apply_combined_scenario(df_base, config)
                    object_name = f"{schema_name}/{scenario_name}.csv"
                    
                    # 1. Save Ground Truth (and optionally CSV for benchmarking)
                    if args.lakehouse_mode:
                        # Lakehouse mode: only save ground truth JSON (no CSV)
                        local_dir = Path("test/data/benchmarks")
                        local_dir.mkdir(parents=True, exist_ok=True)
                        json_path = local_dir / f"{schema_name}_{scenario_name}_truth.json"
                        with open(json_path, 'w') as f:
                            json.dump(meta, f, indent=2, default=str)
                        print(f"    ✓ Saved ground truth: {json_path.name} (no CSV - lakehouse mode)")
                    else:
                        # Traditional mode: save both CSV and JSON
                        local_dir = Path("test/data/generated") / schema_name
                        local_dir.mkdir(parents=True, exist_ok=True)
                        
                        csv_path = local_dir / f"{scenario_name}.csv"
                        json_path = local_dir / f"{scenario_name}_truth.json"
                        
                        df_scenario.to_csv(csv_path, index=False)
                        with open(json_path, 'w') as f:
                            json.dump(meta, f, indent=2, default=str)
                            
                        print(f"    Saved local benchmark data to {csv_path}")

                    # 2. Upload based on selected format (skip if --skip-upload)
                    if not args.skip_upload:
                        if args.format in ["csv", "both"]:
                            upload_df_to_minio(df_scenario, bucket_name, object_name, file_format="csv")
                        
                        if args.format in ["delta", "both"]:
                            # Delta uses directory path, upload function handles .csv removal
                            upload_df_to_minio(df_scenario, bucket_name, object_name, file_format="delta", schema_config=schema)
                    else:
                        print(f"    Skipped MinIO upload (--skip-upload flag)")
                         
                except Exception as e:
                    print(f"  Error processing scenario {scenario_name} for {schema_name}: {e}")
        else:
            print("  No scenarios to apply.")

if __name__ == "__main__":
    main()
