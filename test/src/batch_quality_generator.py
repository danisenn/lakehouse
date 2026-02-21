"""
Batch Quality Generator

Generates multiple quality variants of lakehouse datasets.
Downloads from Dremio, applies quality degradation, and uploads back to lakehouse.
"""

import os
import sys
import json
import argparse
from pathlib import Path
from datetime import datetime
from typing import Dict, List
import pandas as pd
import subprocess

# Path setup
TEST_DIR = Path(__file__).resolve().parent.parent
TEST_SRC_DIR = TEST_DIR / "src"

sys.path.insert(0, str(TEST_DIR))
sys.path.insert(0, str(TEST_SRC_DIR))
from src.lakehouse_loader import list_tables, download_table, convert_dremio_to_yaml_types
from src.storage.upload import upload_df_to_minio
from src.connection.dremio_api import promote_to_dremio
from src.generators.combined_scenarios import apply_combined_scenario
import yaml


def compute_ground_truth_types(df: pd.DataFrame) -> Dict:
    """
    Compute original schema and semantic types by calling compute_ground_truth.py
    as a subprocess (avoids test/backend src namespace collision).
    """
    import tempfile
    tmp = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
    try:
        df.to_csv(tmp.name, index=False)
        script = Path(__file__).resolve().parent / "compute_ground_truth.py"
        result = subprocess.run(
            [sys.executable, str(script), tmp.name],
            capture_output=True, text=True, timeout=30
        )
        if result.returncode == 0:
            return json.loads(result.stdout)
        else:
            print(f"    ⚠ Ground truth computation failed: {result.stderr[:200]}")
            return {}
    finally:
        os.unlink(tmp.name)

 
def load_quality_scenarios(config_path: str = None) -> Dict:
    """Load quality scenarios from YAML configuration.""" 
    if config_path is None:
        config_path = Path(__file__).resolve().parent.parent / 'configs' / 'quality_scenarios.yaml'
    
    with open(config_path, 'r') as f:
        return yaml.safe_load(f) 


def create_schema_config(schema_dict: Dict[str, str]) -> Dict:
    """
    Convert Dremio schema to pipeline schema format.
    
    Args:
        schema_dict: Dictionary from Dremio with column names and SQL types
        
    Returns:
        Dictionary with 'columns' key mapping to simplified types
    """
    columns = {}
    for col_name, dremio_type in schema_dict.items():
        columns[col_name] = convert_dremio_to_yaml_types(dremio_type)
    
    return {'columns': columns}


def generate_quality_variants(
    df: pd.DataFrame,
    table_name: str,
    schema_config: Dict,
    quality_scenarios: Dict,
    bucket_name: str = "datalake",
    upload: bool = True,
    save_local_json: bool = True,
    target_schema: str = None,
    file_format: str = None
) -> None:
    """
    Generate and upload quality variants for a single dataset.
    
    Args:
        df: Source dataframe
        table_name: Name of the table
        schema_config: Schema configuration dict
        quality_scenarios: Quality scenarios configuration
        bucket_name: MinIO bucket name
        upload: Whether to upload to lakehouse
        save_local_json: Whether to save ground truth JSON locally
        file_format: Upload format (csv, delta, parquet). If None, auto-detect from table_name
    """
    benchmarks_dir = Path(__file__).resolve().parent.parent / 'data' / 'benchmarks'
    benchmarks_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"\n{'='*60}")
    print(f"Processing: {table_name}")
    print(f"{'='*60}")
    
    # Compute ground truth schema and semantic types from the ORIGINAL data
    print(f"  Computing ground truth schema and semantic types...")
    gt_types = compute_ground_truth_types(df)
    original_schema = gt_types.get("original_schema", {})
    original_semantic_types = gt_types.get("expected_semantic_types", {})
    print(f"    ✓ Original schema: {len(original_schema)} columns typed")
    print(f"    ✓ Semantic types detected: {original_semantic_types}")
    
    for scenario_name, config in quality_scenarios['scenarios'].items():
        print(f"\n  Generating {scenario_name} variant...")
        
        try:
            # Apply quality degradation
            df_variant, metadata = apply_combined_scenario(df.copy(), config)
            
            # Prepare names
            variant_table_name = f"{table_name}_{scenario_name}"
            
            # Save ground truth JSON locally
            if save_local_json:
                json_path = benchmarks_dir / f"{variant_table_name}_truth.json"
                with open(json_path, 'w') as f:
                    json.dump({
                        'original_table': table_name,
                        'quality_level': scenario_name,
                        'generated_at': datetime.now().isoformat(),
                        'row_count': len(df_variant),
                        'columns': list(df.columns),
                        'original_schema': original_schema,
                        'expected_semantic_types': original_semantic_types,
                        'quality_config': config,
                        'degradation_metadata': metadata
                    }, f, indent=2, default=str)
                print(f"    ✓ Saved ground truth: {json_path.name}")
            
            # Upload to lakehouse (NO local CSV saved)
            if upload:
                # If target_schema is provided, use the last part as a folder prefix 
                # (unless it matches the bucket name exactly)
                if target_schema:
                    schema_parts = target_schema.split(".")
                    # If the last part is the table name itself, we go one level up
                    # but usually target_schema is like 'lakehouse.datalake.quality_variants'
                    prefix = schema_parts[-1]
                    if prefix == bucket_name:
                        object_name = variant_table_name
                    else:
                        object_name = f"{prefix}/{variant_table_name}"
                else:
                    # Default
                    object_name = f"quality_variants/{variant_table_name}"
                    
                upload_df_to_minio(
                    df_variant, 
                    bucket_name, 
                    object_name, 
                    file_format=file_format,
                    schema_config=schema_config
                )
                print(f"    ✓ Uploaded to lakehouse: {object_name} (format: {file_format})")
                
                # Auto-promote to Dremio dataset
                if file_format in ["csv", "parquet", "json"]:
                    print(f"    Promoting {variant_table_name} to Dremio dataset...")
                    # Add extension to object name for promotion if not already there
                    promo_object = object_name
                    if file_format == "csv" and not promo_object.endswith(".csv"):
                        promo_object += ".csv"
                    elif file_format == "parquet" and not promo_object.endswith(".parquet"):
                        promo_object += ".parquet"
                    elif file_format == "json" and not promo_object.endswith(".json"):
                        promo_object += ".json"
                    
                    if promote_to_dremio(bucket_name, promo_object, file_format, target_schema=target_schema):
                        print(f"    ✓ Automatically ingested into Dremio")
                    else:
                        print(f"    ⚠ Auto-ingestion failed (manual declaration might be needed)")
            else:
                print(f"    ⊘ Upload skipped (--no-upload flag)")
                
        except Exception as e:
            print(f"    ✗ Error processing {scenario_name}: {e}")


def main():
    parser = argparse.ArgumentParser(
        description="Generate quality variants of lakehouse datasets",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate variants for a specific table
  python src/batch_quality_generator.py --table amazon_sales
  
  # Process all tables in a schema
  python src/batch_quality_generator.py --all
  
  # Process without uploading (for testing)
  python src/batch_quality_generator.py --table amazon_sales --no-upload
        """
    )
    
    parser.add_argument(
        '--schema',
        type=str,
        default='lakehouse.datalake.raw',
        help='Dremio schema path (default: lakehouse.datalake.raw)'
    )
    
    parser.add_argument(
        '--table',
        type=str,
        help='Specific table name to process'
    )
    
    parser.add_argument(
        '--all',
        action='store_true',
        help='Process all tables in the schema'
    )
    
    parser.add_argument(
        '--no-upload',
        action='store_true',
        help='Skip MinIO upload (only generate ground truth JSON)'
    )
    
    parser.add_argument(
        '--limit',
        type=int,
        help='Limit number of rows to download (for testing)'
    )
    
    parser.add_argument(
        '--bucket',
        type=str,
        default='datalake',
        help='MinIO bucket name (default: datalake)'
    )
    
    parser.add_argument(
        '--target-schema',
        type=str,
        default='lakehouse.datalake.quality_variants',
        help='Target Dremio schema for quality variants (default: lakehouse.datalake.quality_variants)'
    )
    
    args = parser.parse_args()
    
    # Validate arguments
    if not args.table and not args.all:
        parser.error("Either --table or --all must be specified")
    
    # Load quality scenarios
    scenarios = load_quality_scenarios()
    print(f"Loaded quality scenarios: {', '.join(scenarios['scenarios'].keys())}")
    
    # Determine tables to process
    if args.all:
        print(f"\nDiscovering tables in {args.schema}...")
        tables = list_tables(args.schema)
        if not tables:
            print(f"No tables found in {args.schema}")
            return
        print(f"Found {len(tables)} tables: {tables}")
    else:
        tables = [args.table]
    
    # Process each table
    upload = not args.no_upload
    total_variants = 0
    
    for table_name in tables:
        try:
            # Download table from lakehouse
            print(f"\nDownloading {table_name} from {args.schema}...")
            df, schema_dict = download_table(args.schema, table_name, limit=args.limit)
            
            if df.empty:
                print(f"  Skipping {table_name} (empty or error)")
                continue
            
            # Convert schema
            schema_config = create_schema_config(schema_dict)
            
            # Detect format from table name (preserve original format)
            if table_name.endswith('.csv'):
                detected_format = 'csv'
                clean_table_name = table_name.replace('.csv', '')
            elif table_name.endswith('.parquet'):
                detected_format = 'parquet'
                clean_table_name = table_name.replace('.parquet', '')
            elif table_name.endswith('.json'):
                detected_format = 'json'
                clean_table_name = table_name.replace('.json', '')
            else:
                # Assume Delta table if no extension
                detected_format = 'delta'
                clean_table_name = table_name
            
            print(f"  Detected format: {detected_format}")
            
            # Generate variants
            generate_quality_variants(
                df=df,
                table_name=clean_table_name,
                schema_config=schema_config,
                quality_scenarios=scenarios,
                bucket_name=args.bucket,
                upload=upload,
                file_format=detected_format,
                save_local_json=True,
                target_schema=args.target_schema
            )
            
            total_variants += len(scenarios['scenarios'])
            
        except Exception as e:
            print(f"\n✗ Error processing table {table_name}: {e}")
            import traceback
            traceback.print_exc()
    
    # Summary
    print(f"\n{'='*60}")
    print(f"SUMMARY")
    print(f"{'='*60}")
    print(f"Tables processed: {len(tables)}")
    print(f"Quality variants generated: {total_variants}")
    if upload:
        print(f"Uploaded to: {args.bucket}")
    print(f"Ground truth saved to: test/data/benchmarks/")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
