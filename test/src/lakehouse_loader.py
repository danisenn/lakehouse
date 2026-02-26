"""
Lakehouse Loader Module

Downloads datasets from the Dremio lakehouse with automatic schema extraction.
Uses existing connection infrastructure from backend/src/connection/.
"""

import os
import sys
import pandas as pd
import polars as pl
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dotenv import load_dotenv

# Add backend to path to import connection module
try:
    # If __file__ is available, resolve relative to it
    backend_path = Path(__file__).resolve().parent.parent.parent / 'backend' / 'src'
except NameError:
    # Fallback to current working directory assumption
    backend_path = Path.cwd() / 'backend' / 'src'

if str(backend_path) not in sys.path:
    sys.path.insert(0, str(backend_path))

try:
    from connection.connection import get_connection
except ModuleNotFoundError:
    # Fallback if the path logic didn't align perfectly (e.g. docker mounts)
    from src.connection.connection import get_connection
import adbc_driver_flightsql.dbapi as flight_sql


def _get_dbapi_connection():
    """Create a DBAPI connection using credentials from get_connection()."""
    load_dotenv()
    conn_dict = get_connection()
    uri = conn_dict.get('uri')
    username = conn_dict.get('username')
    password = conn_dict.get('password')
    
    return flight_sql.connect(uri, db_kwargs={
        "username": username,
        "password": password,
    })


def list_tables(schema_path: str = "lakehouse.datalake.raw") -> List[str]:
    """
    List all tables in the specified Dremio schema.
    
    Args:
        schema_path: Fully qualified schema path (e.g., 'lakehouse.datalake.raw')
        
    Returns:
        List of table names
    """
    query = f"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.\"TABLES\" WHERE TABLE_SCHEMA = '{schema_path}'"
    
    try:
        with _get_dbapi_connection() as conn:
            tables_df = pl.read_database(query=query, connection=conn)
            return tables_df["TABLE_NAME"].to_list()
    except Exception as e:
        print(f"Error listing tables in schema {schema_path}: {e}")
        return []


def get_table_schema(schema_path: str, table_name: str) -> Dict[str, str]:
    """
    Extract schema information for a specific table from Dremio.
    
    Args:
        schema_path: Fully qualified schema path (e.g., 'lakehouse.datalake.raw')
        table_name: Name of the table
        
    Returns:
        Dictionary mapping column names to data types
    """
    query = f"""
        SELECT COLUMN_NAME, DATA_TYPE 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = '{schema_path}' 
        AND TABLE_NAME = '{table_name}'
        ORDER BY ORDINAL_POSITION
    """
    
    try:
        with _get_dbapi_connection() as conn:
            schema_df = pl.read_database(query=query, connection=conn)
            schema_dict = dict(zip(
                schema_df["COLUMN_NAME"].to_list(),
                schema_df["DATA_TYPE"].to_list()
            ))
            return schema_dict
    except Exception as e:
        print(f"Error getting schema for {table_name}: {e}")
        return {}


def download_table(schema_path: str, table_name: str, limit: Optional[int] = None) -> Tuple[pd.DataFrame, Dict[str, str]]:
    """
    Download a table from Dremio as a Pandas DataFrame with schema information.
    
    Args:
        schema_path: Fully qualified schema path (e.g., 'lakehouse.datalake.raw')
        table_name: Name of the table to download
        limit: Optional row limit for testing (None = download all rows)
        
    Returns:
        Tuple of (DataFrame, schema_dict)
    """
    # Get schema first
    schema = get_table_schema(schema_path, table_name)
    
    # Build query
    if limit:
        query = f'SELECT * FROM {schema_path}."{table_name}" LIMIT {limit}'
    else:
        query = f'SELECT * FROM {schema_path}."{table_name}"'
    
    try:
        with _get_dbapi_connection() as conn:
            # Read as Polars then convert to Pandas for compatibility with existing pipeline
            df_polars = pl.read_database(query=query, connection=conn)
            df_pandas = df_polars.to_pandas()
            
            print(f"✓ Downloaded {len(df_pandas)} rows from {table_name}")
            print(f"  Columns: {', '.join(df_pandas.columns.tolist())}")
            
            return df_pandas, schema
            
    except Exception as e:
        print(f"Error downloading table {table_name}: {e}")
        return pd.DataFrame(), {}


def convert_dremio_to_yaml_types(dremio_type: str) -> str:
    """
    Convert Dremio data types to simplified YAML schema types.
    
    Args:
        dremio_type: Dremio SQL data type
        
    Returns:
        Simplified type string for YAML schema
    """
    dremio_type_lower = dremio_type.lower()
    
    if 'int' in dremio_type_lower or 'bigint' in dremio_type_lower:
        return 'int'
    elif 'float' in dremio_type_lower or 'double' in dremio_type_lower or 'decimal' in dremio_type_lower:
        return 'float'
    elif 'timestamp' in dremio_type_lower or 'date' in dremio_type_lower:
        return 'datetime'
    else:
        return 'string'


if __name__ == "__main__":
    # Test the lakehouse loader
    print("Testing Lakehouse Loader...")
    print("=" * 60)
    
    schema = "lakehouse.datalake.raw"
    
    print(f"\nListing tables in {schema}...")
    tables = list_tables(schema)
    print(f"Found {len(tables)} tables: {tables}")
    
    if tables:
        test_table = tables[0]
        print(f"\nTesting download of '{test_table}' (first 5 rows)...")
        df, schema_dict = download_table(schema, test_table, limit=5)
        
        if not df.empty:
            print(f"\nSchema:")
            for col, dtype in schema_dict.items():
                yaml_type = convert_dremio_to_yaml_types(dtype)
                print(f"  {col}: {dtype} → {yaml_type}")
            
            print(f"\nSample data:")
            print(df.head())
