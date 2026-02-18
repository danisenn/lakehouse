import sys
import os
from pathlib import Path
import json

# Add roots to sys.path
ROOT_DIR = Path(__file__).resolve().parent.parent.parent.parent
sys.path.append(str(ROOT_DIR / "test" / "src"))
sys.path.append(str(ROOT_DIR / "backend"))

from connection.dremio_api import DremioAPI
from src.connection.data_export import list_tables

def probe_benchmark():
    api = DremioAPI()
    if not api.login():
        print("Login failed")
        return

    schema = "lakehouse.datalake.benchmark_test"
    print(f"Probing schema: {schema}")
    
    try:
        tables = list_tables(schema)
        print(f"Tables found in {schema}: {tables}")
        
        if tables:
            table = tables[0]
            print(f"\nAttempting to query {schema}.\"{table}\"...")
            from src.connection.connection import get_connection
            import adbc_driver_flightsql.dbapi as flight_sql
            import polars as pl
            
            conn_info = get_connection()
            with flight_sql.connect(conn_info['uri'], db_kwargs={
                "username": conn_info['username'],
                "password": conn_info['password'],
            }) as conn:
                
                # Test variations
                queries = [
                    'SELECT * FROM "lakehouse"."datalake"."benchmark_test"."amazon_sales_low_quality.csv" LIMIT 1',
                    'SELECT * FROM lakehouse.datalake.benchmark_test."amazon_sales_low_quality.csv" LIMIT 1',
                    'SELECT * FROM "lakehouse".datalake.benchmark_test."amazon_sales_low_quality.csv" LIMIT 1',
                    'SELECT * FROM "lakehouse"."datalake/benchmark_test/amazon_sales_low_quality.csv" LIMIT 1',
                ]
                
                for i, q in enumerate(queries, 1):
                    print(f"\nTrying Query {i}: {q}")
                    try:
                        df = pl.read_database(query=q, connection=conn)
                        print(f"Query {i} SUCCESSFUL!")
                        print(f"Columns: {df.columns}")
                        print(df.to_pandas().iloc[0].to_dict())
                        return # Stop if found
                    except Exception as e:
                        print(f"Query {i} FAILED: {e}")

    except Exception as e:
        print(f"Probe failed: {e}")

if __name__ == "__main__":
    probe_benchmark()
