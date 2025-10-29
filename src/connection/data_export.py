import os
from src.connection.connection import get_connection
from src.utils.logger import logger
import polars as pl

def list_tables(space_path="lakehouse.datalake.raw"):
    """Return a list of all table names in the given schema."""
    conn = get_connection()
    query = f'SELECT TABLE_NAME FROM INFORMATION_SCHEMA."TABLES" WHERE TABLE_SCHEMA = \'{space_path}\''
    try:
        tables = conn.toPolars(query)
        return tables["TABLE_NAME"].to_list()
    except Exception as e:
        logger.error(f"Fehler beim Abrufen der Tabellen im Schema {space_path}: {e}")
        return []

SAMPLE_SIZE =20  # Anzahl der Zeilen im Sample

def export_table_sample_to_csv(schema_name, table_name, output_dir):
    conn = get_connection()
    query = f'SELECT * FROM {schema_name}."{table_name}" LIMIT {SAMPLE_SIZE}'
    try:
        result = conn.toPolars(query)
        df = pl.from_pandas(result.to_pandas())
        base_name = os.path.splitext(table_name)[0]
        output_path = os.path.join(output_dir, f"{base_name}.csv")
        df.write_csv(output_path)
        logger.info(f"Sample von Tabelle {table_name} exportiert nach {output_path}")
    except Exception as e:
        logger.error(f"Fehler beim Export von Sample der Tabelle {table_name}: {e}")

def export_schema_table_samples(schema_name, output_dir):
    tables = list_tables(schema_name)
    for table in tables:
        export_table_sample_to_csv(schema_name, table, output_dir)

if __name__ == "__main__":
    schema = "lakehouse.datalake.raw"
    output_directory = "src/connection/exported_files"
    os.makedirs(output_directory, exist_ok=True)
    logger.info("Start des Sample-Exports")
    export_schema_table_samples(schema, output_directory)
    logger.info("Sample-Export abgeschlossen")
