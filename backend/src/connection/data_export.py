import os
from src.connection.connection import get_connection
from src.utils.logger import logger
import polars as pl
import adbc_driver_flightsql.dbapi as flight_sql

def _get_dbapi_connection():
    """Create a DBAPI connection using credentials from get_connection()."""
    conn_dict = get_connection()
    uri = conn_dict.get('uri')
    username = conn_dict.get('username')
    password = conn_dict.get('password')
    
    # Connect using DBAPI
    # This handles grpc:// (plaintext) and grpc+tls:// (TLS) correctly
    return flight_sql.connect(uri, db_kwargs={
        "username": username,
        "password": password,
    })


def list_tables(space_path="lakehouse.datalake.raw"):
    """Return a list of all table names in the given schema."""
    query = f'SELECT TABLE_NAME FROM INFORMATION_SCHEMA."TABLES" WHERE TABLE_SCHEMA = \'{space_path}\''
    try:
        with _get_dbapi_connection() as conn:
            tables = pl.read_database(query=query, connection=conn)
            return tables["TABLE_NAME"].to_list()
    except Exception as e:
        logger.error(f"Fehler beim Abrufen der Tabellen im Schema {space_path}: {e}")
        if "connection" in str(e).lower() or "failed to connect" in str(e).lower():
             logger.error("HINT: Check your DREMIO_HOST in .env. If running in Docker on a server, use 'host.docker.internal' or the specific IP, not 'localhost'.")
        return []

SAMPLE_SIZE = 20  # Anzahl der Zeilen im Sample

def export_table_sample_to_csv(schema_name, table_name, output_dir):
    query = f'SELECT * FROM {schema_name}."{table_name}" LIMIT {SAMPLE_SIZE}'
    try:
        with _get_dbapi_connection() as conn:
            df = pl.read_database(query=query, connection=conn)
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
    output_directory = "data/exported_files"
    os.makedirs(output_directory, exist_ok=True)
    logger.info("Start des Sample-Exports")
    export_schema_table_samples(schema, output_directory)
    logger.info("Sample-Export abgeschlossen")
