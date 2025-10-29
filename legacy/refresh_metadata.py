from src.connection.connection import get_connection

def refresh_metadata(space_path, table_name):
    conn = get_connection()
    # Quote identifiers properly
    space_parts = space_path.split('.')
    quoted_path = '.'.join(f'"{part}"' for part in space_parts)
    query = f'ALTER TABLE {quoted_path}."{table_name}" REFRESH METADATA'
    try:
        conn.client.query(query).to_pandas()  # Run the refresh SQL
        print(f"Metadata refreshed for {table_name}")
    except Exception as e:
        print(f"Failed to refresh metadata for {table_name}: {e}")

def refresh_metadata_for_all_tables(space_path="lakehouse.datalake.raw"):
    conn = get_connection()
    # Get list of tables
    query = f'SELECT TABLE_NAME FROM INFORMATION_SCHEMA."TABLES" WHERE TABLE_SCHEMA = \'{space_path}\''
    tables_df = conn.toPolars(query)
    for table_name in tables_df["TABLE_NAME"]:
        refresh_metadata(space_path, table_name)

# Example usage for all tables in the space
refresh_metadata_for_all_tables()
