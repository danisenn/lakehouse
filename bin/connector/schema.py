from connection import get_connection

def list_tables(space_path="lakehouse.datalake.raw"):
    """Return a list of all table names in the given schema."""
    conn = get_connection()
    query = f'SELECT TABLE_NAME FROM INFORMATION_SCHEMA."TABLES" WHERE TABLE_SCHEMA = \'{space_path}\''
    tables = conn.toPolars(query)
    return tables["TABLE_NAME"].to_list()


def get_table_schema(space_path, table_name):
    """Return the schema of a single table as a DataFrame."""
    conn = get_connection()
    # Quote the space_path properly
    space_parts = space_path.split('.')
    quoted_path = '.'.join(f'"{part}"' for part in space_parts)
    query = f"""
    SELECT COLUMN_NAME, DATA_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = '{space_path}' AND TABLE_NAME = '{table_name}'
    ORDER BY ORDINAL_POSITION
    """
    try:
        return conn.toPolars(query)
    except Exception as e:
        print(f"Failed to get schema for {table_name}: {e}")
        return None


def extract_schema_safe(space_path, table_name):
    """Print the schema of a table using a sample of 10 rows."""
    conn = get_connection()
    space_parts = space_path.split('.')
    quoted_path = '.'.join(f'"{part}"' for part in space_parts)
    query = f'SELECT * FROM {quoted_path}."{table_name}" LIMIT 10'
    try:
        df = conn.toPolars(query)
        print(f"Schema for {table_name}:")
        print(df.schema)
        return df
    except Exception as e:
        print(f"Failed for {table_name}: {e}")
        return None


def extract_schemas_for_all_tables(space_path="lakehouse.datalake.raw"):
    """Print schemas for all tables in the schema."""
    tables = list_tables(space_path)
    for table_name in tables:
        extract_schema_safe(space_path, table_name)
