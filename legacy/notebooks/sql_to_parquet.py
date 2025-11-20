import sqlite3
import pandas as pd

conn = sqlite3.connect("/Volumes/Intenso/Master Thesis/data/amazon/Amazon Fine Food Reviews/database.sqlite")
cursor = conn.cursor()

# Get all table names
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()

# Export each table
for table_name, in tables:
    df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
    df.to_parquet(f"{table_name}.parquet", index=False)
    print(f"Exported {table_name}.parquet")

conn.close()
