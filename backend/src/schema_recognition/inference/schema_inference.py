import polars as pl
from src.utils.cleaning import clean_column_name

def infer_schema_from_csv(file_path: str, sample_rows: int | None = 1000):
    """Erkennt das Schema einer CSV-Datei (Spaltennamen, Datentypen)."""
    df = pl.read_csv(file_path, n_rows=sample_rows)
    schema = {clean_column_name(name): str(dtype) for name, dtype in df.schema.items()}
    return schema

def infer_schema_from_parquet(file_path: str):
    """Erkennt das Schema einer Parquet-Datei."""
    df = pl.read_parquet(file_path)
    schema = {clean_column_name(name): str(dtype) for name, dtype in df.schema.items()}
    return schema
