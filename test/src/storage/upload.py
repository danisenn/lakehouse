import pandas as pd
from .formats.csv import upload_csv
from .formats.delta import upload_delta
from .formats.parquet import upload_parquet

def upload_df_to_minio(df: pd.DataFrame, bucket: str, object_name: str, file_format: str = "csv", schema_config: dict = None):
    """
    Dispatcher function to upload a DataFrame to MinIO in various formats.
    """
    if file_format == "csv":
        upload_csv(df, bucket, object_name)
    elif file_format == "delta":
        upload_delta(df, bucket, object_name, schema_config=schema_config)
    elif file_format == "parquet":
        upload_parquet(df, bucket, object_name)
    else:
        raise ValueError(f"Unsupported file format: {file_format}")
