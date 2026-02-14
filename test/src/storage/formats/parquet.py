import pandas as pd
from io import BytesIO
from ..minio_client import get_minio_client

def upload_parquet(df: pd.DataFrame, bucket: str, object_name: str):
    """
    Uploads a Pandas DataFrame as a Parquet file to MinIO.
    """
    client = get_minio_client()
    
    # Ensure object name has .parquet extension
    if not object_name.endswith('.parquet'):
        object_name += '.parquet'

    # Convert to Parquet bytes
    # Requires pyarrow or fastparquet
    try:
        parquet_bytes = df.to_parquet(index=False, engine='pyarrow')
    except Exception as e:
        print(f"Warning: Falling back to default engine for Parquet conversion: {e}")
        parquet_bytes = df.to_parquet(index=False)
        
    data = BytesIO(parquet_bytes)

    # Upload
    data.seek(0)
    client.put_object(
        bucket,
        object_name,
        data,
        length=len(parquet_bytes),
        content_type="application/octet-stream"
    )
    
    print(f"Uploaded {object_name} to MinIO bucket {bucket} (Parquet)")
