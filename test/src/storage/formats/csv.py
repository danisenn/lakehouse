import pandas as pd
from io import BytesIO
from ..minio_client import get_minio_client

def upload_csv(df: pd.DataFrame, bucket: str, object_name: str):
    """
    Uploads a Pandas DataFrame as a CSV to MinIO.
    """
    client = get_minio_client()
    
    # Ensure object name has .csv extension
    if not object_name.endswith('.csv'):
        object_name += '.csv'

    # Convert to CSV bytes
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    content_length = len(csv_bytes)
    data = BytesIO(csv_bytes)

    # Upload
    try:
        # Ensure we're at the start
        data.seek(0)
        client.put_object(
            bucket,
            object_name,
            data,
            length=content_length,
            content_type="text/csv"
        )
    except Exception as e:
        print(f"      ⊘ First upload attempt failed: {str(e)[:100]}")
        # Seek back to 0 for retry
        data.seek(0)
        try:
            client.put_object(
                bucket,
                object_name,
                data,
                length=content_length,
                content_type="text/csv"
            )
        except Exception as retry_e:
            print(f"      ✗ Upload failed after retry: {retry_e}")
            raise retry_e
    
    print(f"Uploaded {object_name} to MinIO bucket {bucket} (CSV)")
