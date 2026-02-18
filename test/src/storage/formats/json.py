import pandas as pd
import json
from io import BytesIO
from ..minio_client import get_minio_client

def upload_json(df: pd.DataFrame, bucket: str, object_name: str):
    """
    Uploads a Pandas DataFrame as a JSON (records format) to MinIO.
    """
    client = get_minio_client()
    
    # Ensure object name has .json extension
    if not object_name.endswith('.json'):
        object_name += '.json'

    # Convert to JSON (records format, which is common for datasets)
    json_str = df.to_json(orient='records', indent=2)
    json_bytes = json_str.encode("utf-8")
    content_length = len(json_bytes)
    data = BytesIO(json_bytes)

    # Upload
    try:
        data.seek(0)
        client.put_object(
            bucket,
            object_name,
            data,
            length=content_length,
            content_type="application/json"
        )
    except Exception as e:
        print(f"      ⊘ First upload attempt failed: {str(e)[:100]}")
        data.seek(0)
        try:
            client.put_object(
                bucket,
                object_name,
                data,
                length=content_length,
                content_type="application/json"
            )
        except Exception as retry_e:
            print(f"      ✗ Upload failed after retry: {retry_e}")
            raise retry_e
    
    print(f"Uploaded {object_name} to MinIO bucket {bucket} (JSON)")
