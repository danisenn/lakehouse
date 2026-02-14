from minio import Minio
import os

def get_minio_client():
    endpoint = os.getenv("MINIO_ENDPOINT")
    access_key = os.getenv("MINIO_ACCESS_KEY")
    secret_key = os.getenv("MINIO_SECRET_KEY")
    region = os.getenv("MINIO_REGION")
    
    missing = []
    if not endpoint:
        missing.append("MINIO_ENDPOINT")
    if not access_key:
        missing.append("MINIO_ACCESS_KEY")
    if not secret_key:
        missing.append("MINIO_SECRET_KEY")
    if not region:
        missing.append("MINIO_REGION")
    
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}. Please set them in your .env file.")
    
    return Minio(
        endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=False,  # falls du kein TLS nutzt
        region=region
    )