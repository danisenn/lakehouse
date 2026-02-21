import argparse
import os
import sqlite3
from io import BytesIO

import boto3
import pandas as pd
from botocore.client import Config


def upload_sqlite_to_minio(sqlite_path: str, bucket_name: str, prefix: str, endpoint_url: str, access_key: str, secret_key: str):
    """
    Reads all tables from a SQLite database, converts them to Parquet,
    and uploads them to a MinIO server.
    """
    print(f"Connecting to SQLite database: {sqlite_path}")
    conn = sqlite3.connect(sqlite_path)
    
    # Get all table names
    query = "SELECT name FROM sqlite_master WHERE type='table';"
    tables_df = pd.read_sql_query(query, conn)
    
    if tables_df.empty:
        print("No tables found in the SQLite database.")
        conn.close()
        return

    # Initialize S3 client for MinIO
    print(f"Connecting to MinIO at {endpoint_url}...")
    s3_client = boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        aws_session_token=None,
        config=Config(signature_version='s3v4', s3={'addressing_style': 'path'}),
        verify=False
    )
    
    # Make sure bucket exists or create it
    try:
        s3_client.head_bucket(Bucket=bucket_name)
    except:
        print(f"Bucket '{bucket_name}' not found. Attempting to create it...")
        try:
            s3_client.create_bucket(Bucket=bucket_name)
        except Exception as e:
            print(f"Failed to create bucket: {e}")

    for table_name in tables_df['name']:
        print(f"Reading table '{table_name}'...")
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
        
        # Convert to parquet in memory
        parquet_buffer = BytesIO()
        df.to_parquet(parquet_buffer, engine='pyarrow', index=False)
        parquet_buffer.seek(0)
        
        # Construct the object key
        # Interpreting "datalake.raw.nba" as bucket "datalake", prefix "raw/nba"
        # If the prefix is set, we append the table name.
        object_key = f"{prefix}/{table_name}.parquet".strip('/')
        
        print(f"Uploading '{table_name}' to s3://{bucket_name}/{object_key}...")
        s3_client.upload_fileobj(parquet_buffer, bucket_name, object_key)
        print("Uploaded successfully.")
        
    conn.close()
    print("All tables processed and uploaded.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Upload SQLite data to MinIO as Parquet")
    parser.add_argument('--sqlite', required=True, help="Path to the input SQLite database file")
    
    # Defaults handle "datalake.raw.nba" structure
    parser.add_argument('--bucket', default='datalake', help="MinIO bucket name")
    parser.add_argument('--prefix', default='raw/nba', help="Path prefix inside the bucket")
    
    parser.add_argument('--endpoint', default='http://10.28.1.180:9000', help="MinIO endpoint URL")
    parser.add_argument('--access-key', default=os.getenv('MINIO_ACCESS_KEY', 'minioadmin'), help="MinIO access key")
    parser.add_argument('--secret-key', default=os.getenv('MINIO_SECRET_KEY', 'minioadmin'), help="MinIO secret key")
    
    args = parser.parse_args()
    
    upload_sqlite_to_minio(
        sqlite_path=args.sqlite,
        bucket_name=args.bucket,
        prefix=args.prefix,
        endpoint_url=args.endpoint,
        access_key=args.access_key,
        secret_key=args.secret_key
    )
