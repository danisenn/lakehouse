import os
import sys

# ensure we can import utils
sys.path.append("/app/backend/src")
sys.path.append("/app/test/src")

from dotenv import load_dotenv
load_dotenv("/app/.env")

from minio import Minio
from connection.dremio_api import delete_catalog_entity, get_catalog_entry_by_path

minio_client = Minio(
    os.getenv("MINIO_ENDPOINT", "localhost:9000"),
    access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
    secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
    secure=os.getenv("MINIO_SECURE", "False").lower() == "true",
)

bucket = "datalake"

print("Cleaning MinIO...")
objects = minio_client.list_objects(bucket, recursive=True)
for obj in objects:
    name = obj.object_name
    if name.count("_quality") > 1:
        print(f"Deleting from MinIO: {name}")
        try:
            minio_client.remove_object(bucket, name)
        except Exception as e:
            print(f"Error deleting {name}: {e}")

print("Cleaning Dremio...")
for folder_path in [
    ['lakehouse', 'datalake', 'raw', 'amazon'],
    ['lakehouse', 'datalake', 'benchmark_test', 'amazon']
]:
    folder_entry = get_catalog_entry_by_path(folder_path)
    if folder_entry and "children" in folder_entry:
        for child in folder_entry["children"]:
            if child["type"] == "DATASET" and child["path"][-1].count("_quality") > 1:
                dataset_path = child["path"]
                print(f"Deleting from Dremio: {dataset_path}")
                try:
                    entity = get_catalog_entry_by_path(dataset_path)
                    if entity and "id" in entity:
                        delete_catalog_entity(entity["id"])
                except Exception as e:
                    print(f"Error deleting {dataset_path}: {e}")

print("Done")
