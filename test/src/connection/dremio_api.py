import os
import requests
import json
from typing import Dict, Optional, List
from dotenv import load_dotenv
from pathlib import Path

class DremioAPI:
    def __init__(self, host: str = None, port: int = 9047, username: str = None, password: str = None):
        load_dotenv()
        self.host = host or os.getenv("DREMIO_HOST")
        self.port = port
        self.username = username or os.getenv("DREMIO_USER")
        self.password = password or os.getenv("DREMIO_PASSWORD")
        self.v2_url = f"http://{self.host}:{self.port}/apiv2"
        self.v3_url = f"http://{self.host}:{self.port}/api/v3"
        self.token = None

    def login(self) -> bool:
        """Authenticate with Dremio and store the session token."""
        url = f"{self.v2_url}/login"
        payload = {
            "userName": self.username,
            "password": self.password
        }
        try:
            response = requests.post(url, json=payload, timeout=10)
            response.raise_for_status()
            self.token = response.json().get("token")
            return True
        except Exception as e:
            print(f"Error logging into Dremio API: {e}")
            return False

    def get_headers(self) -> Dict[str, str]:
        """Return the headers required for API requests."""
        if not self.token:
            self.login()
        return {
            "Authorization": f"_dremio{self.token}",
            "Content-Type": "application/json"
        }

    def get_catalog_entry_by_path(self, path: List[str]) -> Optional[Dict]:
        """Get the full catalog entry for a given path."""
        path_str = "/".join(requests.utils.quote(p) for p in path)
        url = f"{self.v3_url}/catalog/by-path/{path_str}"
        try:
            response = requests.get(url, headers=self.get_headers(), timeout=10)
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            print(f"Error getting catalog entry for {path}: {e}")
            return None

    def refresh_source(self, source_name: str) -> bool:
        """Trigger a metadata refresh for a source."""
        url = f"{self.v2_url}/source/{source_name}/refresh"
        try:
            response = requests.post(url, headers=self.get_headers(), timeout=10)
            return response.status_code in [200, 204]
        except Exception as e:
            print(f"Error refreshing source {source_name}: {e}")
            return False

    def find_resource_in_source(self, source_name: str, target_name: str, current_path: List[str] = None) -> Optional[List[str]]:
        """Recursively search for a file/folder in a source and return its full path list."""
        if current_path is None:
            current_path = [source_name]
            
        url = f"{self.v2_url}/source/{source_name}"
        for folder in current_path[1:]:
            url += f"/folder/{requests.utils.quote(folder)}"
            
        try:
            response = requests.get(url, headers=self.get_headers(), timeout=10)
            if response.status_code != 200:
                return None
            
            contents = response.json().get("contents", {})
            # Check files
            for f in contents.get("files", []):
                if f["name"] == target_name:
                    return current_path + [target_name]
            
            # Check folders
            for f in contents.get("folders", []):
                if f["name"] == target_name:
                    return current_path + [target_name]
                
                # Recurse into folders (limit depth to avoid infinite loops)
                if len(current_path) < 5:
                    found = self.find_resource_in_source(source_name, target_name, current_path + [f["name"]])
                    if found:
                        return found
            return None
        except:
            return None

    def promote_dataset(self, path: List[str], format_type: str = "CSV") -> bool:
        """Promote a file/folder to a physical dataset using v3 API."""
        # 1. Ensure the resource is discovered
        entry = self.get_catalog_entry_by_path(path)
        
        if not entry:
            print(f"Path {path} not found. Refreshing source {path[0]}...")
            self.refresh_source(path[0])
            import time
            time.sleep(2)
            entry = self.get_catalog_entry_by_path(path)
            
        if not entry:
            # Try searching if direct path failed
            print(f"Still not found. Searching in {path[0]}...")
            found_path = self.find_resource_in_source(path[0], path[-1])
            if found_path:
                print(f"✓ Found at {found_path}")
                path = found_path
                entry = self.get_catalog_entry_by_path(path)

        if not entry:
            print(f"Error: Could not find resource {path} for promotion")
            return False

        # If already a dataset, skip promotion
        if entry.get("entityType") == "dataset":
            print(f"✓ {path[-1]} is already a dataset")
            return True

        resource_id = entry.get("id")
        if not resource_id:
            return False

        # 2. Promote via v3 POST /catalog/{quoted_id}
        quoted_id = requests.utils.quote(resource_id, safe='')
        url = f"{self.v3_url}/catalog/{quoted_id}"
        
        # Dremio v3 is case-sensitive for format types
        if format_type.upper() == "CSV":
            fmt = "Text"
            format_settings = {
                "type": fmt,
                "fieldDelimiter": ",",
                "lineDelimiter": "\n",
                "quote": "\"",
                "escape": "\"",
                "extractHeader": True,
                "skipFirstLine": False,
                "trimHeader": True
            }
        elif format_type.upper() == "JSON":
            fmt = "JSON"
            format_settings = {
                "type": fmt,
                "ignoreOtherFileFormats": False
            }
        elif format_type.upper() == "PARQUET":
            fmt = "Parquet"
            format_settings = {
                "type": fmt
            }
        else:
            fmt = format_type.capitalize()
            format_settings = {"type": fmt}

        payload = {
            "entityType": "dataset",
            "type": "PHYSICAL_DATASET",
            "path": path,
            "format": format_settings
        }

        try:
            response = requests.post(url, headers=self.get_headers(), json=payload, timeout=10)
            if response.status_code in [200, 201]:
                print(f"✓ Successfully promoted {path[-1]} as {format_type} dataset")
                return True
            elif response.status_code == 409:
                print(f"✓ {path[-1]} is already a dataset")
                return True
            else:
                print(f"Error promoting dataset {path[-1]}: {response.text}")
                return False
        except Exception as e:
            print(f"Error promoting dataset {path[-1]}: {e}")
            return False

    def delete_dataset(self, path: List[str]) -> bool:
        """Delete a physical dataset from the catalog using v3 API."""
        entry = self.get_catalog_entry_by_path(path)
        if not entry:
            return False
            
        if entry.get("entityType") != "dataset":
            print(f"Path {path} is not a dataset")
            return False
            
        dataset_id = entry.get("id")
        quoted_id = requests.utils.quote(dataset_id, safe='')
        url = f"{self.v3_url}/catalog/{quoted_id}"
        
        try:
            response = requests.delete(url, headers=self.get_headers(), timeout=10)
            if response.status_code in [200, 204]:
                print(f"✓ Successfully deleted dataset {path[-1]}")
                return True
            else:
                print(f"Error deleting dataset {path[-1]}: {response.status_code} {response.text}")
                return False
        except Exception as e:
            print(f"Error deleting dataset {path[-1]}: {e}")
            return False

def promote_to_dremio(bucket: str, object_path: str, format_type: str, target_schema: str = None):
    """Utility function to promote an uploaded file in MinIO to a Dremio dataset."""
    api = DremioAPI()
    if not api.login():
        return False
        
    if target_schema:
        schema_parts = target_schema.split(".")
        obj_parts = [p for p in object_path.split("/") if p]
        
        # Avoid duplication: if the first obj_part matches the last schema part, skip it
        # e.g. schema=lakehouse.datalake.benchmark_test + obj=benchmark_test/file.csv
        #   -> should be [lakehouse, datalake, benchmark_test, file.csv]
        if obj_parts and obj_parts[0] == schema_parts[-1]:
            path_components = schema_parts + obj_parts[1:]
        elif obj_parts and obj_parts[0] == bucket and bucket in schema_parts:
            path_components = schema_parts + obj_parts[1:]
        else:
            path_components = schema_parts + obj_parts
    else:
        path_components = ["lakehouse", bucket] + [p for p in object_path.split("/") if p]
    
    path_components = [p for p in path_components if p]
    print(f"    Promotion path: {path_components}")
    return api.promote_dataset(path_components, format_type)

if __name__ == "__main__":
    api = DremioAPI()
    if api.login():
        print("Login successful")
        
        # 1. Get lakehouse source ID
        lakehouse_entry = api.get_catalog_entry_by_path(["lakehouse"])
        lakehouse_id = lakehouse_entry.get("id") if lakehouse_entry else None
        print(f"lakehouse id: {lakehouse_id}")
        
        # 2. Get datalake folder ID
        datalake_entry = api.get_catalog_entry_by_path(["lakehouse", "datalake"])
        datalake_id = datalake_entry.get("id") if datalake_entry else None
        print(f"datalake id: {datalake_id}")
        
        if datalake_id:
            # 3. List datalake children
            url = f"{api.v3_url}/catalog/{datalake_id}"
            resp = requests.get(url, headers=api.get_headers())
            if resp.status_code == 200:
                children = resp.json().get('children', [])
                # 4. Find quality_variants id
                qv_id = next((c.get('id') for c in children if c.get('path')[-1] == 'quality_variants'), None)
                if qv_id:
                     # Use the ID directly (it might be a URI, so don't quote it again unless needed)
                     # Dremio v3 API usually wants the ID quoted if it contains special characters
                     quoted_id = requests.utils.quote(qv_id, safe='')
                     url = f"{api.v3_url}/catalog/{quoted_id}"
                     resp = requests.get(url, headers=api.get_headers())
                     if resp.status_code == 200:
                         qv_children = resp.json().get('children', [])
                         print(f"Variants in Dremio: {[c.get('path')[-1] for c in qv_children]}")
                     else:
                         print(f"Failed to list qv children: {resp.status_code} {resp.text}")
    else:
        print("Login failed")
