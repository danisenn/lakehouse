from __future__ import annotations
import json
import os
import uuid
from pathlib import Path
from typing import Dict, Any, Optional, List

from src.assistant.runner import run_assistant, MappingConfig, AnomalyConfig, AssistantReport
from src.assistant.datasource import LocalFilesDataSource

# Base artifacts directory (can be overridden with env)
ARTIFACT_DIR = Path(os.getenv("ARTIFACT_DIR", "artifacts"))
REPORTS_DIR = ARTIFACT_DIR / "api_reports"
INDEX_FILE = REPORTS_DIR / "index.json"

# Ensure directories exist
REPORTS_DIR.mkdir(parents=True, exist_ok=True)
(ARTIFACT_DIR / "anomalies").mkdir(parents=True, exist_ok=True)


# Simple JSON index for runs

def _load_index() -> Dict[str, Any]:
    if INDEX_FILE.exists():
        try:
            return json.loads(INDEX_FILE.read_text())
        except Exception:
            return {"runs": []}
    return {"runs": []}


def _save_index(idx: Dict[str, Any]) -> None:
    INDEX_FILE.write_text(json.dumps(idx, indent=2))


def create_report_id() -> str:
    return str(uuid.uuid4())


def run_sync(
    source_model: Any,  # Union[LocalSourceModel, SQLSourceModel]
    mapping: MappingConfig,
    anomaly: Optional[AnomalyConfig],
) -> AssistantReport:
    if source_model.type == "local":
        source = LocalFilesDataSource(root=source_model.root, max_rows=source_model.max_rows)
    elif source_model.type == "sql":
        from src.assistant.datasource import LakehouseSQLDataSource
        from src.connection.connection import get_connection
        
        conn = get_connection()
        source = LakehouseSQLDataSource(
            connection_uri=conn,
            query=source_model.query,
            schema=source_model.schema,
            max_rows=source_model.max_rows
        )
    else:
        raise ValueError(f"Unknown source type: {source_model.type}")

    return run_assistant(
        source,
        mapping,
        anomaly_cfg=anomaly,
        save_dir=ARTIFACT_DIR / "anomalies",
    )


def save_report(report_id: str, report: AssistantReport) -> Path:
    path = REPORTS_DIR / f"{report_id}.json"
    path.write_text(report.to_json())
    idx = _load_index()
    runs = idx.get("runs", [])
    # Upsert entry
    for r in runs:
        if r.get("id") == report_id:
            r.update({"status": "complete"})
            break
    else:
        runs.append({"id": report_id, "status": "complete"})
    idx["runs"] = runs
    _save_index(idx)
    return path


def mark_run_status(report_id: str, status: str, extra: Optional[Dict[str, Any]] = None) -> None:
    idx = _load_index()
    runs = idx.get("runs", [])
    for r in runs:
        if r.get("id") == report_id:
            r.update({"status": status})
            if extra:
                r.update(extra)
            break
    else:
        data: Dict[str, Any] = {"id": report_id, "status": status}
        if extra:
            data.update(extra)
        runs.append(data)
    idx["runs"] = runs
    _save_index(idx)


def load_report(report_id: str) -> Optional[Dict[str, Any]]:
    path = REPORTS_DIR / f"{report_id}.json"
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except Exception:
        return None


def list_artifacts_for_report(report_json: Dict[str, Any]) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    datasets = report_json.get("datasets", []) or []
    for ds in datasets:
        saved = ds.get("anomaly_samples_saved", {}) or {}
        for method, p in saved.items():
            if not p:
                continue
            artifact_path = Path(p)
            if not artifact_path.is_absolute():
                artifact_path = ARTIFACT_DIR / artifact_path
            try:
                artifact_path = artifact_path.resolve(strict=False)
            except Exception:
                continue
            try:
                size = artifact_path.stat().st_size
            except Exception:
                size = 0
            safe_name = artifact_path.name
            items.append(
                {
                    "name": safe_name,
                    "size": size,
                    "dataset_name": ds.get("name"),
                    "method": method,
                }
            )
    return items


def resolve_artifact_path(safe_name: str) -> Optional[Path]:
    # Try direct path under ARTIFACT_DIR
    candidate = (ARTIFACT_DIR / safe_name).resolve()
    try:
        art_root = ARTIFACT_DIR.resolve()
    except Exception:
        return None
    if candidate.exists() and candidate.is_file() and str(candidate).startswith(str(art_root)):
        return candidate
    # Search by filename under ARTIFACT_DIR
    for sub in ARTIFACT_DIR.rglob("*"):
        if sub.is_file() and sub.name == safe_name:
            return sub.resolve()
    return None
