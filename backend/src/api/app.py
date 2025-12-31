from __future__ import annotations
import os
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

from src.api.models import RunRequest, RunAccepted, ArtifactList, ArtifactItem
from src.api.services import (
    run_sync,
    create_report_id,
    save_report,
    mark_run_status,
    load_report,
    list_artifacts_for_report,
    resolve_artifact_path,
)
from src.assistant.runner import MappingConfig, AnomalyConfig

API_TITLE = "Lakehouse Assistant API"
API_VERSION = os.getenv("API_VERSION", "v1")

app = FastAPI(title=API_TITLE, version=API_VERSION, openapi_url="/openapi.json")

# CORS
origins = [o.strip() for o in os.getenv("CORS_ORIGINS", "*").split(",") if o.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Request Logging Middleware
# Request Logging Middleware
@app.middleware("http")
async def log_requests(request, call_next):
    from src.utils.logger import logger
    import time
    
    start_time = time.time()
    try:
        response = await call_next(request)
        process_time = (time.time() - start_time) * 1000
        
        log_data = {
            "method": request.method,
            "path": request.url.path,
            "status_code": response.status_code,
            "duration_ms": round(process_time, 2),
        }
        logger.info(f"Request processed: {log_data}")
        return response
    except Exception as e:
        process_time = (time.time() - start_time) * 1000
        logger.error(f"Request failed: {request.method} {request.url.path} - {str(e)} - {process_time:.2f}ms")
        raise

# Global Exception Handler
from fastapi.responses import JSONResponse
from fastapi.requests import Request

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    from src.utils.logger import logger
    logger.error(f"Unhandled exception: {str(exc)}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal Server Error", "error": str(exc)},
    )


@app.get("/api/v1/health")
def health():
    return {
        "status": "ok",
        "time": datetime.now(timezone.utc).isoformat(),
        "version": API_VERSION,
    }


@app.post("/api/v1/run", response_model=RunAccepted | dict)
def start_run(payload: RunRequest, mode: str = Query("sync", pattern="^(sync|async)$")):
    mapping = MappingConfig(
        reference_fields=list(payload.mapping.reference_fields),
        synonyms=payload.mapping.synonyms,
        threshold=payload.mapping.threshold,
        epsilon=payload.mapping.epsilon,
    )
    anomaly = None
    if payload.anomaly is not None:
        anomaly = AnomalyConfig(
            z_threshold=payload.anomaly.z_threshold,
            use_iqr=payload.anomaly.use_iqr,
            use_zscore=payload.anomaly.use_zscore,
            use_isolation_forest=payload.anomaly.use_isolation_forest,
            use_missing_values=payload.anomaly.use_missing_values,
            missing_threshold=payload.anomaly.missing_threshold,
            contamination=payload.anomaly.contamination,
            n_estimators=payload.anomaly.n_estimators,
            random_state=payload.anomaly.random_state,
        )

    if mode == "sync":
        try:
            report = run_sync(
                source_model=payload.source,
                mapping=mapping,
                anomaly=anomaly,
            )
            # Convert dataclass to dict for JSON serialization
            from dataclasses import asdict
            return asdict(report)
        except Exception as e:
            from src.utils.logger import logger
            logger.error(f"Run failed: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Run failed: {e}")
    else:
        report_id = create_report_id()
        mark_run_status(report_id, "queued", {"submitted_at": datetime.now(timezone.utc).isoformat()})
        # Synchronous execution in background-like flow (minimal viable async)
        try:
            report = run_sync(
                source_model=payload.source,
                mapping=mapping,
                anomaly=anomaly,
            )
            save_report(report_id, report)
            mark_run_status(report_id, "complete", {"finished_at": datetime.now(timezone.utc).isoformat()})
        except Exception as e:
            from src.utils.logger import logger
            logger.error(f"Async run failed: {e}", exc_info=True)
            mark_run_status(report_id, "error", {"error": str(e)})
        return RunAccepted(report_id=report_id, status="queued")


@app.get("/api/v1/reports/{report_id}")
def get_report(report_id: str):
    data = load_report(report_id)
    if not data:
        raise HTTPException(status_code=404, detail="Report not found")
    return data


@app.get("/api/v1/reports/{report_id}/artifacts", response_model=ArtifactList)
def list_artifacts(report_id: str):
    data = load_report(report_id)
    if not data:
        raise HTTPException(status_code=404, detail="Report not found")
    items = [ArtifactItem(**i) for i in list_artifacts_for_report(data)]
    return {"items": items}


@app.get("/api/v1/reports/{report_id}/artifacts/{name}")
def download_artifact(report_id: str, name: str):
    # Validate report exists
    data = load_report(report_id)
    if not data:
        raise HTTPException(status_code=404, detail="Report not found")
    path = resolve_artifact_path(name)
    if not path:
        raise HTTPException(status_code=404, detail="Artifact not found")
    return FileResponse(path, media_type="text/csv", filename=path.name)


@app.get("/api/v1/tables")
def list_tables_endpoint(schema: str = "lakehouse.datalake.raw"):
    from src.connection.data_export import list_tables
    try:
        tables = list_tables(schema)
        return {"tables": tables}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list tables: {e}")


@app.get("/api/v1/schemas")
def list_schemas_endpoint():
    from src.connection.data_export import list_schemas
    try:
        schemas = list_schemas()
        return {"schemas": schemas}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list schemas: {e}")
