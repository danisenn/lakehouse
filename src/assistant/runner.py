from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, List, Optional, Sequence

import polars as pl

from src.assistant.datasource import DataSource, Dataset
from src.schema_recognition.inference import schema_inference
from src.semantic_filed_mapping import SemanticFieldMapper, map_columns
from src.anomaly_detection.utils import (
    detect_anomalies,
    select_numeric_columns,
)


@dataclass
class AnomalyConfig:
    z_threshold: float = 3.0
    use_iqr: bool = True
    use_zscore: bool = True
    use_isolation_forest: bool = True
    contamination: float = 0.01
    n_estimators: int = 100
    random_state: int = 42


@dataclass
class MappingConfig:
    reference_fields: Sequence[str]
    synonyms: Optional[Dict[str, List[str]]] = None
    threshold: float = 0.7
    epsilon: float = 0.05


@dataclass
class DatasetReport:
    name: str
    path: Optional[str]
    rows: int
    cols: int
    schema: Dict[str, str]
    mapping: Dict
    ambiguous: List[str]
    unmapped: List[str]
    anomalies: Dict[str, int]  # method -> count
    anomaly_samples_saved: Dict[str, Optional[str]]  # method -> path


@dataclass
class AssistantReport:
    data_root: Optional[str]
    datasets: List[DatasetReport]

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2)


def infer_schema(df: pl.DataFrame) -> Dict[str, str]:
    # Reuse existing helper by writing to a temp file if needed? We also have a simple version:
    # We'll compute directly from df to avoid I/O.
    return {str(name): str(dtype) for name, dtype in df.schema.items()}


def run_on_dataset(
    dataset: Dataset,
    mapping_cfg: MappingConfig,
    anomaly_cfg: AnomalyConfig,
    save_dir: Optional[Path] = None,
    save_samples_limit: int = 200,
) -> DatasetReport:
    df = dataset.df
    rows, cols = df.height, df.width

    # Schema
    schema = infer_schema(df)

    # Semantic mapping
    mapper = SemanticFieldMapper(
        reference_fields=list(mapping_cfg.reference_fields),
        synonyms=mapping_cfg.synonyms,
        threshold=mapping_cfg.threshold,
        epsilon=mapping_cfg.epsilon,
    )
    mapping_result = mapper.map_columns(df)

    # Anomaly detection
    anomalies_counts: Dict[str, int] = {}
    anomalies_saved: Dict[str, Optional[str]] = {}

    numeric_cols = select_numeric_columns(df)

    def maybe_save(name: str, adf: pl.DataFrame) -> Optional[str]:
        if save_dir is None or adf.is_empty():
            return None
        out = save_dir / f"{Path(dataset.name).as_posix().replace('/', '__')}__{name}.csv"
        out.parent.mkdir(parents=True, exist_ok=True)
        adf.head(save_samples_limit).write_csv(out)
        return str(out)

    if mapping_cfg.reference_fields and numeric_cols:
        # Per-column methods
        if anomaly_cfg.use_zscore:
            total = 0
            for c in numeric_cols:
                try:
                    adf = detect_anomalies(df, method="zscore", columns=[c], threshold=anomaly_cfg.z_threshold)
                    total += adf.height
                except Exception:
                    continue
            anomalies_counts["zscore"] = total
            anomalies_saved["zscore"] = None
        if anomaly_cfg.use_iqr:
            total = 0
            for c in numeric_cols:
                try:
                    adf = detect_anomalies(df, method="iqr", columns=[c])
                    total += adf.height
                except Exception:
                    continue
            anomalies_counts["iqr"] = total
            anomalies_saved["iqr"] = None
        # Multi-column Isolation Forest
        if anomaly_cfg.use_isolation_forest and len(numeric_cols) >= 1:
            try:
                adf = detect_anomalies(
                    df,
                    method="isolation_forest",
                    columns=numeric_cols,
                    contamination=anomaly_cfg.contamination,
                    n_estimators=anomaly_cfg.n_estimators,
                    random_state=anomaly_cfg.random_state,
                )
                anomalies_counts["isolation_forest"] = adf.height
                anomalies_saved["isolation_forest"] = maybe_save("isoforest", adf)
            except Exception:
                anomalies_counts["isolation_forest"] = 0
                anomalies_saved["isolation_forest"] = None

    return DatasetReport(
        name=dataset.name,
        path=str(dataset.path) if dataset.path else None,
        rows=rows,
        cols=cols,
        schema=schema,
        mapping=mapping_result.get("mapping", {}),
        ambiguous=mapping_result.get("ambiguous", []),
        unmapped=mapping_result.get("unmapped", []),
        anomalies=anomalies_counts,
        anomaly_samples_saved=anomalies_saved,
    )


def run_assistant(
    source: DataSource,
    mapping_cfg: MappingConfig,
    anomaly_cfg: Optional[AnomalyConfig] = None,
    save_dir: Optional[Path] = None,
) -> AssistantReport:
    anomaly_cfg = anomaly_cfg or AnomalyConfig()
    datasets: List[DatasetReport] = []

    if save_dir:
        save_dir.mkdir(parents=True, exist_ok=True)

    for dataset in source.iter_datasets():
        ds_save_dir = save_dir / dataset.name.replace("/", "__") if save_dir else None
        report = run_on_dataset(dataset, mapping_cfg, anomaly_cfg, ds_save_dir)
        datasets.append(report)

    root = getattr(source, "root", None)
    return AssistantReport(data_root=str(root) if root else None, datasets=datasets)
