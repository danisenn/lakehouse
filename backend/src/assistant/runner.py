from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence

import polars as pl

from src.utils.logger import logger

from src.assistant.datasource import DataSource, Dataset
from src.schema_recognition.inference import schema_inference
from src.schema_recognition.inference.nested_detection import detect_nested_structures
from src.schema_recognition.inference.nested_detection import detect_nested_structures
from src.schema_recognition.inference.type_inference import refine_types
from src.schema_recognition.inference.statistics import (
    calculate_missing_ratios,
    calculate_numeric_stats,
    calculate_text_stats,
    detect_categorical
)
from src.schema_recognition.inference.semantic import SemanticTypeDetector
from src.semantic_field_mapping import SemanticFieldMapper, map_columns
from src.assistant.llm_client import LLMClient
from src.anomaly_detection.utils import (
    detect_anomalies,
    select_numeric_columns,
)
from src.anomaly_detection.categorical import detect_categorical_anomalies
from src.anomaly_detection.missing_values import detect_missing_value_anomalies


@dataclass
class AnomalyConfig:
    z_threshold: float = 3.0
    use_iqr: bool = True
    use_zscore: bool = True
    use_isolation_forest: bool = True
    use_missing_values: bool = True
    missing_threshold: int = 1
    contamination: float = 0.01
    n_estimators: int = 100
    random_state: int = 42


@dataclass
class MappingConfig:
    reference_fields: Sequence[str]
    synonyms: Optional[Dict[str, List[str]]] = None
    threshold: float = 0.7
    epsilon: float = 0.05


from src.api.models import DatasetReport, AssistantReport


def infer_schema(df: pl.DataFrame) -> Dict[str, str]:
    # Reuse existing helper by writing to a temp file if needed? We also have a simple version:
    # We'll compute directly from df to avoid I/O.
    return {str(name): str(dtype) for name, dtype in df.schema.items()}


from typing import Tuple, Any

def _detect_all_anomalies(
    dataset_name: str,
    df_anom: pl.DataFrame,
    schema: Dict[str, str],
    mapping_cfg: MappingConfig,
    anomaly_cfg: AnomalyConfig,
    numeric_cols: List[str],
    save_dir: Optional[Path],
    save_samples_limit: int,
) -> Tuple[Dict[str, int], Dict[str, Optional[str]], Dict[str, List[int]], Dict[str, List[Dict[str, Any]]], Optional[str]]:
    # Anomaly detection
    anomalies_counts: Dict[str, int] = {}
    anomaly_explanation: Optional[str] = None
    anomalies_saved: Dict[str, Optional[str]] = {}
    anomalies_rows: Dict[str, List[int]] = {}
    anomalies_previews: Dict[str, List[Dict[str, Any]]] = {}


    def maybe_save(name: str, adf: pl.DataFrame) -> Optional[str]:
        if save_dir is None or adf.is_empty():
            return None
        out = save_dir / f"{Path(dataset_name).as_posix().replace('/', '__')}__{name}.csv"
        out.parent.mkdir(parents=True, exist_ok=True)
        adf.drop("row_idx").head(save_samples_limit).write_csv(out)
        return str(out)

    if numeric_cols:
        # Per-column methods
        if anomaly_cfg.use_zscore:
            total = 0
            for c in numeric_cols:
                try:
                    adf = detect_anomalies(df_anom, method="zscore", columns=[c], threshold=anomaly_cfg.z_threshold)
                    total += adf.height
                    if not adf.is_empty():
                        current_rows = anomalies_rows.get("zscore", [])
                        current_rows.extend(adf["row_idx"].to_list())
                        anomalies_rows["zscore"] = sorted(list(set(current_rows)))
                        if "zscore" not in anomalies_previews:
                             anomalies_previews["zscore"] = adf.drop("row_idx").head(5).to_dicts()
                except (pl.ComputeError, ValueError) as e:
                    # Log error but continue with other columns
                    logger.error(f"Error in Z-Score detection for column {c}: {e}")
                    continue
            anomalies_counts["zscore"] = total
            anomalies_saved["zscore"] = None
        if anomaly_cfg.use_iqr:
            total = 0
            for c in numeric_cols:
                try:
                    adf = detect_anomalies(df_anom, method="iqr", columns=[c])
                    total += adf.height
                    if not adf.is_empty():
                        current_rows = anomalies_rows.get("iqr", [])
                        current_rows.extend(adf["row_idx"].to_list())
                        anomalies_rows["iqr"] = sorted(list(set(current_rows)))
                        if "iqr" not in anomalies_previews:
                             anomalies_previews["iqr"] = adf.drop("row_idx").head(5).to_dicts()
                except (pl.ComputeError, ValueError) as e:
                     logger.error(f"Error in IQR detection for column {c}: {e}")
                     continue
            anomalies_counts["iqr"] = total
            anomalies_saved["iqr"] = None
        # Multi-column Isolation Forest
        if anomaly_cfg.use_isolation_forest and len(numeric_cols) >= 1:
            try:
                adf = detect_anomalies(
                    df_anom,
                    method="isolation_forest",
                    columns=numeric_cols,
                    contamination=anomaly_cfg.contamination,
                    n_estimators=anomaly_cfg.n_estimators,
                    random_state=anomaly_cfg.random_state,
                )
                anomalies_counts["isolation_forest"] = adf.height
                if not adf.is_empty():
                    anomalies_rows["isolation_forest"] = adf["row_idx"].to_list()
                    anomalies_previews["isolation_forest"] = adf.drop("row_idx").head(5).to_dicts()
                anomalies_saved["isolation_forest"] = maybe_save("isoforest", adf)
            except (pl.ComputeError, ValueError, ImportError) as e:
                logger.error(f"Error in Isolation Forest detection: {e}")
                anomalies_counts["isolation_forest"] = 0
                anomalies_saved["isolation_forest"] = None

        # Categorical Anomalies
        try:
            cat_anomalies = detect_categorical_anomalies(df_anom)
            if cat_anomalies.height > 0:
                anomalies_counts["categorical"] = cat_anomalies.height
                anomalies_rows["categorical"] = cat_anomalies["row_idx"].to_list()
                anomalies_previews["categorical"] = cat_anomalies.drop("row_idx").head(5).to_dicts()
                anomalies_saved["categorical"] = maybe_save("categorical", cat_anomalies)
        except Exception as e:
            logger.error(f"Error in Categorical Anomaly detection: {e}")

        # Missing Value Anomalies
        if anomaly_cfg.use_missing_values:
            try:
                missing_anomalies = detect_missing_value_anomalies(df_anom, threshold=anomaly_cfg.missing_threshold)
                if missing_anomalies.height > 0:
                    anomalies_counts["missing_values"] = missing_anomalies.height
                    anomalies_rows["missing_values"] = missing_anomalies["row_idx"].to_list()
                    anomalies_previews["missing_values"] = missing_anomalies.drop("row_idx").head(5).to_dicts()
                    anomalies_saved["missing_values"] = maybe_save("missing_values", missing_anomalies)
            except Exception as e:
                logger.error(f"Error in Missing Value detection: {e}")

        # LLM Anomaly Explanation
        # If we have any anomalies, pick a few samples and ask LLM to explain
        try:
            anomaly_samples = []
            for method, previews in anomalies_previews.items():
                for p in previews[:2]:
                    sample = p.copy()
                    sample["_flagged_by"] = method
                    anomaly_samples.append(sample)
                if len(anomaly_samples) >= 5:
                    break
                    
            if anomaly_samples:
                llm = LLMClient()
                explanation = llm.explain_anomalies(dataset_name, schema, anomaly_samples[:5])
                if explanation:
                    anomaly_explanation = explanation
        except Exception as e:
            logger.error(f"LLM Anomaly Explanation failed: {e}")

    return anomalies_counts, anomalies_saved, anomalies_rows, anomalies_previews, anomaly_explanation

def run_on_dataset(
    dataset: Dataset,
    mapping_cfg: MappingConfig,
    anomaly_cfg: AnomalyConfig,
    save_dir: Optional[Path] = None,
    save_samples_limit: int = 200,
) -> DatasetReport:
    df = dataset.df
    # Enhance schema recognition for non-Parquet files (CSV/JSON) or where inference failed
    df = refine_types(df)
    
    rows, cols = df.height, df.width
    
    # Create a version with row index specifically for anomaly tracking
    df_anom = df.with_row_count("row_idx")

    # Schema
    schema = infer_schema(df)
    
    # Advanced Schema Recognition
    nested_cols = detect_nested_structures(df)
    missing_ratios = calculate_missing_ratios(df)
    numeric_stats = calculate_numeric_stats(df)
    text_stats = calculate_text_stats(df)
    categorical_cols = detect_categorical(df)
    
    semantic_detector = SemanticTypeDetector()
    semantic_types = semantic_detector.detect(df)
    
    statistics = {
        "missing_ratios": missing_ratios,
        "numeric_stats": numeric_stats,
        "text_stats": text_stats,
        "row_count": rows,
        "col_count": cols
    }

    # LLM Enrichment
    llm_insights = {"descriptions": {}, "summary": None, "anomaly_explanation": None}
    try:
        llm = LLMClient()
        # Generate summary
        sample_rows = df.head(3).to_dicts()
        llm_insights["summary"] = llm.summarize_table(dataset.name, schema, sample_rows)
        
        # Generate descriptions for a few interesting columns (limit to avoid slow response)
        for col in df.columns:
            if col not in semantic_types:
                desc = llm.generate_column_description(col, df[col].head(5).to_list())
                if desc:
                    llm_insights["descriptions"][col] = desc
    except Exception as e:
        logger.error(f"LLM Enrichment failed: {e}")

    # Semantic mapping
    mapper = SemanticFieldMapper(
        reference_fields=list(mapping_cfg.reference_fields),
        synonyms=mapping_cfg.synonyms,
        threshold=mapping_cfg.threshold,
        epsilon=mapping_cfg.epsilon,
    )
    mapping_result = mapper.map_columns(df)

    # Anomaly detection
    numeric_cols = select_numeric_columns(df_anom, exclude=["row_idx"])
    (
        anomalies_counts,
        anomalies_saved,
        anomalies_rows,
        anomalies_previews,
        anomaly_explanation,
    ) = _detect_all_anomalies(
        dataset_name=dataset.name,
        df_anom=df_anom,
        schema=schema,
        mapping_cfg=mapping_cfg,
        anomaly_cfg=anomaly_cfg,
        numeric_cols=numeric_cols,
        save_dir=save_dir,
        save_samples_limit=save_samples_limit,
    )
    if anomaly_explanation:
        llm_insights["anomaly_explanation"] = anomaly_explanation

    return DatasetReport(
        name=dataset.name,
        path=str(dataset.path) if dataset.path else None,
        rows=rows,
        cols=cols,
        schema=schema,
        semantic_types=semantic_types,
        statistics=statistics,
        nested_structures=nested_cols,
        categorical_cols=categorical_cols,
        llm_insights=llm_insights,
        mapping=mapping_result.get("mapping", {}),
        ambiguous=mapping_result.get("ambiguous", []),
        unmapped=mapping_result.get("unmapped", []),
        anomalies=anomalies_counts,
        anomaly_samples_saved=anomalies_saved,
        anomaly_rows=anomalies_rows,
        anomaly_previews=anomalies_previews,
    )


def run_assistant(
    source: DataSource,
    mapping_cfg: MappingConfig,
    anomaly_cfg: Optional[AnomalyConfig] = None,
    save_dir: Optional[Path] = None,
) -> AssistantReport:
    anomaly_cfg = anomaly_cfg or AnomalyConfig()
    datasets_reports: List[DatasetReport] = []

    if save_dir:
        save_dir.mkdir(parents=True, exist_ok=True)

    # 1. Buffer all datasets to memory to enable global analysis
    # Note: For very large data, we might want to just scan schemas first, 
    # but DataSource.iter_datasets yields dataframes directly.
    all_datasets: List[Dataset] = list(source.iter_datasets())
    
    # 2. Global Schema Discovery
    # Collect all unique column names across all datasets to use as potential mapping targets
    global_columns = set()
    for ds in all_datasets:
        global_columns.update(ds.df.columns)
        
    # 3. Process each dataset with dynamic mapping
    for dataset in all_datasets:
        ds_save_dir = save_dir / dataset.name.replace("/", "__") if save_dir else None
        
        # Dynamic Mapping Logic:
        # The reference fields for this dataset should be:
        #   User provided fields (explicit targets)
        #   + All columns from OTHER tables (implicit targets)
        #   - Columns in THIS table (avoid self-mapping if names are identical, though mapper handles exact match)
        
        # We keep user provided fields as high priority. 
        # The mapper treats all reference fields equally, but we can rely on the fact that
        # if a column matches a user field, it's a good map.
        # If it matches another table's column, it's also a good map (cross-table link).
        
        current_cols = set(dataset.df.columns)
        # Potential targets from other tables
        other_table_cols = global_columns - current_cols
        
        # Combine user refs and other table cols
        # Use a set to avoid duplicates
        dynamic_refs = set(mapping_cfg.reference_fields) | other_table_cols
        
        # Create a specific config for this dataset
        dynamic_mapping_cfg = MappingConfig(
            reference_fields=list(dynamic_refs),
            synonyms=mapping_cfg.synonyms,
            threshold=mapping_cfg.threshold,
            epsilon=mapping_cfg.epsilon
        )
        
        report = run_on_dataset(dataset, dynamic_mapping_cfg, anomaly_cfg, ds_save_dir)
        datasets_reports.append(report)

    root = getattr(source, "root", None)
    return AssistantReport(data_root=str(root) if root else None, datasets=datasets_reports)
