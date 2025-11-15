#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional

try:
    import yaml  # type: ignore
except Exception:  # pragma: no cover
    yaml = None

# Ensure local src is importable when running as a script
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.assistant.datasource import LocalFilesDataSource
from src.assistant.runner import (
    AnomalyConfig,
    MappingConfig,
    run_assistant,
)


def load_yaml(path: Optional[str]) -> dict:
    if not path:
        return {}
    if yaml is None:
        raise RuntimeError("pyyaml is required to load YAML configs. Install with: pip install pyyaml")
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with p.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Run lakehouse assistance: anomaly detection, schema recognition, and semantic field mapping over datasets.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    ap.add_argument("--root", default="data", help="Folder to scan for CSV/Parquet files")
    ap.add_argument("--max-rows", type=int, default=0, help="Limit rows per file (0 = all)")

    # Mapping / semantic fields
    ap.add_argument("--refs", type=str, default="", help="Comma-separated reference fields (e.g., label,title,text)")
    ap.add_argument("--refs-file", type=str, default="", help="YAML/JSON file with list under 'reference_fields'")
    ap.add_argument("--synonyms-file", type=str, default="", help="YAML/JSON file with mapping 'field: [aliases]' ")
    ap.add_argument("--threshold", type=float, default=0.7, help="Semantic mapping acceptance threshold")
    ap.add_argument("--epsilon", type=float, default=0.05, help="Ambiguity window epsilon")

    # Anomaly detection config
    ap.add_argument("--use-zscore", action="store_true", help="Enable Z-Score detector")
    ap.add_argument("--use-iqr", action="store_true", help="Enable IQR detector")
    ap.add_argument("--use-isoforest", action="store_true", help="Enable Isolation Forest detector")
    ap.add_argument("--z-threshold", type=float, default=3.0, help="Z-Score threshold")
    ap.add_argument("--contamination", type=float, default=0.01, help="Isolation Forest contamination")
    ap.add_argument("--n-estimators", type=int, default=100, help="Isolation Forest estimators")
    ap.add_argument("--random-state", type=int, default=42, help="Isolation Forest random state")

    # I/O
    ap.add_argument("--report", type=str, default="artifacts/assistant_report.json", help="Path to write JSON report")
    ap.add_argument("--save-anomalies", type=str, default="artifacts/anomalies", help="Folder to save anomaly samples (per dataset)")
    ap.add_argument("--config", type=str, default="", help="Optional YAML config to override CLI options")
    ap.add_argument("--verbose", action="store_true", help="Verbose console output")
    return ap.parse_args(argv)


def load_json_or_yaml(path: str) -> dict:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(path)
    if p.suffix.lower() in {".yml", ".yaml"}:
        return load_yaml(path)
    # JSON
    with p.open("r", encoding="utf-8") as f:
        return json.load(f)


def coalesce_reference_fields(args: argparse.Namespace, cfg: dict) -> List[str]:
    # From config file
    refs = cfg.get("reference_fields") if isinstance(cfg.get("reference_fields"), list) else []
    # From refs-file
    if args.refs_file:
        obj = load_json_or_yaml(args.refs_file)
        if isinstance(obj, dict) and isinstance(obj.get("reference_fields"), list):
            refs = obj.get("reference_fields")
        elif isinstance(obj, list):
            refs = obj
    # From --refs
    if args.refs:
        refs = [x.strip() for x in args.refs.split(",") if x.strip()]
    return refs


def maybe_load_synonyms(args: argparse.Namespace) -> Optional[Dict[str, List[str]]]:
    if not args.synonyms_file:
        return None
    obj = load_json_or_yaml(args.synonyms_file)
    if isinstance(obj, dict):
        return {str(k): list(v) for k, v in obj.items()}
    raise ValueError("synonyms-file must be a dict mapping field -> [aliases]")


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    cfg = load_yaml(args.config) if args.config else {}

    data_root = cfg.get("root", args.root)
    max_rows = int(cfg.get("max_rows", args.max_rows)) or 0

    # Semantic mapping config
    refs = coalesce_reference_fields(args, cfg)
    synonyms = maybe_load_synonyms(args)
    threshold = float(cfg.get("threshold", args.threshold))
    epsilon = float(cfg.get("epsilon", args.epsilon))

    # Anomaly config
    use_z = cfg.get("use_zscore", args.use_zscore)
    use_iqr = cfg.get("use_iqr", args.use_iqr)
    use_iso = cfg.get("use_isoforest", args.use_isoforest)
    # Default to all enabled if none explicitly chosen
    if not any([use_z, use_iqr, use_iso]):
        use_z = use_iqr = use_iso = True

    z_thr = float(cfg.get("z_threshold", args.z_threshold))
    contamination = float(cfg.get("contamination", args.contamination))
    n_estimators = int(cfg.get("n_estimators", args.n_estimators))
    random_state = int(cfg.get("random_state", args.random_state))

    report_path = Path(cfg.get("report", args.report))
    save_anomalies = Path(cfg.get("save_anomalies", args.save_anomalies))
    verbose = bool(cfg.get("verbose", args.verbose))

    if not refs:
        print("[warn] No reference fields provided. Use --refs or --refs-file or config. Semantic mapping will be trivial.")

    source = LocalFilesDataSource(root=data_root, max_rows=max_rows)

    mapping_cfg = MappingConfig(
        reference_fields=refs,
        synonyms=synonyms,
        threshold=threshold,
        epsilon=epsilon,
    )
    anomaly_cfg = AnomalyConfig(
        z_threshold=z_thr,
        use_iqr=use_iqr,
        use_zscore=use_z,
        use_isolation_forest=use_iso,
        contamination=contamination,
        n_estimators=n_estimators,
        random_state=random_state,
    )

    report = run_assistant(source, mapping_cfg, anomaly_cfg, save_dir=save_anomalies)

    # Write report
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(report.to_json(), encoding="utf-8")

    # Console summary
    print(f"Scanned root: {getattr(source, 'root', None)}\nDatasets: {len(report.datasets)}\nReport: {report_path}")
    for ds in report.datasets:
        print(f"\n== {ds.name} ==")
        print(f"  shape: {ds.rows} x {ds.cols}")
        print(f"  mapped: {len(ds.mapping)} | ambiguous: {len(ds.ambiguous)} | unmapped: {len(ds.unmapped)}")
        if verbose:
            for k, v in ds.mapping.items():
                tgt = v.get('target'); score = v.get('score')
                print(f"    {k} -> {tgt} (score={score})")
        if ds.anomalies:
            print("  anomalies:")
            for method, count in ds.anomalies.items():
                saved = ds.anomaly_samples_saved.get(method)
                extra = f" saved: {saved}" if saved else ""
                print(f"    {method}: {count}{extra}")

    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
