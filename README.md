# AI-Powered Assistance System for Semantic Data Integration in Lakehouse Architectures 

## Lakehouse Assistance System

This repository includes a unified assistant that evaluates datasets using:
- anomaly_detection (Z-Score, IQR, Isolation Forest)
- schema_recognition (infers column names and dtypes)
- semantic_filed_mapping (maps dataset columns to your reference fields)

### Quick start
1) Install dependencies:
```
pip install -r requirements.txt
```

2) Run on local data folder (defaults to `data/`):
```
python3 scripts/lakehouse_assistant.py --config configs/assistant_example.yml --refs label,title,text --verbose
```

3) See outputs:
- JSON report at `artifacts/assistant_report.json`
- Anomaly samples under `artifacts/anomalies/`

### CLI options
```
python scripts/lakehouse_assistant.py --help
```
Key flags:
- `--root PATH`               folder to scan for CSV/Parquet
- `--refs a,b,c`              reference fields for semantic mapping
- `--refs-file PATH`          YAML/JSON containing `reference_fields: [...]`
- `--synonyms-file PATH`      YAML/JSON dict of `{ field: [aliases...] }`
- `--threshold FLOAT`         mapping acceptance cutoff (default 0.7)
- `--epsilon FLOAT`           ambiguity window (default 0.05)
- `--use-zscore/--use-iqr/--use-isoforest` toggles detectors (default: all on)
- `--z-threshold FLOAT`       Z-Score threshold (default 3.0)
- `--contamination FLOAT`     Isolation Forest contamination (default 0.01)

### Run tests
- All tests: `python -m pytest -q`
- Semantic mapping only: `python -m pytest -q src/tests/test_semantic_filed_mapping.py`

### Notes on Lakehouse connections
A stub `LakehouseSQLDataSource` is provided under `src/assistant/datasource.py`. Implement your
connection and table iteration to fetch DataFrames from the lakehouse if needed.
