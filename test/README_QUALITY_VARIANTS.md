# Quality Variant Generation System

This system generates multiple quality variants (high/medium/low) of datasets for testing the lakehouse data quality detection capabilities.

## Overview

The system can:
1. **Download datasets from Dremio lakehouse** with automatic schema extraction
2. **Generate 3 quality variants** with progressive data quality degradation
3. **Upload variants to lakehouse** without requiring local CSV storage (saves disk space)
4. **Save ground truth metadata** as JSON for benchmarking

## Key Features

- ✓ **Lakehouse-First**: Download directly from Dremio using existing schemas.
- ✓ **Format Preservation**: Automatically detects and preserves source format (CSV, Delta, Parquet).
- ✓ **Disk Optimized**: No local CSV storage for generated variants.
- ✓ **Ground Truth**: JSON metadata saved for easy benchmarking.

## Quick Start

### 1. Generate quality variants for a specific table

```bash
cd /Users/daniel/lakehouse/test
python3 src/batch_quality_generator.py --table amazon_sales
```

This will:
- Download `amazon_sales` from `lakehouse.datalake.raw`
- Generate 3 quality variants (high_quality, medium_quality, low_quality)
- Upload each variant as a Delta table to MinIO
- Save ground truth JSON files to `test/data/benchmarks/`
- **No local CSV files created** (disk space optimized)

### 2. Process all datasets in the lakehouse

```bash
cd /Users/daniel/lakehouse/test
python3 src/batch_quality_generator.py --all
```

### 3. Test without uploading (dry run)

```bash
cd /Users/daniel/lakehouse/test
python3 src/batch_quality_generator.py --table amazon_sales --no-upload
```

## Quality Levels

The system generates three quality variants with different degradation levels:

| Quality Level | Missing Values | Outliers | Invalid Values | Duplicates | Format Errors |
|--------------|----------------|----------|----------------|------------|---------------|
| **High**     | 3%             | 1%       | -              | -          | -             |
| **Medium**   | 10%            | -        | 5%             | 5%         | -             |
| **Low**      | 20%            | 5%       | 10%            | -          | 2%            |

These parameters are configured in `test/configs/quality_scenarios.yaml`.

## Architecture

### Modules

1. **`lakehouse_loader.py`** - Downloads datasets from Dremio
   - Queries `INFORMATION_SCHEMA` for table discovery and schema extraction
   - Uses existing `backend/src/connection/` infrastructure
   - Returns Pandas DataFrames with proper type conversion

2. **`batch_quality_generator.py`** - Main batch processing script
   - Discovers available tables in specified schema
   - Downloads tables in-memory (no local storage)
   - Applies quality degradation scenarios
   - Uploads variants to MinIO as Delta tables
   - Saves only ground truth JSON locally

3. **`pipeline.py`** - Traditional pipeline (still available)
   - Supports both local file and lakehouse modes
   - Use `--lakehouse-mode` flag to skip local CSV saving

### File Structure

```
test/
├── configs/
│   ├── quality_scenarios.yaml      # Quality degradation configs
│   └── *_schema.yaml                # (Optional) Manual schema configs
├── data/
│   ├── benchmarks/                  # Ground truth JSON files
│   └── generated/                   # (Only in traditional mode) CSV files
└── src/
    ├── lakehouse_loader.py          # Dremio download module
    ├── batch_quality_generator.py   # NEW: Main batch script
    ├── pipeline.py                  # Updated with --lakehouse-mode
    ├── generators/                  # Quality degradation functions
    └── storage/                     # MinIO upload functions
```

## Command Reference

### batch_quality_generator.py

```bash
# Process specific table
python3 src/batch_quality_generator.py --table TABLE_NAME

# Process all tables in schema
python3 src/batch_quality_generator.py --all

# Use different schema
python3 src/batch_quality_generator.py --all --schema lakehouse.datalake.processed

# Test with limited rows
python3 src/batch_quality_generator.py --table amazon_sales --limit 100

# Skip upload (testing)
python3 src/batch_quality_generator.py --table amazon_sales --no-upload

# Use different MinIO bucket
python3 src/batch_quality_generator.py --all --bucket testing
```

### pipeline.py (traditional mode)

```bash
# Generate from local schema files (saves CSVs)
python3 src/pipeline.py --schema amazon_sales

# Lakehouse mode (no local CSVs)
python3 src/pipeline.py --schema amazon_sales --lakehouse-mode

# Skip upload
python3 src/pipeline.py --schema amazon_sales --skip-upload
```

## Output

### Generated Tables in Dremio/MinIO

For each source table (e.g., `amazon_sales`), three variant tables are created:
- `amazon_sales_high_quality`
- `amazon_sales_medium_quality`
- `amazon_sales_low_quality`

These are uploaded to `datalake/quality_variants/` as Delta tables.

### Ground Truth Metadata

JSON files saved to `test/data/benchmarks/`:

```json
{
  "original_table": "amazon_sales",
  "quality_level": "medium_quality",
  "generated_at": "2026-02-14T11:45:00",
  "row_count": 21,
  "quality_config": {
    "missing_values": 0.10,
    "invalid_values": 0.05,
    "duplicates": 0.05
  },
  "degradation_metadata": {
    "missing_values": {
      "affected_rows": [3, 5, 8],
      "columns": ["price", "rating"]
    },
    ...
  }
}
```

## Customization

### Modify Quality Levels

Edit `test/configs/quality_scenarios.yaml`:

```yaml
scenarios:
  high_quality:
    missing_values: 0.03
    outliers: 0.01
  custom_quality:
    missing_values: 0.15
    outliers: 0.08
    duplicates: 0.10
```

### Add New Degradation Types

Quality degradation functions are in `test/src/generators/`:
- `missing_values.py`
- `outliers.py`
- `invalid_values.py`
- `duplicates.py`
- `format_errors.py`

## Requirements

- Python 3.8+
- Access to Dremio lakehouse (configured in `.env`)
- Dependencies: `pandas`, `polars`, `adbc_driver_flightsql`, `pyyaml`

## Environment Variables

Required in `.env` file:

```bash
DREMIO_USER=python
DREMIO_PASSWORD=your_password
DREMIO_HOST=10.28.1.180
DREMIO_PORT=32010
DREMIO_USE_TLS=false
```

## Troubleshooting

### Connection Issues

Test Dremio connection:
```bash
python3 backend/src/test_connection.py
```

### Module Not Found

Ensure you're running from the `test/` directory and have installed dependencies:
```bash
cd /Users/daniel/lakehouse/test
pip3 install -r requirements.txt  # if requirements.txt exists
```

### No Tables Found

Check schema name:
```bash
python3 -c "from src.lakehouse_loader import list_tables; print(list_tables('lakehouse.datalake.raw'))"
```
