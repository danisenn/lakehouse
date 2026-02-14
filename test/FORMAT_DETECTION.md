# Format Detection Summary

The batch_quality_generator now automatically detects and preserves the source table format:

## Format Detection Rules

| Source Table Name | Detected Format | Upload Format |
|-------------------|----------------|---------------|
| `amazon_sales.csv` | CSV | CSV |
| `data.parquet` | Parquet | CSV* |
| `orders` (no extension) | Delta | Delta |

*Parquet sources are uploaded as CSV to avoid additional dependencies

## How It Works

1. **Download**: Table is downloaded from Dremio (e.g., `amazon_sales.csv`)
2. **Format Detection**: 
   - If filename ends with `.csv` → use CSV format
   - If filename ends with `.parquet` → use CSV format
   - Otherwise → use Delta format
3. **Upload**: Quality variants uploaded in the same format

## Example

```bash
python3 src/batch_quality_generator.py --table "amazon_sales.csv"
```

Output:
```
Downloading amazon_sales.csv from lakehouse.datalake.raw...
✓ Downloaded 1465 rows
  Detected format: csv

Processing: amazon_sales
  Generating high_quality variant...
    ✓ Saved ground truth: amazon_sales_high_quality_truth.json
    ✓ Uploaded to lakehouse: quality_variants/amazon_sales_high_quality (format: csv)
```

## Benefits

✅ **No Delta/Spark errors** for CSV source tables
✅ **Preserves original format** automatically  
✅ **Simpler uploads** for CSV files
✅ **Still supports Delta** for Delta source tables
