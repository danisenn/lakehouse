# Quick Test - Amazon Sales Quality Variants

Now that we know the table name in Dremio is `amazon_sales.csv`, you can run:

```bash
cd /Users/daniel/lakehouse/test
source ../.venv/bin/activate

# Generate quality variants for amazon_sales
python3 src/batch_quality_generator.py --table "amazon_sales.csv"

# Or process all tables in the schema
python3 src/batch_quality_generator.py --all
```

The script now automatically removes the `.csv` extension when creating output files, so you'll get clean names like:
- `amazon_sales_high_quality` 
- `amazon_sales_medium_quality`
- `amazon_sales_low_quality`

## What to expect

```
Loaded quality scenarios: high_quality, medium_quality, low_quality

Downloading amazon_sales.csv from lakehouse.datalake.raw...
✓ Downloaded 21 rows from amazon_sales.csv
  Columns: product_id, product_name, category, ...

============================================================
Processing: amazon_sales
============================================================

  Generating high_quality variant...
    ✓ Saved ground truth: amazon_sales_high_quality_truth.json
    ✓ Uploaded to lakehouse: quality_variants/amazon_sales_high_quality

  Generating medium_quality variant...
    ✓ Saved ground truth: amazon_sales_medium_quality_truth.json
    ✓ Uploaded to lakehouse: quality_variants/amazon_sales_medium_quality

  Generating low_quality variant...
    ✓ Saved ground truth: amazon_sales_low_quality_truth.json
    ✓ Uploaded to lakehouse: quality_variants/amazon_sales_low_quality

============================================================
SUMMARY
============================================================
Tables processed: 1
Quality variants generated: 3
Uploaded to: datalake
Ground truth saved to: test/data/benchmarks/
============================================================
```

## Verify Results

After running, check:

1. **In Dremio/MinIO**: You should see 3 new Delta tables in `quality_variants/`
2. **Locally**: Ground truth JSON files in `test/data/benchmarks/`
3. **No CSV files saved locally** (disk space optimized!)
