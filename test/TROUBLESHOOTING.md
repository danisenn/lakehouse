# Troubleshooting: Table Not Found

## Issue

When running:
```bash
python3 src/batch_quality_generator.py --table amazon_sales
```

Error received:
```
Object 'amazon_sales' not found within 'lakehouse.datalake.raw'
```

## Possible Causes

1. **Table not yet uploaded to Dremio** - The amazon_sales data exists locally but hasn't been uploaded to the lakehouse
2. **Different schema path** - The table might be in a different schema (e.g., `lakehouse.datalake` instead of `lakehouse.datalake.raw`)
3. **Different table name** - The table might have a slightly different name in Dremio

## Solution Options

### Option 1: Discover What's Actually in the Lakehouse

Run the discovery script with your venv active:

```bash
cd /Users/daniel/lakehouse/test
source ../.venv/bin/activate  # Activate your virtual environment
python3 discover_lakehouse.py
```

This will show you:
- All available schemas
- All tables in each schema
- The correct schema path and table names to use

### Option 2: Upload amazon_sales First

If the table doesn't exist yet in Dremio, upload it using the traditional pipeline:

```bash
cd /Users/daniel/lakehouse/test
source ../.venv/bin/activate

# Upload amazon_sales from local file to lakehouse
python3 src/pipeline.py --schema amazon_sales --format delta
```

This will:
1. Load from `data/archive/amazon_sales.csv` (as defined in the schema YAML)
2. Generate quality variants
3. Upload all variants as Delta tables to MinIO

### Option 3: Use Local File Mode (Workaround)

If you just want to test the quality generation without lakehouse connectivity, use the traditional pipeline:

```bash
cd /Users/daniel/lakehouse/test
source ../.venv/bin/activate

# Generate variants from local file (no lakehouse download needed)
python3 src/pipeline.py --schema amazon_sales --skip-upload

# Or to save to lakehouse
python3 src/pipeline.py --schema amazon_sales
```

This uses the local `amazon_sales_schema.yaml` configuration.

### Option 4: Update Schema Configuration

Enable loading from local file in the schema:

1. Edit `test/configs/amazon_sales_schema.yaml`
2. Uncomment the `source_file` line:
   ```yaml
   source_file: /Users/daniel/lakehouse/data/archive/amazon_sales.csv
   ```
3. Run: `python3 src/pipeline.py --schema amazon_sales`

## Recommended Workflow

**For initial testing:**

1. Start with local file mode to verify quality generation works:
   ```bash
   cd test
   source ../.venv/bin/activate
   python3 src/pipeline.py --schema amazon_sales --skip-upload
   ```

2. Check output in `test/data/generated/amazon_sales/`

3. Once verified, upload to lakehouse:
   ```bash
   python3 src/pipeline.py --schema amazon_sales --format delta
   ```

**For batch processing existing lakehouse data:**

1. Discover what's available:
   ```bash
   source ../.venv/bin/activate
   python3 discover_lakehouse.py
   ```

2. Use the actual schema/table names shown:
   ```bash
   python3 src/batch_quality_generator.py --schema <actual_schema> --table <actual_table>
   ```

## Quick Commands Reference

```bash
# Activate venv (IMPORTANT for all commands)
cd /Users/daniel/lakehouse/test
source ../.venv/bin/activate

# Discover lakehouse contents
python3 discover_lakehouse.py

# Generate from local file and upload
python3 src/pipeline.py --schema amazon_sales

# Batch process lakehouse (once you know the schema name)
python3 src/batch_quality_generator.py --all --schema <actual_schema_name>
```
