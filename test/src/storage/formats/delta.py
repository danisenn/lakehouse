import pandas as pd
import tempfile
import os
from ..spark import get_spark_session

def upload_delta(df: pd.DataFrame, bucket: str, object_name: str, schema_config: dict = None):
    """
    Uploads a Pandas DataFrame as a Delta Lake table to MinIO.
    Uses an intermediate Parquet file to bypass Python serialization issues.
    """
    spark = get_spark_session()
    
    # Clean object name for Delta (directory based)
    clean_object_name = object_name.replace(".csv", "")
    s3a_path = f"s3a://{bucket}/{clean_object_name}"
    
    print(f"Converting Pandas DataFrame to Spark DataFrame for {clean_object_name}...")
    
    # 1. Sanitize "object" columns for Parquet/Arrow compatibility
    df_clean = df.copy()
    for col in df_clean.columns:
        if df_clean[col].dtype == 'object':
            # Map various null representations to None (Parquet NULL)
            df_clean[col] = df_clean[col].apply(
                lambda x: str(x) if pd.notnull(x) and str(x).lower() not in ['nan', 'none', 'nat'] else None
            )
    
    # 2. Write to intermediate Parquet
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = tmp.name
        
    try:
        df_clean.to_parquet(tmp_path, index=False, engine='pyarrow')
        
        # 3. Read back using Spark
        spark_df = spark.read.parquet(tmp_path)
        
        # 4. Apply schema-aware casting if config provided
        if schema_config and 'columns' in schema_config:
            from pyspark.sql.functions import col as spark_col
            print(f"  Applying schema-aware casting for {clean_object_name}...")
            for col_name, dtype in schema_config['columns'].items():
                if col_name in spark_df.columns:
                    if dtype == 'int':
                        spark_df = spark_df.withColumn(col_name, spark_col(col_name).cast("long"))
                    elif dtype == 'float':
                        spark_df = spark_df.withColumn(col_name, spark_col(col_name).cast("double"))
                    elif dtype == 'datetime':
                        spark_df = spark_df.withColumn(col_name, spark_col(col_name).cast("timestamp"))
                    elif dtype == 'string':
                        spark_df = spark_df.withColumn(col_name, spark_col(col_name).cast("string"))

        print(f"Spark schema for {clean_object_name}:")
        spark_df.printSchema()
        
        # 5. Write Delta table
        print(f"Writing Delta table to {s3a_path}...")
        spark_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("delta.compatibility.symlinkFormatManifest.enabled", "true") \
            .save(s3a_path)
            
        # 6. Generate manifest for Dremio
        try: 
            from delta.tables import DeltaTable
            deltaTable = DeltaTable.forPath(spark, s3a_path)
            deltaTable.generate("symlink_format_manifest")
            print(f"Generated Symlink Manifest for {clean_object_name}")
        except Exception as e:
            print(f"Warning: Could not generate manifest: {e}")
            
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)

    print(f"Uploaded {clean_object_name} to MinIO bucket {bucket} (Delta)")
