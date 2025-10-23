from pyspark.sql import SparkSession

# Initialize Spark with Iceberg support
spark = (
    SparkSession.builder
    .appName("CSV to Iceberg")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", "warehouse/")  # path to save Iceberg data
    .getOrCreate()
)

# --- CONFIGURE PATHS ---
csv_path = '/Volumes/Intenso/Master Thesis/data/amazon/Amazon Delivery Dataset/amazon_delivery.csv'
iceberg_table = "local.db.my_iceberg_table"  # database and table name
# ------------------------

# Read CSV file
df = spark.read.option("header", True).csv(csv_path)

# Write as Iceberg table
df.writeTo(iceberg_table).createOrReplace()

print(f"✅ CSV converted to Iceberg table at '{iceberg_table}'")

spark.stop()
