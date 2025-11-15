import polars as pl
import matplotlib.pyplot as plt
from sklearn.ensemble import IsolationForest
from src.connection.connection import get_connection


conn = get_connection()
query = 'SELECT * FROM lakehouse.datalake.raw.amazon_co-ecomerce_sample'
df = conn.toPolars(query)

# 1. Data Profiling: Summary statistics
def profile_data(df):
    print("Shape:", df.shape)
    print("Columns:", df.columns)
    print("Data types:\n", df.schema)
    print("Missing values per column:\n", df.null_count())
    print("Describe:\n", df.describe())

# 2. Anomaly Detection (Isolation Forest on numeric fields)
def find_anomalies(df, numeric_fields):
    #convert to numpy for sklearn
    X = df.select(numeric_fields).to_numpy()
    # Fit Isolation Forest
    clf = IsolationForest(contamination=0.05, random_state=42)
    preds = clf.fit_predict(X)
    df = df.with_column(pl.Series("anomaly", preds))
    print(f"Anomaly distribution:\n{df['anomaly'].value_counts()}")
    # Plot if 2D
    if len(numeric_fields) == 2:
        plt.scatter(df[numeric_fields[0]], df[numeric_fields[1]], c=df["anomaly"])
        plt.xlabel(numeric_fields[0])
        plt.ylabel(numeric_fields[1])
        plt.title("Anomaly Visualization")
        plt.show()
    return df.filter(pl.col("anomaly") == -1)

# 3. Semantic Field Mapping (very basic - string matches)
def semantic_field_mapping(df, reference_fields):
    found_matches = {}
    for col in df.columns:
        for ref in reference_fields:
            if col.lower() == ref.lower():
                found_matches[col] = ref
    print("Mapped fields:\n", found_matches)
    return found_matches

# Example usage:
#profile_data(df)
num_cols = ['discounted_price', 'actual_price', 'discount_percentage', 'rating', 'rating_count']

for col in num_cols:
    df = df.with_columns(
        pl.col(col)
        .str.replace_all(r"[^\d\.]", "")
        .cast(pl.Float64)
        .alias(col)
    )
anomalies = find_anomalies(df, numeric_fields=['your_numeric_column1', 'your_numeric_column2'])
field_mappings = semantic_field_mapping(df, reference_fields=['expected_field1', 'expected_field2'])

# Extend these functions modularly as you add more AI logic or workflow automation.
