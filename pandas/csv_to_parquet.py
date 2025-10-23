import pandas as pd
import sys
import os

def csv_to_parquet(csv_path, parquet_path=None):
    """
    Converts a CSV file to a Parquet file.

    Args:
        csv_path (str): Path to the input CSV file.
        parquet_path (str, optional): Path for the output Parquet file.
                                      Defaults to same name as input with .parquet extension.
    """
    # If no output path is given, replace .csv with .parquet
    if parquet_path is None:
        parquet_path = os.path.splitext(csv_path)[0] + ".parquet"

    # Read CSV file
    print(f"Reading CSV file: {csv_path}")
    df = pd.read_csv(csv_path)

    # Save as Parquet
    print(f"Saving as Parquet file: {parquet_path}")
    df.to_parquet(parquet_path, index=False, engine="pyarrow")

    print("Conversion successful!")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python csv_to_parquet.py <input_csv> [output_parquet]")
        sys.exit(1)

    csv_file = sys.argv[1]
    parquet_file = sys.argv[2] if len(sys.argv) > 2 else None

    csv_to_parquet(csv_file, parquet_file)
