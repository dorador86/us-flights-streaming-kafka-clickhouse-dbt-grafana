import pyarrow.parquet as pq
import pandas as pd
import os

def explore_parquet(file_path):
    """
    Explores metadata and shows the first few rows of a Parquet file safely.
    """
    print(f"=== Data Exploration: {file_path} ===")
    
    # 1. Metadata (Without loading data)
    parquet_file = pq.ParquetFile(file_path)
    print("\n[Metadata]")
    print(f" - Total rows: {parquet_file.metadata.num_rows:,}")
    print(f" - Columns: {len(parquet_file.schema.names)}")
    print(f" - Row Groups: {parquet_file.num_row_groups}")
    
    # 2. Data Sample (Loading only the first 5 rows)
    print("\n[Sample of the first 5 rows]")
    # We use slice(0, 5) to read strictly what is necessary
    table_sample = parquet_file.read_row_group(0).slice(0, 5)
    df_sample = table_sample.to_pandas()
    
    # Show transposed to read all columns comfortably
    pd.set_option('display.max_columns', None)
    print(df_sample.transpose())

def create_csv_sample(parquet_path, csv_path, n_rows=50000):
    """
    Creates a small CSV file from Parquet for performance benchmarking.
    """
    print(f"\n=== Generating CSV sample: {csv_path} ({n_rows} rows) ===")
    
    if os.path.exists(csv_path):
        print(f" ! The file {csv_path} already exists. Skipping generation.")
        return

    # Read only the requested number of rows
    parquet_file = pq.ParquetFile(parquet_path)
    # Read the first row group and slice
    table = parquet_file.read_row_group(0).slice(0, n_rows)
    df = table.to_pandas()
    
    df.to_csv(csv_path, index=False)
    print(f" ✓ CSV sample created successfully. Size: {os.path.getsize(csv_path) / (1024*1024):.2f} MB")

if __name__ == "__main__":
    PARQUET_PATH = "data/raw/Combined_Flights_2022.parquet"
    CSV_SAMPLE_PATH = "data/raw/flights_sample.csv"
    
    if os.path.exists(PARQUET_PATH):
        explore_parquet(PARQUET_PATH)
        create_csv_sample(PARQUET_PATH, CSV_SAMPLE_PATH)
    else:
        print(f"Error: {PARQUET_PATH} not found")
