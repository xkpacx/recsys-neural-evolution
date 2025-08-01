import kagglehub
from kagglehub import KaggleDatasetAdapter
from pathlib import Path

# Define processed data directory
processed_dir = Path("data/processed/retailrocket")
processed_dir.mkdir(parents=True, exist_ok=True)

print("Loading retailrocket ecommerce dataset with Polars...")

# Dataset files to load
dataset_files = [
    "events.csv",
    "category_tree.csv", 
    "item_properties_part1.csv",
    "item_properties_part2.csv",
]

# Load each file and persist as Parquet
for file_path in dataset_files:
    try:
        print(f"\nProcessing {file_path}...")
        
        # Load the latest version using Polars (returns LazyFrame)
        lf = kagglehub.load_dataset(
            KaggleDatasetAdapter.POLARS,
            "retailrocket/ecommerce-dataset",
            file_path
        )
        
        # Collect the LazyFrame to get actual DataFrame
        df = lf.collect()
        
        print("First 5 records:")
        print(df.head())
        print(f"Shape: {df.shape}")
        print(f"Columns: {df.columns}")
        
        # Save as Parquet
        file_name = file_path.replace('.csv', '')
        parquet_path = processed_dir / f"{file_name}.parquet"
        df.write_parquet(parquet_path)
        print(f"Saved to: {parquet_path}")
        
    except Exception as e:
        print(f"Could not process {file_path}: {e}")
        continue

print(f"\n✅ Data processing complete!")
print(f"Processed data saved to: {processed_dir}")

# Verification
parquet_files = list(processed_dir.glob("*.parquet"))
print(f"Created {len(parquet_files)} Parquet files:")
for pf in parquet_files:
    print(f"  - {pf.name}")
