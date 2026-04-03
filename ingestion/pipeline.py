from extract import run as extract
from load import run as load_bronze

if __name__ == "__main__":
    print("=== Step 1: Extract ===")
    raw_files = extract()

    print("\n=== Step 2: Load to Bronze ===")
    load_bronze(raw_files)

    print("\nPipeline complete.")