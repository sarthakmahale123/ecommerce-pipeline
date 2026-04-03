import requests
import json
import os
from datetime import datetime, timezone
from pathlib import Path

BASE_URL = "https://dummyjson.com"
RAW_DIR = Path("data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)

ENDPOINTS = {
    "products": "/products",
    "carts":    "/carts",
    "users":    "/users",
}

PAGE_SIZE = 100  # dummyjson max per page


def fetch_all(endpoint: str) -> list:
    """Fetch every record from a paginated dummyjson endpoint."""
    records = []
    skip = 0

    while True:
        url = f"{BASE_URL}{endpoint}?limit={PAGE_SIZE}&skip={skip}"
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        # dummyjson wraps results: {"products": [...], "total": 194, ...}
        key = endpoint.lstrip("/")  # "products", "carts", "users"
        batch = data.get(key, [])
        total = data.get("total", 0)

        records.extend(batch)
        skip += PAGE_SIZE

        print(f"  Fetched {len(records)}/{total} from {endpoint}")

        if len(records) >= total:
            break

    return records


def save_raw(name: str, records: list, ingested_at: str) -> Path:
    """Save records as a timestamped JSON file."""
    filename = RAW_DIR / f"{name}_{ingested_at}.json"
    payload = {
        "_metadata": {
            "source": "dummyjson",
            "endpoint": name,
            "ingested_at": ingested_at,
            "record_count": len(records),
        },
        "records": records,
    }
    with open(filename, "w") as f:
        json.dump(payload, f, indent=2)
    print(f"  Saved {len(records)} records → {filename}")
    return filename


def run():
    ingested_at = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    print(f"\nIngestion run: {ingested_at}")

    results = {}
    for name, path in ENDPOINTS.items():
        print(f"\nFetching: {name}")
        try:
            records = fetch_all(path)
            filepath = save_raw(name, records, ingested_at)
            results[name] = {"count": len(records), "file": str(filepath)}
        except Exception as e:
            print(f"  ERROR on {name}: {e}")
            results[name] = {"count": 0, "error": str(e)}

    print("\n--- Ingestion summary ---")
    for name, info in results.items():
        print(f"  {name}: {info}")

    return results


if __name__ == "__main__":
    run()