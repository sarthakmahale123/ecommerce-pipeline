import json
import duckdb
from pathlib import Path
from datetime import datetime, timezone

RAW_DIR = Path("data/raw")
DB_PATH = "data/ecommerce.duckdb"


def get_connection():
    return duckdb.connect(DB_PATH)


def setup_schemas(conn):
    conn.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    conn.execute("CREATE SCHEMA IF NOT EXISTS silver")
    conn.execute("CREATE SCHEMA IF NOT EXISTS gold")
    print("Schemas ready: bronze, silver, gold")


def load_products(conn, records: list, ingested_at: str):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS bronze.products_raw (
            id                   INTEGER,
            title                VARCHAR,
            description          VARCHAR,
            category             VARCHAR,
            price                DOUBLE,
            discount_percentage  DOUBLE,
            rating               DOUBLE,
            stock                INTEGER,
            tags                 VARCHAR,
            brand                VARCHAR,
            sku                  VARCHAR,
            weight               DOUBLE,
            dimensions           VARCHAR,
            warranty_information VARCHAR,
            shipping_information VARCHAR,
            availability_status  VARCHAR,
            reviews              VARCHAR,
            return_policy        VARCHAR,
            minimum_order_qty    INTEGER,
            meta                 VARCHAR,
            thumbnail            VARCHAR,
            _ingested_at         TIMESTAMP
        )
    """)
    rows = [
        (
            r["id"], r["title"], r["description"], r["category"],
            r["price"], r["discountPercentage"], r["rating"], r["stock"],
            json.dumps(r.get("tags", [])),
            r.get("brand"), r.get("sku"), r.get("weight"),
            json.dumps(r.get("dimensions", {})),
            r.get("warrantyInformation"), r.get("shippingInformation"),
            r.get("availabilityStatus"),
            json.dumps(r.get("reviews", [])),
            r.get("returnPolicy"), r.get("minimumOrderQuantity"),
            json.dumps(r.get("meta", {})),
            r.get("thumbnail"), ingested_at,
        )
        for r in records
    ]
    conn.executemany("INSERT INTO bronze.products_raw VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
    print(f"  Loaded {len(rows)} rows → bronze.products_raw")


def load_carts(conn, records: list, ingested_at: str):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS bronze.carts_raw (
            id                INTEGER,
            user_id           INTEGER,
            total             DOUBLE,
            discounted_total  DOUBLE,
            total_products    INTEGER,
            total_quantity    INTEGER,
            products          VARCHAR,
            _ingested_at      TIMESTAMP
        )
    """)
    rows = [
        (
            r["id"], r["userId"], r["total"], r["discountedTotal"],
            r["totalProducts"], r["totalQuantity"],
            json.dumps(r.get("products", [])), ingested_at,
        )
        for r in records
    ]
    conn.executemany("INSERT INTO bronze.carts_raw VALUES (?,?,?,?,?,?,?,?)", rows)
    print(f"  Loaded {len(rows)} rows → bronze.carts_raw")


def load_users(conn, records: list, ingested_at: str):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS bronze.users_raw (
            id           INTEGER,
            first_name   VARCHAR,
            last_name    VARCHAR,
            maiden_name  VARCHAR,
            age          INTEGER,
            gender       VARCHAR,
            email        VARCHAR,
            phone        VARCHAR,
            username     VARCHAR,
            birth_date   VARCHAR,
            blood_group  VARCHAR,
            height       DOUBLE,
            weight       DOUBLE,
            eye_color    VARCHAR,
            hair         VARCHAR,
            address      VARCHAR,
            university   VARCHAR,
            company      VARCHAR,
            role         VARCHAR,
            ssn          VARCHAR,
            ein          VARCHAR,
            _ingested_at TIMESTAMP
        )
    """)
    rows = [
        (
            r["id"], r["firstName"], r["lastName"], r.get("maidenName"),
            r.get("age"), r.get("gender"), r["email"], r.get("phone"),
            r.get("username"), r.get("birthDate"), r.get("bloodGroup"),
            r.get("height"), r.get("weight"), r.get("eyeColor"),
            json.dumps(r.get("hair", {})),
            json.dumps(r.get("address", {})),
            r.get("university"),
            json.dumps(r.get("company", {})),
            r.get("role"),
            r.get("ssn"), r.get("ein"),
            ingested_at,
        )
        for r in records
    ]
    conn.executemany("INSERT INTO bronze.users_raw VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
    print(f"  Loaded {len(rows)} rows → bronze.users_raw")


def run(raw_files: dict):
    ingested_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    conn = get_connection()
    setup_schemas(conn)

    for name, info in raw_files.items():
        if "error" in info:
            print(f"Skipping {name} — extract failed")
            continue

        with open(info["file"]) as f:
            payload = json.load(f)

        records = payload["records"]
        print(f"\nLoading: {name} ({len(records)} records)")

        if name == "products":
            load_products(conn, records, ingested_at)
        elif name == "carts":
            load_carts(conn, records, ingested_at)
        elif name == "users":
            load_users(conn, records, ingested_at)

    conn.close()
    print("\nBronze load complete.")


if __name__ == "__main__":
    from extract import run as extract_run
    raw_files = extract_run()
    run(raw_files)