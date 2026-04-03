import json
import os
from pathlib import Path
from datetime import datetime, timezone
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

RAW_DIR = Path("data/raw")


def get_connection():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema="BRONZE",
        role=os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
    )


def load_products(cursor, records: list, ingested_at: str):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS BRONZE.products_raw (
            id                   INTEGER,
            title                VARCHAR,
            description          VARCHAR,
            category             VARCHAR,
            price                FLOAT,
            discount_percentage  FLOAT,
            rating               FLOAT,
            stock                INTEGER,
            tags                 VARCHAR,        -- JSON array as string
            brand                VARCHAR,
            sku                  VARCHAR,
            weight               FLOAT,
            dimensions           VARCHAR,        -- JSON object as string
            warranty_information VARCHAR,
            shipping_information VARCHAR,
            availability_status  VARCHAR,
            reviews              VARCHAR,        -- JSON array as string
            return_policy        VARCHAR,
            minimum_order_qty    INTEGER,
            meta                 VARCHAR,        -- JSON object as string
            thumbnail            VARCHAR,
            _ingested_at         TIMESTAMP_NTZ
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
    cursor.executemany(
        """INSERT INTO BRONZE.products_raw VALUES
        (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
        rows,
    )
    print(f"  Loaded {len(rows)} products into BRONZE.products_raw")


def load_carts(cursor, records: list, ingested_at: str):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS BRONZE.carts_raw (
            id                INTEGER,
            user_id           INTEGER,
            total             FLOAT,
            discounted_total  FLOAT,
            total_products    INTEGER,
            total_quantity    INTEGER,
            products          VARCHAR,   -- JSON array as string
            _ingested_at      TIMESTAMP_NTZ
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
    cursor.executemany(
        """INSERT INTO BRONZE.carts_raw VALUES
        (%s,%s,%s,%s,%s,%s,%s,%s)""",
        rows,
    )
    print(f"  Loaded {len(rows)} carts into BRONZE.carts_raw")


def load_users(cursor, records: list, ingested_at: str):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS BRONZE.users_raw (
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
            height       FLOAT,
            weight       FLOAT,
            eye_color    VARCHAR,
            hair         VARCHAR,        -- JSON object as string
            address      VARCHAR,        -- JSON object as string
            university   VARCHAR,
            company      VARCHAR,        -- JSON object as string
            role         VARCHAR,
            -- sensitive fields stored but not exposed in Silver
            ssn          VARCHAR,
            ein          VARCHAR,
            _ingested_at TIMESTAMP_NTZ
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
    cursor.executemany(
        """INSERT INTO BRONZE.users_raw VALUES
        (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
        rows,
    )
    print(f"  Loaded {len(rows)} users into BRONZE.users_raw")


def run(raw_files: dict):
    conn = get_connection()
    cursor = conn.cursor()

    # Ensure schemas exist
    cursor.execute("CREATE SCHEMA IF NOT EXISTS BRONZE")
    cursor.execute("CREATE SCHEMA IF NOT EXISTS SILVER")
    cursor.execute("CREATE SCHEMA IF NOT EXISTS GOLD")

    ingested_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    for name, info in raw_files.items():
        if "error" in info:
            print(f"Skipping {name} — extract failed")
            continue

        with open(info["file"]) as f:
            payload = json.load(f)

        records = payload["records"]
        print(f"\nLoading: {name} ({len(records)} records)")

        if name == "products":
            load_products(cursor, records, ingested_at)
        elif name == "carts":
            load_carts(cursor, records, ingested_at)
        elif name == "users":
            load_users(cursor, records, ingested_at)

    conn.commit()
    cursor.close()
    conn.close()
    print("\nBronze load complete.")


if __name__ == "__main__":
    # For standalone testing — load the most recent files
    from extract import run as extract_run
    raw_files = extract_run()
    run(raw_files)