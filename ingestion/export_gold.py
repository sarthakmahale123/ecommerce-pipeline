import duckdb
from pathlib import Path

conn = duckdb.connect("data/ecommerce.duckdb")
export_dir = Path("data/exports")
export_dir.mkdir(exist_ok=True)

tables = [
    "gold.daily_revenue",
    "gold.top_products",
    "gold.category_performance",
    "silver.dim_products",
    "silver.fct_cart_items",
    "silver.dim_users",
]

for table in tables:
    name = table.split(".")[1]
    path = export_dir / f"{name}.csv"
    conn.execute(f"COPY {table} TO '{path}' (HEADER, DELIMITER ',')")
    count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    print(f"Exported {table} → {path} ({count} rows)")

conn.close()
print("\nAll Gold tables exported to data/exports/")