import duckdb

conn = duckdb.connect("data/ecommerce.duckdb")

print("\n=== Row Counts ===")
for table in ["bronze.products_raw", "bronze.carts_raw", "bronze.users_raw"]:
    count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    print(f"  {table}: {count} rows")

print("\n=== Sample Product (nested JSON preserved) ===")
row = conn.execute("""
    SELECT id, title, category, price, dimensions, 
           LEFT(reviews, 120) AS reviews_preview
    FROM bronze.products_raw LIMIT 1
""").fetchdf()
print(row.to_string())

print("\n=== Sample Cart (products array) ===")
row = conn.execute("""
    SELECT id, user_id, total, LEFT(products, 150) AS products_preview
    FROM bronze.carts_raw LIMIT 1
""").fetchdf()
print(row.to_string())

print("\n=== Null Check on Products ===")
result = conn.execute("""
    SELECT 
        COUNT(*) AS total,
        COUNT(id) AS has_id,
        COUNT(title) AS has_title,
        COUNT(price) AS has_price,
        COUNT(category) AS has_category
    FROM bronze.products_raw
""").fetchdf()
print(result.to_string())

conn.close()