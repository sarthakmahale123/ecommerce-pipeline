import duckdb

conn = duckdb.connect("data/ecommerce.duckdb")

print("=== DAILY REVENUE ===")
print(conn.execute("SELECT * FROM gold.daily_revenue").fetchdf().to_string())

print("\n=== TOP 10 PRODUCTS BY REVENUE ===")
print(conn.execute("""
    SELECT product_id, product_title, category,
           times_ordered, total_qty_sold, net_revenue
    FROM gold.top_products
    LIMIT 10
""").fetchdf().to_string())

print("\n=== CATEGORY PERFORMANCE ===")
print(conn.execute("""
    SELECT category, product_count, total_qty_sold,
           net_revenue, avg_discount_pct, avg_review_rating
    FROM gold.category_performance
""").fetchdf().to_string())

conn.close()