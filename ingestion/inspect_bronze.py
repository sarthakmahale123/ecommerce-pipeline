import duckdb
import json
from pathlib import Path

conn = duckdb.connect("data/ecommerce.duckdb")

output = {}

for table in ["bronze.products_raw", "bronze.carts_raw", "bronze.users_raw"]:
    schema_name, table_name = table.split(".")
    
    cols = conn.execute(f"DESCRIBE {table}").fetchall()
    rows = conn.execute(f"SELECT * FROM {table} LIMIT 5").fetchall()
    col_names = [c[0] for c in cols]
    col_types = [c[1] for c in cols]
    count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

    output[table_name] = {
        "count": count,
        "columns": [{"name": n, "type": t} for n, t in zip(col_names, col_types)],
        "rows": [dict(zip(col_names, [str(v) for v in row])) for row in rows]
    }

conn.close()

Path("data/bronze_report.json").write_text(json.dumps(output, indent=2))
print("Done — open data/bronze_report.json")