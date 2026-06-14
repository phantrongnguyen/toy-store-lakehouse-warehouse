import duckdb

# Kết nối DuckDB
con = duckdb.connect()

# Đọc cấu trúc các cột (schema) của file Parquet vừa tải
schema = con.execute("DESCRIBE SELECT * FROM 'data/bronze/staging_orders/*.parquet'").df()
print("--- Cấu trúc các cột ---")
print(schema[['column_name', 'column_type']])

# Xem thử 5 dòng dữ liệu đầu tiên
data_preview = con.execute("SELECT * FROM 'data/bronze/staging_orders/*.parquet' LIMIT 5").df()
print("\n--- Bản xem trước dữ liệu ---")
print(data_preview)