import os
import sys
import duckdb

# Thêm thư mục gốc vào PYTHONPATH để import được utils (nếu cần dùng db_connector)
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def extract_table(table_name: str, bucket_name: str = "toy-store-lakehouse") -> str:
    """
    Trích xuất toàn bộ bảng từ PostgreSQL và lưu trực tiếp lên MinIO (Bronze Layer) bằng DuckDB.
    
    Args:
        table_name (str): Tên bảng cần lấy dữ liệu (ví dụ: 'staging_orders')
        bucket_name (str): Tên bucket MinIO lưu trữ
        
    Returns:
        str: Đường dẫn S3 URI của file Parquet đã lưu
    """
    db_user = os.getenv("DB_USER", "postgres")
    db_password = os.getenv("DB_PASSWORD", "nguyen")
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME", "toy_store_db")
    
    # Cấu hình MinIO
    s3_access_key = os.getenv("AWS_ACCESS_KEY_ID", "minio_admin")
    s3_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "minio_password")
    s3_endpoint = os.getenv("AWS_ENDPOINT_URL", "http://localhost:9000")
    
    # Xử lý s3_endpoint cho DuckDB (loại bỏ tiền tố http/https)
    s3_endpoint_host = s3_endpoint.replace("http://", "").replace("https://", "")
    use_ssl = "true" if s3_endpoint.startswith("https://") else "false"
    
    s3_path = f"s3://{bucket_name}/bronze/{table_name}/{table_name}.parquet"
    
    print(f"[EXTRACT] Connecting to PostgreSQL ({db_host}:{db_port}) -> Exporting '{table_name}' to MinIO...")
    
    con = duckdb.connect()
    try:
        # Load extensions
        con.execute("INSTALL postgres; LOAD postgres;")
        con.execute("INSTALL httpfs; LOAD httpfs;")
        
        # Configure S3/MinIO
        con.execute(f"SET s3_endpoint='{s3_endpoint_host}';")
        con.execute(f"SET s3_access_key_id='{s3_access_key}';")
        con.execute(f"SET s3_secret_access_key='{s3_secret_key}';")
        con.execute(f"SET s3_use_ssl={use_ssl};")
        con.execute("SET s3_url_style='path';")
        
        # Kết nối và đính kèm Postgres
        pg_conn_str = f"dbname={db_name} user={db_user} password={db_password} host={db_host} port={db_port}"
        con.execute(f"ATTACH '{pg_conn_str}' AS pg (TYPE postgres);")
        
        # Sử dụng COPY để stream thẳng lên S3 Parquet
        con.execute(f"COPY (SELECT * FROM pg.public.{table_name}) TO '{s3_path}' (FORMAT 'PARQUET');")
        
        # Đếm số lượng dòng trích xuất thành công
        row_count = con.execute(f"SELECT COUNT(*) FROM '{s3_path}'").fetchone()[0]
        print(f"[SUCCESS] Extracted {row_count} rows for table '{table_name}' -> '{s3_path}'")
        
        return s3_path
        
    except Exception as e:
        print(f"[ERROR] Failed to extract table '{table_name}': {e}")
        raise e
    finally:
        con.close()

if __name__ == "__main__":
    # Danh sách toàn bộ các bảng staging trong cơ sở dữ liệu Toy Store
    tables = [
        "staging_website_sessions",
        "staging_website_pageviews",
        "staging_orders",
        "staging_order_items",
        "staging_products",
        "staging_order_item_refunds"
    ]
    
    print("=== STARTING EXTRACT: POSTGRESQL -> BRONZE ===")
    for table in tables:
        extract_table(table)
    print("=== BRONZE EXTRACTION COMPLETED SUCCESSFULLY ===")

