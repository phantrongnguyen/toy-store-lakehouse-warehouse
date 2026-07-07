import os
import duckdb

def test_data(bucket_name: str = "toy-store-lakehouse"):
    """
    Kiểm tra nhanh kết nối tới MinIO và xem trước dữ liệu tầng Bronze.
    """
    con = duckdb.connect()
    
    # Load and configure HTTPFS for S3
    con.execute("INSTALL httpfs; LOAD httpfs;")
    
    s3_access_key = os.getenv("AWS_ACCESS_KEY_ID", "minio_admin")
    s3_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "minio_password")
    s3_endpoint = os.getenv("AWS_ENDPOINT_URL", "http://localhost:9000")
    
    s3_endpoint_host = s3_endpoint.replace("http://", "").replace("https://", "")
    use_ssl = "true" if s3_endpoint.startswith("https://") else "false"
    
    con.execute(f"SET s3_endpoint='{s3_endpoint_host}';")
    con.execute(f"SET s3_access_key_id='{s3_access_key}';")
    con.execute(f"SET s3_secret_access_key='{s3_secret_key}';")
    con.execute(f"SET s3_use_ssl={use_ssl};")
    con.execute("SET s3_url_style='path';")
    
    s3_path = f"s3://{bucket_name}/bronze/staging_orders/*.parquet"
    
    print(f"--- Connecting to MinIO: {s3_path} ---")
    try:
        schema = con.execute(f"DESCRIBE SELECT * FROM '{s3_path}'").df()
        print("--- Cấu trúc các cột ---")
        print(schema[['column_name', 'column_type']])
        
        data_preview = con.execute(f"SELECT * FROM '{s3_path}' LIMIT 5").df()
        print("\n--- Bản xem trước dữ liệu ---")
        print(data_preview)
    except Exception as e:
        print(f"[ERROR] Failed to query data from MinIO: {e}")
        raise e
    finally:
        con.close()

if __name__ == "__main__":
    test_data()