import os
import sys
import pandas as pd
from sqlalchemy import text

# Add root folder to PYTHONPATH
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.db_connector import get_postgres_engine

def load_gold_to_postgres(bucket_name: str = "toy-store-lakehouse"):
    """
    Reverse ETL: Loads processed Gold analytical tables from MinIO (Parquet) back into PostgreSQL.
    Ensures zero-downtime by using TRUNCATE & INSERT instead of DROP TABLE (replace).
    """
    engine = get_postgres_engine()
    
    # Map table name to MinIO Parquet file path
    marts = {
        "gold_fact_order_items": f"s3://{bucket_name}/gold/fact_order_items/fact_order_items.parquet",
        "gold_dim_products": f"s3://{bucket_name}/gold/dim_products/dim_products.parquet",
        "gold_dim_website_sessions": f"s3://{bucket_name}/gold/dim_website_sessions/dim_website_sessions.parquet",
        "gold_mart_product_performance": f"s3://{bucket_name}/gold/mart_product_performance/mart_product_performance.parquet",
        "gold_mart_session_conversion": f"s3://{bucket_name}/gold/mart_session_conversion/mart_session_conversion.parquet"
    }
    
    # MinIO storage options for Pandas
    s3_access_key = os.getenv("AWS_ACCESS_KEY_ID", "minio_admin")
    s3_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "minio_password")
    s3_endpoint = os.getenv("AWS_ENDPOINT_URL", "http://localhost:9000")
    
    storage_options = {
        "key": s3_access_key,
        "secret": s3_secret_key,
        "client_kwargs": {
            "endpoint_url": s3_endpoint
        }
    }
    
    print("[LOAD] Starting to load Gold analytical tables back to PostgreSQL from MinIO...")
    
    for table_name, s3_path in marts.items():
        print(f"- Loading {s3_path} into Postgres table '{table_name}'...")
        try:
            # Read from MinIO Parquet
            df = pd.read_parquet(s3_path, storage_options=storage_options)
            
            # Write to database inside a single transaction to maintain integrity and prevent metadata drop
            with engine.begin() as conn:
                # Check if table already exists in database
                table_exists = conn.execute(text(f"""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' AND table_name = '{table_name}'
                    );
                """)).scalar()
                
                if table_exists:
                    # Truncate existing data and append new data to keep schema/grants/indexes intact
                    conn.execute(text(f"TRUNCATE TABLE {table_name};"))
                    df.to_sql(table_name, con=conn, if_exists='append', index=False)
                    print(f"  [SUCCESS] Truncated and loaded {len(df)} rows into existing table '{table_name}'")
                else:
                    # Create table for the first time (fallback to replace)
                    df.to_sql(table_name, con=conn, if_exists='replace', index=False)
                    print(f"  [SUCCESS] Created table and loaded {len(df)} rows into new table '{table_name}'")
                    
        except Exception as e:
            print(f"  [ERROR] Failed to load table '{table_name}': {e}")
            raise e
            
    print("[LOAD] Gold tables loading completed.")

if __name__ == "__main__":
    load_gold_to_postgres()
