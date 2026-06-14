import os
import sys
import pandas as pd

# Add root folder to PYTHONPATH
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.db_connector import get_postgres_engine

def load_gold_to_postgres():
    """
    Reverse ETL: Loads processed Gold analytical tables from Parquet back into PostgreSQL.
    This allows standard SQL-based BI tools like Metabase to query the final reports.
    """
    engine = get_postgres_engine()
    
    # Map table name to Parquet file path
    marts = {
        "gold_fact_order_items": "data/gold/fact_order_items/fact_order_items.parquet",
        "gold_dim_products": "data/gold/dim_products/dim_products.parquet",
        "gold_dim_website_sessions": "data/gold/dim_website_sessions/dim_website_sessions.parquet",
        "gold_mart_product_performance": "data/gold/mart_product_performance/mart_product_performance.parquet",
        "gold_mart_session_conversion": "data/gold/mart_session_conversion/mart_session_conversion.parquet"
    }
    
    print("[LOAD] Starting to load Gold analytical tables back to PostgreSQL...")
    
    for table_name, parquet_path in marts.items():
        if os.path.exists(parquet_path):
            print(f"- Loading {parquet_path} into table '{table_name}'...")
            try:
                df = pd.read_parquet(parquet_path)
                # Write to database (replace if exists)
                df.to_sql(table_name, con=engine, if_exists='replace', index=False)
                print(f"  [SUCCESS] Loaded {len(df)} rows into table '{table_name}'")
            except Exception as e:
                print(f"  [ERROR] Failed to load table '{table_name}': {e}")
        else:
            print(f"  [WARNING] Parquet file not found: {parquet_path}")
            
    print("[LOAD] Gold tables loading completed.")

if __name__ == "__main__":
    load_gold_to_postgres()
