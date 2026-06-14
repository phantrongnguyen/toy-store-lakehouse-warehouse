import os
import sys
import pandas as pd

# Thêm thư mục gốc vào PYTHONPATH để import được utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.db_connector import get_postgres_engine

def extract_table(table_name: str, target_dir: str = "data/bronze") -> str:
    """
    Trích xuất toàn bộ bảng từ PostgreSQL và lưu thành file Parquet (Bronze Layer).
    
    Args:
        table_name (str): Tên bảng cần lấy dữ liệu (ví dụ: 'orders', 'customers')
        target_dir (str): Thư mục lưu trữ tầng Bronze
        
    Returns:
        str: Đường dẫn của file Parquet đã được lưu
    """
    engine = get_postgres_engine()
    
    # Định nghĩa đường dẫn đích
    output_dir = os.path.join(target_dir, table_name)
    os.makedirs(output_dir, exist_ok=True)
    output_filepath = os.path.join(output_dir, f"{table_name}.parquet")
    
    print(f"[EXTRACT] Connecting to PostgreSQL to extract table: '{table_name}'...")
    
    try:
        # Sử dụng Pandas đọc dữ liệu từ bảng
        df = pd.read_sql_table(table_name, con=engine)
        
        # Ghi đè dữ liệu ra file Parquet (nén bằng Snappy mặc định thông qua pyarrow)
        df.to_parquet(output_filepath, index=False, engine='pyarrow')
        
        print(f"[SUCCESS] Extracted {len(df)} rows for table '{table_name}' -> '{output_filepath}'")
        return output_filepath
        
    except Exception as e:
        print(f"[ERROR] Failed to extract table '{table_name}': {e}")
        raise e

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
