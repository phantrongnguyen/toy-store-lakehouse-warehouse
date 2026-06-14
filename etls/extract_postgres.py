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
    
    print(f"[EXTRACT] Đang kết nối tới PostgreSQL để trích xuất bảng: '{table_name}'...")
    
    try:
        # Sử dụng Pandas đọc dữ liệu từ bảng
        df = pd.read_sql_table(table_name, con=engine)
        
        # Ghi đè dữ liệu ra file Parquet (nén bằng Snappy mặc định thông qua pyarrow)
        df.to_parquet(output_filepath, index=False, engine='pyarrow')
        
        print(f"[SUCCESS] Đã trích xuất {len(df)} dòng của bảng '{table_name}' -> '{output_filepath}'")
        return output_filepath
        
    except Exception as e:
        print(f"[ERROR] Thất bại khi trích xuất bảng '{table_name}': {e}")
        raise e

if __name__ == "__main__":
    # Ví dụ chạy thử trích xuất bảng orders và customers
    # Nhớ thiết lập các biến môi trường trước khi chạy
    # extract_table("staging_orders")
    extract_table("staging_products")
    extract_table("staging_order_item_refunds")
    extract_table("staging_order_items")
    extract_table("staging_website_pageviews")
    extract_table("staging_website_sessions")
