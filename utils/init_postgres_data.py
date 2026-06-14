import os
import sys
from datetime import datetime, timedelta
from sqlalchemy import text

# Thêm thư mục gốc vào PYTHONPATH
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.db_connector import get_postgres_engine

def initialize_postgres_data():
    """
    Tạo các bảng staging và nạp dữ liệu mẫu vào PostgreSQL chạy trong Docker.
    """
    engine = get_postgres_engine()
    
    print("[INIT] Đang kết nối tới PostgreSQL để tạo bảng và nạp dữ liệu mẫu...")
    
    # Định nghĩa các câu lệnh SQL khởi tạo bảng
    queries = [
        # 1. Bảng staging_products
        """
        CREATE TABLE IF NOT EXISTS staging_products (
            product_id INT PRIMARY KEY,
            create_at TIMESTAMP,
            product_name VARCHAR(50)
        );
        """,
        # 2. Bảng staging_website_sessions
        """
        CREATE TABLE IF NOT EXISTS staging_website_sessions (
            website_session_id INT PRIMARY KEY,
            created_at TIMESTAMP,
            user_id INT,
            is_repeat_session INT,
            utm_source VARCHAR(50),
            utm_campaign VARCHAR(50),
            utm_content VARCHAR(50),
            device_type VARCHAR(50),
            http_referer VARCHAR(100)
        );
        """,
        # 3. Bảng staging_website_pageviews
        """
        CREATE TABLE IF NOT EXISTS staging_website_pageviews (
            website_pageview_id INT PRIMARY KEY,
            create_at TIMESTAMP,
            website_session_id INT,
            pageview_url VARCHAR(100)
        );
        """,
        # 4. Bảng staging_orders
        """
        CREATE TABLE IF NOT EXISTS staging_orders (
            order_id INT PRIMARY KEY,
            create_at TIMESTAMP,
            website_session_id INT,
            user_id INT,
            primary_product_id INT,
            items_purchased INT,
            price_usd NUMERIC(10, 2),
            cogs_usd NUMERIC(10, 2)
        );
        """,
        # 5. Bảng staging_order_items
        """
        CREATE TABLE IF NOT EXISTS staging_order_items (
            order_item_id INT PRIMARY KEY,
            create_at TIMESTAMP,
            order_id INT,
            product_id INT,
            is_primary_item INT,
            price_usd NUMERIC(10, 2),
            cogs_usd NUMERIC(10, 2)
        );
        """,
        # 6. Bảng staging_order_item_refunds
        """
        CREATE TABLE IF NOT EXISTS staging_order_item_refunds (
            order_item_refund_id INT PRIMARY KEY,
            create_at TIMESTAMP,
            order_item_id INT,
            order_id INT,
            refund_amount_usd NUMERIC(10, 2)
        );
        """
    ]
    
    # Thực thi tạo bảng
    with engine.begin() as conn:
        for query in queries:
            conn.execute(text(query))
        print("[SUCCESS] Đã tạo các bảng staging (nếu chưa tồn tại).")

    # Nạp dữ liệu mẫu
    with engine.begin() as conn:
        # Kiểm tra xem bảng products đã có dữ liệu chưa
        result = conn.execute(text("SELECT COUNT(*) FROM staging_products")).scalar()
        if result > 0:
            print("[INFO] Bảng đã có dữ liệu mẫu. Bỏ qua bước nạp dữ liệu.")
            return

        print("[INIT] Đang nạp dữ liệu mẫu...")
        
        # Thêm sản phẩm
        conn.execute(text("""
            INSERT INTO staging_products (product_id, create_at, product_name) VALUES
            (1, '2026-01-01 09:00:00', 'The Original Mr. Fuzzy'),
            (2, '2026-01-01 09:00:00', 'Mini Mr. Fuzzy'),
            (3, '2026-02-15 09:00:00', 'Love Bear'),
            (4, '2026-03-01 09:00:00', 'Birthday Bear');
        """))
        
        # Thêm website sessions
        conn.execute(text("""
            INSERT INTO staging_website_sessions (website_session_id, created_at, user_id, is_repeat_session, utm_source, utm_campaign, utm_content, device_type, http_referer) VALUES
            (101, '2026-06-10 10:00:00', 1001, 0, 'gsearch', 'nonbrand', 'bar_code', 'desktop', 'https://www.google.com'),
            (102, '2026-06-10 10:05:00', 1002, 0, NULL, NULL, NULL, 'mobile', 'https://www.bing.com'),
            (103, '2026-06-11 11:00:00', 1003, 0, 'bsearch', 'brand', 'banner', 'desktop', 'https://www.bing.com'),
            (104, '2026-06-11 14:00:00', 1001, 1, 'gsearch', 'nonbrand', 'bar_code', 'desktop', 'https://www.google.com'),
            (105, '2026-06-12 09:00:00', 1004, 0, 'social', 'summer_sale', 'ad_post', 'mobile', 'https://www.facebook.com');
        """))
        
        # Thêm website pageviews
        conn.execute(text("""
            INSERT INTO staging_website_pageviews (website_pageview_id, create_at, website_session_id, pageview_url) VALUES
            (201, '2026-06-10 10:00:05', 101, '/home'),
            (202, '2026-06-10 10:01:00', 101, '/products'),
            (203, '2026-06-10 10:02:00', 101, '/cart'),
            (204, '2026-06-10 10:03:00', 101, '/shipping'),
            (205, '2026-06-10 10:05:10', 102, '/home'),
            (206, '2026-06-11 11:00:05', 103, '/home'),
            (207, '2026-06-12 09:00:05', 105, '/home'),
            (208, '2026-06-12 09:01:30', 105, '/products');
        """))
        
        # Thêm orders
        conn.execute(text("""
            INSERT INTO staging_orders (order_id, create_at, website_session_id, user_id, primary_product_id, items_purchased, price_usd, cogs_usd) VALUES
            (501, '2026-06-10 10:03:30', 101, 1001, 1, 1, 19.99, 9.00),
            (502, '2026-06-12 09:05:00', 105, 1004, 3, 2, 45.00, 18.00);
        """))
        
        # Thêm order items
        conn.execute(text("""
            INSERT INTO staging_order_items (order_item_id, create_at, order_id, product_id, is_primary_item, price_usd, cogs_usd) VALUES
            (601, '2026-06-10 10:03:30', 501, 1, 1, 19.99, 9.00),
            (602, '2026-06-12 09:05:00', 502, 3, 1, 25.00, 10.00),
            (603, '2026-06-12 09:05:00', 502, 2, 0, 20.00, 8.00);
        """))
        
        # Thêm order item refunds
        conn.execute(text("""
            INSERT INTO staging_order_item_refunds (order_item_refund_id, create_at, order_item_id, order_id, refund_amount_usd) VALUES
            (701, '2026-06-13 14:00:00', 603, 502, 20.00);
        """))
        
        print("[SUCCESS] Đã nạp dữ liệu mẫu thành công!")

if __name__ == "__main__":
    initialize_postgres_data()
