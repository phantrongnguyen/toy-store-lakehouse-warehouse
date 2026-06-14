import os
import duckdb

def transform_bronze_to_silver():
    """
    Silver Layer (Cleaned / Standardized):
    - Reads raw Parquet files from the Bronze layer.
    - Standardizes column names (created_at).
    - Cleans data, handles null values, casts types.
    - Saves clean Parquet files to data/silver/.
    """
    print("[TRANSFORM] Starting data cleaning (Bronze -> Silver)...")
    
    con = duckdb.connect()
    
    # Create target directories for Silver layer
    tables = [
        "website_sessions",
        "website_pageviews",
        "orders",
        "order_items",
        "products",
        "order_item_refunds"
    ]
    for table in tables:
        os.makedirs(f"data/silver/{table}", exist_ok=True)
        
    try:
        # 1. Clean Website Sessions
        print("- Processing table: website_sessions...")
        con.execute("""
            COPY (
                SELECT 
                    CAST(website_session_id AS INTEGER) AS website_session_id,
                    CAST(created_at AS TIMESTAMP) AS created_at,
                    CAST(user_id AS INTEGER) AS user_id,
                    CAST(is_repeat_session AS BOOLEAN) AS is_repeat_session,
                    LOWER(TRIM(COALESCE(utm_source, 'direct'))) AS utm_source,
                    LOWER(TRIM(COALESCE(utm_campaign, 'none'))) AS utm_campaign,
                    LOWER(TRIM(COALESCE(utm_content, 'none'))) AS utm_content,
                    LOWER(TRIM(COALESCE(device_type, 'unknown'))) AS device_type,
                    TRIM(COALESCE(http_referer, 'direct')) AS http_referer
                FROM 'data/bronze/staging_website_sessions/*.parquet'
            ) TO 'data/silver/website_sessions/website_sessions.parquet' (FORMAT 'PARQUET');
        """)
        
        # 2. Clean Website Pageviews
        print("- Processing table: website_pageviews...")
        con.execute("""
            COPY (
                SELECT 
                    CAST(website_pageview_id AS INTEGER) AS website_pageview_id,
                    CAST(create_at AS TIMESTAMP) AS created_at,  -- Standardize to created_at
                    CAST(website_session_id AS INTEGER) AS website_session_id,
                    LOWER(TRIM(pageview_url)) AS pageview_url
                FROM 'data/bronze/staging_website_pageviews/*.parquet'
            ) TO 'data/silver/website_pageviews/website_pageviews.parquet' (FORMAT 'PARQUET');
        """)
        
        # 3. Clean Orders
        print("- Processing table: orders...")
        con.execute("""
            COPY (
                SELECT 
                    CAST(order_id AS INTEGER) AS order_id,
                    CAST(create_at AS TIMESTAMP) AS created_at,  -- Standardize to created_at
                    CAST(website_session_id AS INTEGER) AS website_session_id,
                    CAST(user_id AS INTEGER) AS user_id,
                    CAST(primary_product_id AS INTEGER) AS primary_product_id,
                    CAST(items_purchased AS INTEGER) AS items_purchased,
                    CAST(price_usd AS DOUBLE) AS price_usd,
                    CAST(cogs_usd AS DOUBLE) AS cogs_usd
                FROM 'data/bronze/staging_orders/*.parquet'
                WHERE price_usd >= 0
            ) TO 'data/silver/orders/orders.parquet' (FORMAT 'PARQUET');
        """)
        
        # 4. Clean Order Items
        print("- Processing table: order_items...")
        con.execute("""
            COPY (
                SELECT 
                    CAST(order_item_id AS INTEGER) AS order_item_id,
                    CAST(create_at AS TIMESTAMP) AS created_at,  -- Standardize to created_at
                    CAST(order_id AS INTEGER) AS order_id,
                    CAST(product_id AS INTEGER) AS product_id,
                    CAST(is_primary_item AS BOOLEAN) AS is_primary_item,
                    CAST(price_usd AS DOUBLE) AS price_usd,
                    CAST(cogs_usd AS DOUBLE) AS cogs_usd
                FROM 'data/bronze/staging_order_items/*.parquet'
                WHERE price_usd >= 0
            ) TO 'data/silver/order_items/order_items.parquet' (FORMAT 'PARQUET');
        """)
        
        # 5. Clean Products
        print("- Processing table: products...")
        con.execute("""
            COPY (
                SELECT 
                    CAST(product_id AS INTEGER) AS product_id,
                    CAST(create_at AS TIMESTAMP) AS created_at,  -- Standardize to created_at
                    TRIM(product_name) AS product_name
                FROM 'data/bronze/staging_products/*.parquet'
            ) TO 'data/silver/products/products.parquet' (FORMAT 'PARQUET');
        """)
        
        # 6. Clean Order Item Refunds
        print("- Processing table: order_item_refunds...")
        con.execute("""
            COPY (
                SELECT 
                    CAST(order_item_refund_id AS INTEGER) AS order_item_refund_id,
                    CAST(create_at AS TIMESTAMP) AS created_at,  -- Standardize to created_at
                    CAST(order_item_id AS INTEGER) AS order_item_id,
                    CAST(order_id AS INTEGER) AS order_id,
                    CAST(refund_amount_usd AS DOUBLE) AS refund_amount_usd
                FROM 'data/bronze/staging_order_item_refunds/*.parquet'
            ) TO 'data/silver/order_item_refunds/order_item_refunds.parquet' (FORMAT 'PARQUET');
        """)
        
        print("[SUCCESS] Completed Silver layer transformations.")
        
    except Exception as e:
        print(f"[ERROR] Silver layer transformation failed: {e}")
        raise e


def transform_silver_to_gold():
    """
    Gold Layer (Business / Analytical):
    - Reads clean data from the Silver layer.
    - Builds a Star Schema (Fact and Dimension tables).
    - Creates Business Marts ready for reporting and analysis.
    """
    print("[TRANSFORM] Starting modeling and analytics (Silver -> Gold)...")
    
    con = duckdb.connect()
    
    # Create target directories for Gold layer
    gold_marts = [
        "fact_order_items",
        "dim_products",
        "dim_website_sessions",
        "mart_product_performance",
        "mart_session_conversion"
    ]
    for mart in gold_marts:
        os.makedirs(f"data/gold/{mart}", exist_ok=True)
        
    try:
        # 1. Fact Table: fact_order_items (joins orders, items, and refunds)
        print("- Building Fact table: fact_order_items...")
        con.execute("""
            COPY (
                SELECT 
                    oi.order_item_id,
                    oi.order_id,
                    o.created_at AS order_date,
                    o.website_session_id,
                    o.user_id,
                    oi.product_id,
                    oi.is_primary_item,
                    oi.price_usd AS revenue_usd,
                    oi.cogs_usd,
                    COALESCE(r.refund_amount_usd, 0.0) AS refund_amount_usd,
                    (oi.price_usd - oi.cogs_usd - COALESCE(r.refund_amount_usd, 0.0)) AS net_profit_usd
                FROM 'data/silver/order_items/*.parquet' oi
                JOIN 'data/silver/orders/*.parquet' o ON oi.order_id = o.order_id
                LEFT JOIN 'data/silver/order_item_refunds/*.parquet' r ON oi.order_item_id = r.order_item_id
            ) TO 'data/gold/fact_order_items/fact_order_items.parquet' (FORMAT 'PARQUET');
        """)
        
        # 2. Dimension Table: dim_products
        print("- Building Dimension table: dim_products...")
        con.execute("""
            COPY (
                SELECT product_id, product_name, created_at FROM 'data/silver/products/*.parquet'
            ) TO 'data/gold/dim_products/dim_products.parquet' (FORMAT 'PARQUET');
        """)
        
        # 3. Dimension Table: dim_website_sessions
        print("- Building Dimension table: dim_website_sessions...")
        con.execute("""
            COPY (
                SELECT 
                    website_session_id, 
                    created_at AS session_start_time, 
                    user_id, 
                    is_repeat_session, 
                    utm_source, 
                    utm_campaign, 
                    utm_content, 
                    device_type, 
                    http_referer 
                FROM 'data/silver/website_sessions/*.parquet'
            ) TO 'data/gold/dim_website_sessions/dim_website_sessions.parquet' (FORMAT 'PARQUET');
        """)
        
        # 4. Mart: Product Performance (mart_product_performance)
        print("- Generating Mart table: mart_product_performance...")
        con.execute("""
            COPY (
                SELECT 
                    p.product_id,
                    p.product_name,
                    COUNT(f.order_item_id) AS total_units_sold,
                    SUM(f.revenue_usd) AS gross_revenue_usd,
                    SUM(f.refund_amount_usd) AS total_refund_usd,
                    SUM(f.net_profit_usd) AS net_profit_usd
                FROM 'data/gold/dim_products/*.parquet' p
                LEFT JOIN 'data/gold/fact_order_items/*.parquet' f ON p.product_id = f.product_id
                GROUP BY p.product_id, p.product_name
            ) TO 'data/gold/mart_product_performance/mart_product_performance.parquet' (FORMAT 'PARQUET');
        """)
        
        # 5. Mart: Session Conversion by Marketing Channel & Device (mart_session_conversion)
        print("- Generating Mart table: mart_session_conversion...")
        con.execute("""
            COPY (
                WITH session_metrics AS (
                    SELECT 
                        utm_source,
                        utm_campaign,
                        device_type,
                        COUNT(website_session_id) AS total_sessions
                    FROM 'data/gold/dim_website_sessions/*.parquet'
                    GROUP BY utm_source, utm_campaign, device_type
                ),
                order_metrics AS (
                    SELECT 
                        s.utm_source,
                        s.utm_campaign,
                        s.device_type,
                        COUNT(DISTINCT f.order_id) AS total_orders
                    FROM 'data/gold/fact_order_items/*.parquet' f
                    JOIN 'data/gold/dim_website_sessions/*.parquet' s ON f.website_session_id = s.website_session_id
                    GROUP BY s.utm_source, s.utm_campaign, s.device_type
                )
                SELECT 
                    sm.utm_source,
                    sm.utm_campaign,
                    sm.device_type,
                    sm.total_sessions,
                    COALESCE(om.total_orders, 0) AS total_orders,
                    ROUND(COALESCE(om.total_orders, 0) * 100.0 / sm.total_sessions, 2) AS conversion_rate_percent
                FROM session_metrics sm
                LEFT JOIN order_metrics om 
                  ON sm.utm_source = om.utm_source 
                 AND sm.utm_campaign = om.utm_campaign 
                 AND sm.device_type = om.device_type
            ) TO 'data/gold/mart_session_conversion/mart_session_conversion.parquet' (FORMAT 'PARQUET');
        """)
        
        print("[SUCCESS] Completed Gold layer transformations.")
        
    except Exception as e:
        print(f"[ERROR] Gold layer transformation failed: {e}")
        raise e

if __name__ == "__main__":
    transform_bronze_to_silver()
    transform_silver_to_gold()
