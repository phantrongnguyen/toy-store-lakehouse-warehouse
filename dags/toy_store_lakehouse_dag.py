from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# Hàm wrapper để gọi hàm trích xuất
def run_extraction():
    from etls.extract_postgres import extract_table
    tables = [
        "staging_website_sessions",
        "staging_website_pageviews",
        "staging_orders",
        "staging_order_items",
        "staging_products",
        "staging_order_item_refunds"
    ]
    for table in tables:
        extract_table(table)

# Hàm wrapper để gọi hàm làm sạch (Silver)
def run_silver_transformation():
    from etls.transform_duckdb import transform_bronze_to_silver
    transform_bronze_to_silver()

# Hàm wrapper để gọi hàm tổng hợp (Gold)
def run_gold_transformation():
    from etls.transform_duckdb import transform_silver_to_gold
    transform_silver_to_gold()

# Hàm wrapper để nạp dữ liệu Gold ngược lại PostgreSQL phục vụ BI
def run_postgres_loading():
    from etls.load_gold_to_postgres import load_gold_to_postgres
    load_gold_to_postgres()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'toy_store_lakehouse_pipeline',
    default_args=default_args,
    description='Pipeline xử lý dữ liệu End-to-End từ PostgreSQL sang Data Lakehouse',
    schedule='@daily',  # Chạy hàng ngày
    catchup=False,
) as dag:

    # 1. Task trích xuất dữ liệu thô (PostgreSQL -> Bronze)
    task_extract = PythonOperator(
        task_id='extract_postgres_to_bronze',
        python_callable=run_extraction,
    )

    # 2. Task làm sạch dữ liệu (Bronze -> Silver)
    task_silver = PythonOperator(
        task_id='transform_bronze_to_silver',
        python_callable=run_silver_transformation,
    )

    # 3. Task tổng hợp dữ liệu (Silver -> Gold)
    task_gold = PythonOperator(
        task_id='transform_silver_to_gold',
        python_callable=run_gold_transformation,
    )

    # 4. Task nạp dữ liệu phân tích ngược lại PostgreSQL (Gold -> PostgreSQL)
    task_load = PythonOperator(
        task_id='load_gold_to_postgres',
        python_callable=run_postgres_loading,
    )

    # Thiết lập thứ tự chạy
    task_extract >> task_silver >> task_gold >> task_load
