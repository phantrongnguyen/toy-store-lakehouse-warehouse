import os
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

def get_postgres_engine() -> Engine:
    """
    Tạo và trả về đối tượng Engine kết nối tới PostgreSQL sử dụng các biến môi trường.
    """
    db_user = os.getenv("DB_USER", "postgres")
    db_password = os.getenv("DB_PASSWORD", "nguyen")
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME", "toy_store_db")
    
    # Chuỗi URI kết nối PostgreSQL
    connection_uri = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    
    # Tạo engine kết nối
    engine = create_engine(connection_uri)
    return engine
