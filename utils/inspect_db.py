import os
import sys
from sqlalchemy import inspect

# Add root directory to PYTHONPATH
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.db_connector import get_postgres_engine

def inspect_database():
    engine = get_postgres_engine()
    inspector = inspect(engine)
    
    print("=== TABLES IN DATABASE ===")
    tables = inspector.get_table_names()
    if not tables:
        print("No tables found in database.")
        return
        
    for table in tables:
        print(f"\nTable: {table}")
        columns = inspector.get_columns(table)
        for col in columns:
            col_name = col['name']
            col_type = str(col['type'])
            is_nullable = col['nullable']
            print(f"  - {col_name}: {col_type} (Nullable: {is_nullable})")

if __name__ == "__main__":
    inspect_database()
