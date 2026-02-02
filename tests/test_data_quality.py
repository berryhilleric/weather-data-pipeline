import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv()

DB_CONNECTION = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@localhost:5432/{os.getenv('POSTGRES_DB')}"

def test_no_null_values():
    """Check for null values in critical columns"""
    engine = create_engine(DB_CONNECTION)
    query = """
        SELECT COUNT(*) as null_count
        FROM raw.weather_data
        WHERE city IS NULL OR temperature IS NULL OR timestamp IS NULL
    """
    result = pd.read_sql(query, engine)
    assert result['null_count'][0] == 0, "Found null values in critical columns"
    print("✓ No null values found")

def test_data_freshness():
    """Check if data is recent"""
    engine = create_engine(DB_CONNECTION)
    query = """
        SELECT MAX(timestamp) as latest_timestamp
        FROM raw.weather_data
    """
    result = pd.read_sql(query, engine)
    # Add your freshness check logic
    print(f"✓ Latest data: {result['latest_timestamp'][0]}")

if __name__ == "__main__":
    test_no_null_values()
    test_data_freshness()