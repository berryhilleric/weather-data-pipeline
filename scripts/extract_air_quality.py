import os  # Provides functions for interacting with the operating system, such as environment variables and file paths
import requests  # Allows sending HTTP requests easily, used here to call the OpenAQ API
import pandas as pd  # Powerful data analysis and manipulation library, used for working with tabular data (DataFrames)
from sqlalchemy import create_engine  # SQL toolkit and Object Relational Mapper, used to connect to databases
from datetime import datetime, timedelta  # Provides classes for manipulating dates and times
from dotenv import load_dotenv  # Loads environment variables from a .env file into the environment

load_dotenv()

DB_CONNECTION = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@postgres:5432/{os.getenv('POSTGRES_DB')}"

def extract_air_quality_data():
    """Extract air quality data from OpenAQ API"""
    cities = [
        {'city': 'New York', 'country': 'US'},
        {'city': 'London', 'country': 'GB'},
        {'city': 'Tokyo', 'country': 'JP'}
    ]
    
    all_records = []
    
    for location in cities:
        url = f"https://api.openaq.org/v2/latest?limit=100&city={location['city']}&country={location['country']}"
        response = requests.get(url)
        
        if response.status_code == 200:
            data = response.json()
            
            for result in data.get('results', []):
                for measurement in result.get('measurements', []):
                    record = {
                        'city': location['city'],
                        'country': location['country'],
                        'parameter': measurement['parameter'],
                        'value': measurement['value'],
                        'unit': measurement['unit'],
                        'timestamp': measurement['lastUpdated']
                    }
                    all_records.append(record)
            
            print(f"✓ Extracted air quality data for {location['city']}")
        else:
            print(f"✗ Failed to get data for {location['city']}")
    
    return pd.DataFrame(all_records)

def load_to_database(df, table_name):
    """Load data to PostgreSQL"""
    engine = create_engine(DB_CONNECTION)
    df.to_sql(table_name, engine, schema='raw', if_exists='append', index=False)
    print(f"✓ Loaded {len(df)} records to {table_name}")

if __name__ == "__main__":
    print("Starting air quality data extraction...")
    df = extract_air_quality_data()
    if not df.empty:
        load_to_database(df, 'air_quality_data')
    print("Extraction complete!")