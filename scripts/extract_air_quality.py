import os  # Provides functions for interacting with the operating system, such as environment variables and file paths
import requests  # Allows sending HTTP requests easily, used here to call the WAQI API
import pandas as pd  # Powerful data analysis and manipulation library, used for working with tabular data (DataFrames)
from sqlalchemy import create_engine  # SQL toolkit and Object Relational Mapper, used to connect to databases
from datetime import datetime  # Provides classes for manipulating dates and times
from dotenv import load_dotenv  # Loads environment variables from a .env file into the environment

load_dotenv()

# Configuration
API_KEY = os.getenv('WAQI_API_KEY')
CITIES = ['New York', 'London', 'Tokyo', 'Sydney', 'Mumbai', 'São Paulo']
DB_CONNECTION = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@postgres:5432/{os.getenv('POSTGRES_DB')}"

def extract_air_quality_data():
    """Extract air quality data from WAQI (World Air Quality Index) API"""
    all_records = []
    
    for city in CITIES:
        url = f"https://api.waqi.info/feed/{city}/?token={API_KEY}"
        
        try:
            response = requests.get(url)
            
            if response.status_code == 200:
                data = response.json()
                
                if data.get('status') == 'ok':
                    station_data = data.get('data', {})
                    city_info = station_data.get('city', {})
                    city_name = city_info.get('name', city)
                    station_name = city_name.split(',')[0]  # Get location name
                    
                    # Extract country code from city name (e.g., "London, United Kingdom")
                    country_code = 'UN'  # Default
                    if isinstance(city_info.get('name'), str) and ',' in city_info.get('name', ''):
                        parts = city_info.get('name').split(',')
                        if len(parts) > 1:
                            country_code = parts[-1].strip()[:10]  # Take last part, limit to 10 chars
                    
                    # Extract individual pollutant measurements
                    iaqi = station_data.get('iaqi', {})
                    timestamp = station_data.get('time', {}).get('iso', datetime.now().isoformat())
                    
                    # Map of pollutants to extract
                    pollutants = {
                        'pm25': 'pm25',
                        'pm10': 'pm10',
                        'o3': 'o3',
                        'no2': 'no2',
                        'so2': 'so2',
                        'co': 'co'
                    }
                    
                    for param_key, param_name in pollutants.items():
                        if param_key in iaqi:
                            value = iaqi[param_key].get('v')
                            if value is not None:
                                record = {
                                    'city': city,
                                    'country': country_code,
                                    'location_name': station_name[:200],
                                    'parameter': param_name,
                                    'value': float(value),
                                    'unit': 'AQI',  # WAQI uses Air Quality Index
                                    'timestamp': timestamp
                                }
                                all_records.append(record)
                    
                    print(f"✓ Extracted air quality data for {city} ({len([r for r in all_records if r['city'] == city])} measurements)")
                else:
                    print(f"⚠ No data available for {city}")
            else:
                print(f"✗ Failed to get data for {city} (Status: {response.status_code})")
        except Exception as e:
            print(f"✗ Error extracting data for {city}: {str(e)}")
    
    print(f"Total extracted: {len(all_records)} air quality measurements")
    
    if not all_records:
        print("⚠ Warning: No air quality data extracted")
    
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