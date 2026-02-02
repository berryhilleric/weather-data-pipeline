import os  # Provides functions for interacting with the operating system, such as environment variables and file paths
import requests  # Allows sending HTTP requests easily, used here to call the OpenWeatherMap API
import pandas as pd  # Powerful data analysis and manipulation library, used for working with tabular data (DataFrames)
from sqlalchemy import create_engine  # SQL toolkit and Object Relational Mapper, used to connect to databases
from datetime import datetime  # Provides classes for manipulating dates and times
from dotenv import load_dotenv  # Loads environment variables from a .env file into the environment

load_dotenv()

# Configuration
API_KEY = os.getenv('OPENWEATHER_API_KEY')
CITIES = ['New York', 'London', 'Tokyo', 'Sydney', 'Mumbai', 'São Paulo']
DB_CONNECTION = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@postgres:5432/{os.getenv('POSTGRES_DB')}"
#DB_CONNECTION = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@localhost:5432/{os.getenv('POSTGRES_DB')}"

def extract_weather_data():
    """Extract weather data from OpenWeatherMap API"""
    weather_records = []
    
    for city in CITIES:
        url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
        response = requests.get(url)
        
        if response.status_code == 200:
            data = response.json()
            record = {
                'city': data['name'],
                'country': data['sys']['country'],
                'temperature': data['main']['temp'],
                'feels_like': data['main']['feels_like'],
                'humidity': data['main']['humidity'],
                'pressure': data['main']['pressure'],
                'wind_speed': data['wind']['speed'],
                'weather_description': data['weather'][0]['description'],
                'timestamp': datetime.fromtimestamp(data['dt'])
            }
            weather_records.append(record)
            print(f"✓ Extracted weather data for {city}")
        else:
            print(f"✗ Failed to get data for {city}")
    
    return pd.DataFrame(weather_records)

def load_to_database(df, table_name):
    """Load data to PostgreSQL"""
    engine = create_engine(DB_CONNECTION)
    df.to_sql(table_name, engine, schema='raw', if_exists='append', index=False)
    print(f"✓ Loaded {len(df)} records to {table_name}")
    
 # __name__ is a special built-in variable. value "__main__" indicates that the script is being run directly, not imported as a module.
 # This block runs only if the script is executed directly, not when imported as a module
if __name__ == "__main__":
    print("Starting weather data extraction...")
    df = extract_weather_data()
    load_to_database(df, 'weather_data')
    print("Extraction complete!")