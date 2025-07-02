from airflow.decorators import dag, task
from datetime import datetime
import pandas as pd
import requests
import os

cities = [
    {"name": "Paris", "latitude": 48.85, "longitude": 2.35},
    {"name": "London", "latitude": 51.51, "longitude": -0.13},
    {"name": "Berlin", "latitude": 52.52, "longitude": 13.41}
]

@dag(
    dag_id="weather_pipeline",
    start_date=datetime(2025, 7, 1),
    schedule="0 8 * * *",
    catchup=False,
    tags=["weather", "etl"]
)
def weather_etl_pipeline():
    
    @task
    def extract_weather_data():
        print("Extracting weather data...")
        weather_data = []
        
        for city in cities:
            url = f"https://api.open-meteo.com/v1/forecast?latitude={city['latitude']}&longitude={city['longitude']}&current_weather=true"
            response = requests.get(url)
            data = response.json()
            
            current = data['current_weather']
            
            weather_record = {
                'city': city['name'],
                'temperature': current['temperature'],
                'windspeed': current['windspeed'], 
                'weather_code': current['weathercode'],
                'timestamp': datetime.now().isoformat()
            }
            
            weather_data.append(weather_record)
            print(f"{city['name']}: {weather_record['temperature']}°C")
                
        return weather_data
    
    @task
    def transform_load_data(raw_data):
        print("Transforming and loading data...")
        
        df = pd.DataFrame(raw_data)
        df['extraction_date'] = datetime.now().date()
        df['temperature_fahrenheit'] = (df['temperature'] * 9/5) + 32
        
        data_path = "/opt/airflow/data"
        os.makedirs(data_path, exist_ok=True)
        csv_path = os.path.join(data_path, "weather_data.csv")
        
        if os.path.exists(csv_path):
            df.to_csv(csv_path, mode='a', header=False, index=False)
        else:
            df.to_csv(csv_path, index=False)
        
        return f"Successfully processed {len(df)} records"
    
    raw_weather = extract_weather_data()
    result = transform_load_data(raw_weather)

    raw_weather >> result

weather_dag = weather_etl_pipeline()