# scripts/weather_data.py
import pandas as pd
import random
from datetime import date, timedelta

regions = ["Auckland", "Wellington", "Christchurch", "Hamilton", "Dunedin"]

def generate_weather_data(days=90):
    start_date = date.today() - timedelta(days=days)
    dates = [start_date + timedelta(days=i) for i in range(days)]
    
    weather_data = []
    for day in dates:
        for region in regions:
            weather_data.append({
                "date": day.strftime("%Y-%m-%d"),
                "region": region,
                "max_temp_c": round(random.uniform(10, 25), 1),
                "min_temp_c": round(random.uniform(0, 15), 1),
                "rainfall_mm": round(random.uniform(0, 20), 1)
            })
    
    df = pd.DataFrame(weather_data)
    df.to_parquet("data/weather_data.parquet")
    return df

if __name__ == "__main__":
    weather_df = generate_weather_data()
    print(f"Generated weather data ({len(weather_df)} records) to data/weather_data.parquet")
