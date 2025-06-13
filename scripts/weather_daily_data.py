import uuid
import random
import os
from datetime import datetime, timedelta

def generate_weather_daily(start_date="2023-01-01", end_date="2024-06-20"):
    stations = ["AKL_AIR", "WGN_KEL", "CHC_ART", "HAM_HS", "DUD_MET"]
    regions = ["Auckland", "Wellington", "Christchurch", "Hamilton", "Dunedin"]
    
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    date_diff = (end_dt - start_dt).days
    
    sql = "INSERT INTO fact_weather_daily VALUES\n"
    values = []
    
    for i in range(date_diff + 1):
        current_date = start_dt + timedelta(days=i)
        for station, region in zip(stations, regions):
            observation_id = str(uuid.uuid4())
            
            # NZ seasonal temperature ranges
            if region == "Auckland":
                max_temp = round(random.uniform(15.0, 25.0), 1)
                min_temp = round(random.uniform(8.0, 15.0), 1)
            elif region == "Wellington":
                max_temp = round(random.uniform(12.0, 20.0), 1)
                min_temp = round(random.uniform(6.0, 12.0), 1)
            else:  # South Island
                max_temp = round(random.uniform(10.0, 18.0), 1)
                min_temp = round(random.uniform(3.0, 10.0), 1)
            
            mean_temp = round((max_temp + min_temp) / 2, 1)
            rainfall = round(random.uniform(0.0, 15.0), 1) if random.random() > 0.7 else 0.0
            sunshine = round(random.uniform(0.0, 10.0), 1)
            wind_speed = round(random.uniform(5.0, 40.0), 1)
            
            values.append(
                f"('{observation_id}', '{station}', '{current_date.date()}', "
                f"{max_temp}, {min_temp}, {mean_temp}, {rainfall}, "
                f"{sunshine}, {wind_speed}, '{region}')"
            )
    
    sql += ",\n".join(values) + ";\n"
    
    os.makedirs("../sql", exist_ok=True)
    with open("../sql/weather_daily_data.sql", "w") as f:
        f.write("-- fact_weather_daily table data\n")
        f.write("TRUNCATE TABLE fact_weather_daily;\n")
        f.write(sql)

if __name__ == "__main__":
    generate_weather_daily()
    print("Generated weather daily data to ../sql/weather_daily_data.sql")
