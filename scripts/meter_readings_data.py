import uuid
import random
from datetime import datetime, timedelta
import os

def generate_meter_readings(num_records=10000, start_date="2023-01-01", end_date="2024-06-20"):
    regions = ["Auckland", "Wellington", "Christchurch", "Hamilton", "Dunedin"]
    weather_conditions = ["Sunny", "Rainy", "Cloudy", "Windy", "Stormy"]
    meter_ids = [f"MTR_{10000 + i}" for i in range(1, 1001)]
    customer_ids = [f"CUST_{1000 + i}" for i in range(1, 1001)]
    
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    date_diff = (end_dt - start_dt).days
    
    sql = "INSERT INTO fact_meter_readings VALUES\n"
    values = []
    
    for _ in range(num_records):
        reading_id = str(uuid.uuid4())
        meter_id = random.choice(meter_ids)
        customer_id = f"CUST_{1000 + int(meter_id.split('_')[1]) - 10000}"
        random_days = random.randint(0, date_diff)
        random_seconds = random.randint(0, 86400)
        timestamp = start_dt + timedelta(days=random_days, seconds=random_seconds)
        
        is_weekday = timestamp.weekday() < 5
        hour = timestamp.hour
        is_peak = is_weekday and ((hour >= 7 and hour < 9) or (hour >= 17 and hour < 21))
        
        kwh = round(random.uniform(0.1, 5.0), 4)
        voltage = round(random.uniform(230, 250), 2)
        current = round(kwh * 1000 / voltage, 2) if voltage > 0 else 0
        pf = round(random.uniform(0.85, 0.99), 2)
        region = random.choice(regions)
        weather = random.choice(weather_conditions)
        temp = round(random.uniform(5.0, 25.0), 1)
        
        values.append(
            f"('{reading_id}', '{meter_id}', '{customer_id}', '{timestamp.isoformat()}', "
            f"{kwh}, {voltage}, {current}, {pf}, '{region}', {is_peak}, "
            f"'{weather}', {temp})"
        )
    
    sql += ",\n".join(values) + ";\n"
    
    os.makedirs("../sql", exist_ok=True)
    with open("../sql/meter_readings_data.sql", "w") as f:
        f.write("-- fact_meter_readings table data\n")
        f.write("TRUNCATE TABLE fact_meter_readings;\n")
        f.write(sql)

if __name__ == "__main__":
    generate_meter_readings()
    print("Generated meter readings data to ../sql/meter_readings_data.sql")
