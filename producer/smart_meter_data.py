# scripts/smart_meter_data.py
import json
import random
from datetime import datetime, timedelta
import time

regions = ["Auckland", "Wellington", "Christchurch", "Hamilton", "Dunedin"]
tariff_plans = ["PEAK", "OFFPEAK", "TIME_OF_USE"]

def generate_meter_data(num_records=1000):
    data = []
    for i in range(num_records):
        record = {
            "meter_id": f"{random.choice(regions).upper()}_{random.randint(100, 999)}",
            "timestamp": (datetime.now() - timedelta(minutes=random.randint(0, 1440))).isoformat() + "Z",
            "kwh_usage": round(random.uniform(0.5, 5.0), 2),
            "voltage": random.choice([230, 240]),
            "customer_id": f"CUST_{random.randint(1000, 9999)}",
            "region": random.choice(regions)
        }
        data.append(record)
    return data

if __name__ == "__main__":
    # Generate 100 records and save to JSON file
    meter_data = generate_meter_data(1000)
    with open("data/smart_meter_data.json", "w") as f:
        json.dump(meter_data, f, indent=2)
    
    print("Generated smart meter data to data/smart_meter_data.json")
