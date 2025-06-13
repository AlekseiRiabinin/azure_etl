import random
from datetime import datetime, timedelta
import os

def generate_meter_data(num_meters=1000):
    manufacturers = ["EML", "Orion", "Advanced Metering", "Itron"]
    meter_types = ["SMART_V1", "SMART_V2", "SMART_V3", "LEGACY"]
    regions = ["Auckland", "Wellington", "Christchurch", "Hamilton", "Dunedin"]
    
    sql = "INSERT INTO dim_meter VALUES\n"
    values = []
    
    for i in range(1, num_meters + 1):
        meter_id = f"MTR_{10000 + i}"
        customer_id = f"CUST_{1000 + i}"
        install_date = datetime.now() - timedelta(days=random.randint(1, 365*5))
        meter_type = random.choice(meter_types)
        manufacturer = random.choice(manufacturers)
        firmware = f"FW{random.randint(1, 10)}.{random.randint(0, 9)}"
        calibration = install_date + timedelta(days=random.randint(180, 540))
        grid_ref = f"NZTM2000:{random.randint(100000, 200000)},{random.randint(5000000, 6000000)}"
        
        values.append(
            f"('{meter_id}', '{customer_id}', '{install_date.date()}', "
            f"'{meter_type}', '{manufacturer}', '{firmware}', "
            f"'{calibration.date()}', {random.choice([True, False])}, "
            f"'{grid_ref}')"
        )
    
    sql += ",\n".join(values) + ";\n"
    
    os.makedirs("../sql", exist_ok=True)
    with open("../sql/meter_data.sql", "w") as f:
        f.write("-- dim_meter table data\n")
        f.write("TRUNCATE TABLE dim_meter;\n")
        f.write(sql)

if __name__ == "__main__":
    generate_meter_data()
    print("Generated meter data to ../sql/meter_data.sql")
