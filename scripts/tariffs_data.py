import os
from datetime import datetime, timedelta

def generate_tariff_data():
    tariffs = [
        {
            "code": "PEAK",
            "name": "Residential Peak/Off-Peak",
            "desc": "Standard residential tariff with peak and off-peak rates",
            "peak_rate": 0.28,
            "offpeak_rate": 0.18,
            "fixed": 1.20,
            "from_date": "2020-01-01"
        },
        {
            "code": "OFFPEAK",
            "name": "Residential Off-Peak Only",
            "desc": "For customers with off-peak usage only",
            "peak_rate": 0.32,
            "offpeak_rate": 0.15,
            "fixed": 1.00,
            "from_date": "2021-04-01"
        },
        {
            "code": "TIMEOFUSE",
            "name": "Time of Use",
            "desc": "Variable rates based on time of day",
            "peak_rate": 0.35,
            "offpeak_rate": 0.12,
            "fixed": 0.80,
            "from_date": "2022-07-01"
        },
        {
            "code": "LOWUSER",
            "name": "Low User",
            "desc": "For low consumption households",
            "peak_rate": 0.30,
            "offpeak_rate": 0.20,
            "fixed": 0.60,
            "from_date": "2023-01-01"
        }
    ]
    
    sql = "INSERT INTO dim_tariff VALUES\n"
    values = []
    
    for tariff in tariffs:
        values.append(
            f"('{tariff['code']}', '{tariff['name']}', '{tariff['desc']}', "
            f"{tariff['peak_rate']}, {tariff['offpeak_rate']}, {tariff['fixed']}, "
            f"'{tariff['from_date']}', NULL, TRUE)"
        )
    
    sql += ",\n".join(values) + ";\n"
    
    os.makedirs("../sql", exist_ok=True)
    with open("../sql/tariff_data.sql", "w") as f:
        f.write("-- dim_tariff table data\n")
        f.write("TRUNCATE TABLE dim_tariff;\n")
        f.write(sql)

if __name__ == "__main__":
    generate_tariff_data()
    print("Generated tariff data to ../sql/tariff_data.sql")
