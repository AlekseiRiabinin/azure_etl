import os

def generate_weather_stations():
    stations = [
        {"id": "AKL_AIR", "name": "Auckland Airport", "region": "Auckland", "lat": -37.0085, "lon": 174.7920, "elev": 7.0},
        {"id": "WGN_KEL", "name": "Kelburn, Wellington", "region": "Wellington", "lat": -41.2825, "lon": 174.7669, "elev": 125.0},
        {"id": "CHC_ART", "name": "Christchurch Art Gallery", "region": "Christchurch", "lat": -43.5316, "lon": 172.6366, "elev": 15.0},
        {"id": "HAM_HS", "name": "Hamilton High School", "region": "Hamilton", "lat": -37.7833, "lon": 175.2833, "elev": 40.0},
        {"id": "DUD_MET", "name": "Dunedin MetService", "region": "Dunedin", "lat": -45.8788, "lon": 170.5028, "elev": 1.2}
    ]
    
    sql = "INSERT INTO dim_weather_station VALUES\n"
    values = []
    
    for station in stations:
        values.append(
            f"('{station['id']}', '{station['name']}', '{station['region']}', "
            f"{station['lat']}, {station['lon']}, {station['elev']})"
        )
    
    sql += ",\n".join(values) + ";\n"
    
    os.makedirs("../sql", exist_ok=True)
    with open("../sql/weather_stations_data.sql", "w") as f:
        f.write("-- dim_weather_station table data\n")
        f.write("TRUNCATE TABLE dim_weather_station;\n")
        f.write(sql)

if __name__ == "__main__":
    generate_weather_stations()
    print("Generated weather stations data to ../sql/weather_stations_data.sql")
