from meter_readings_data import generate_meter_readings
from billing_data import generate_billing_data
from meters_data import generate_meter_data
from tariffs_data import generate_tariff_data
from weather_stations_data import generate_weather_stations
from customer_addresses_data import generate_customer_addresses
from weather_daily_data import generate_weather_daily

if __name__ == "__main__":
    print("Generating all sample data...")
    generate_weather_stations()
    generate_tariff_data()
    generate_meter_data()
    generate_customer_addresses()
    generate_weather_daily()
    generate_meter_readings()
    generate_billing_data()
    print("All data generation complete!")
