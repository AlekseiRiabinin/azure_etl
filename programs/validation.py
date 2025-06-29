from functools import wraps
from typing import Dict

def validate_reading(func):
    """Decorator to check if kwh_usage is positive."""
    @wraps(func)
    def wrapper(reading: Dict) -> None:
        if reading.get("kwh_usage", 0) <= 0:
            raise ValueError(f"Invalid reading: {reading}")
        return func(reading)
    return wrapper

@validate_reading
def process_reading(reading: Dict) -> Dict:
    """Add derived fields to valid readings."""
    reading["cost"] = reading["kwh_usage"] * 0.28  # NZD/kWh
    return reading

# Usage
try:
    print(process_reading({"meter_id": "MTR_1001", "kwh_usage": 4.5}))   # Works
    print(process_reading({"meter_id": "MTR_1002", "kwh_usage": -1.0}))  # Raises
except ValueError as e:
    print(e)