import random
import os
from datetime import datetime, timedelta

def generate_customer_addresses(num_customers=500):
    regions = {
        "Auckland": ["Queen St", "Dominion Rd", "Ponsonby Rd", "K Rd", "Symonds St"],
        "Wellington": ["Lambton Quay", "Courtenay Pl", "Adelaide Rd", "The Terrace", "Willis St"],
        "Christchurch": ["Colombo St", "Riccarton Rd", "Papanui Rd", "Manchester St", "Cashel St"],
        "Hamilton": ["Victoria St", "Grey St", "Bryce St", "Anglesea St", "Collingwood St"],
        "Dunedin": ["George St", "Princes St", "Stuart St", "Great King St", "Moray Pl"]
    }
    
    sql = "INSERT INTO bridge_customer_address VALUES\n"
    values = []
    
    for i in range(1, num_customers + 1):
        customer_id = f"CUST_{1000 + i}"
        region = random.choice(list(regions.keys()))
        street = random.choice(regions[region])
        address = f"{random.randint(1, 500)} {street}, {region}"
        
        # Generate 1-3 address records per customer
        num_addresses = random.randint(1, 3)
        for addr_num in range(num_addresses):
            start_date = datetime.now() - timedelta(days=random.randint(365*2, 365*5))
            end_date = start_date + timedelta(days=random.randint(180, 365*3)) if addr_num < num_addresses-1 else None
            is_current = end_date is None
            
            values.append(
                f"(DEFAULT, '{customer_id}', '{address}', "
                f"'{start_date.date()}', {f"'{end_date.date()}'" if end_date else 'NULL'}, "
                f"{is_current}, '{region}')"
            )
    
    sql += ",\n".join(values) + ";\n"
    
    os.makedirs("../sql", exist_ok=True)
    with open("../sql/customer_addresses_data.sql", "w") as f:
        f.write("-- bridge_customer_address table data\n")
        f.write("TRUNCATE TABLE bridge_customer_address;\n")
        f.write(sql)

if __name__ == "__main__":
    generate_customer_addresses()
    print("Generated customer addresses data to ../sql/customer_addresses_data.sql")
