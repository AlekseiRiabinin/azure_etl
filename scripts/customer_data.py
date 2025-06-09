# scripts/customer_data.py
import random

regions = {
    "Auckland": ["Queen St", "Dominion Rd", "Ponsonby Rd"],
    "Wellington": ["Lambton Quay", "Courtenay Pl", "Adelaide Rd"],
    "Christchurch": ["Colombo St", "Riccarton Rd", "Papanui Rd"]
}

tariff_plans = ["PEAK", "OFFPEAK", "TIME_OF_USE"]

def generate_customer_data(num_customers=50):
    sql = "INSERT INTO dim_customer VALUES\n"
    values = []
    
    for i in range(1, num_customers + 1):
        region = random.choice(list(regions.keys()))
        street = random.choice(regions[region])
        address = f"{random.randint(1, 500)} {street}, {region}"
        
        values.append(
            f"('CUST_{1000 + i}', '{address}', '{random.choice(tariff_plans)}', '****@****.com')"
        )
    
    sql += ",\n".join(values) + ";"
    return sql

if __name__ == "__main__":
    customer_sql = generate_customer_data()
    with open("sql/customer_data.sql", "w") as f:
        f.write("-- dim_customer table data\n")
        f.write("TRUNCATE TABLE dim_customer;\n")
        f.write(customer_sql)
    
    print("Generated customer data to sql/customer_data.sql")
