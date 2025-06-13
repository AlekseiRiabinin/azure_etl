import uuid
import random
from datetime import datetime, timedelta
import os

def generate_billing_data(num_customers=500, start_month="2023-01-01", end_month="2024-06-01"):
    regions = ["Auckland", "Wellington", "Christchurch", "Hamilton", "Dunedin"]
    customer_ids = [f"CUST_{1000 + i}" for i in range(1, num_customers + 1)]
    
    start_dt = datetime.strptime(start_month, "%Y-%m-%d")
    end_dt = datetime.strptime(end_month, "%Y-%m-%d")
    
    current_month = start_dt
    sql = "INSERT INTO fact_billing VALUES\n"
    values = []
    
    while current_month <= end_dt:
        for customer_id in customer_ids:
            bill_id = str(uuid.uuid4())
            region = random.choice(regions)
            total_kwh = round(random.uniform(200, 1500), 2)
            peak_ratio = random.uniform(0.3, 0.7)
            peak_kwh = round(total_kwh * peak_ratio, 2)
            offpeak_kwh = round(total_kwh - peak_kwh, 2)
            amount_due = round(total_kwh * random.uniform(0.25, 0.35), 2)
            paid_amount = round(amount_due * random.uniform(0.8, 1.0), 2) if random.random() > 0.2 else 0
            payment_date = (current_month + timedelta(days=random.randint(10, 20))) if paid_amount > 0 else None
            late_fee = round(amount_due * 0.1, 2) if paid_amount < amount_due and random.random() > 0.7 else 0
            discount = round(amount_due * 0.05, 2) if random.random() > 0.8 else 0
            
            values.append(
                f"('{bill_id}', '{customer_id}', '{current_month.date()}', "
                f"{total_kwh}, {peak_kwh}, {offpeak_kwh}, {amount_due}, "
                f"{paid_amount}, {f"'{payment_date.isoformat()}'" if payment_date else 'NULL'}, "
                f"{late_fee}, {discount}, '{region}')"
            )
        
        # Move to next month
        if current_month.month == 12:
            current_month = current_month.replace(year=current_month.year + 1, month=1)
        else:
            current_month = current_month.replace(month=current_month.month + 1)
    
    sql += ",\n".join(values) + ";\n"
    
    os.makedirs("../sql", exist_ok=True)
    with open("../sql/billing_data.sql", "w") as f:
        f.write("-- fact_billing table data\n")
        f.write("TRUNCATE TABLE fact_billing;\n")
        f.write(sql)

if __name__ == "__main__":
    generate_billing_data()
    print("Generated billing data to ../sql/billing_data.sql")
