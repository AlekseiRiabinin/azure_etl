-- 1. Create a masking function (example)
-- data_masking_rbac.sql

CREATE OR REPLACE FUNCTION mask_email(email TEXT) RETURNS TEXT AS
$$
BEGIN
    RETURN regexp_replace(email, '[^@]+', '****', 'g');
END;
$$
LANGUAGE plpgsql;

-- Use a view to simulate RBAC-controlled masking
CREATE VIEW secure_dim_customer AS
SELECT 
    customer_id,
    address,
    tariff_plan,
    CASE 
        WHEN current_user = 'data_analyst' THEN mask_email(masked_contact)
        ELSE masked_contact
    END AS masked_contact
FROM dim_customer;


-- 2. Custom Functions for Energy Analysis
-- Function to Calculate Estimated Bill

CREATE OR REPLACE FUNCTION calculate_estimated_bill(
    p_customer_id INT,
    p_month DATE
) RETURNS DECIMAL(10,2) AS
$$
DECLARE
    v_total_kwh DECIMAL(10,2);
    v_tariff_rate DECIMAL(10,2);
    v_discount DECIMAL(5,2);
    v_estimated_bill DECIMAL(10,2);
BEGIN
    -- Get total usage for the month
    SELECT SUM(kwh_usage) INTO v_total_kwh
    FROM fact_meter_readings
    WHERE customer_id = p_customer_id
    AND DATE_TRUNC('month', timestamp) = DATE_TRUNC('month', p_month);
    
    -- Get tariff rate
    SELECT rate INTO v_tariff_rate
    FROM dim_tariff
    WHERE tariff_code = (
        SELECT tariff_type 
        FROM dim_customer 
        WHERE customer_id = p_customer_id
    );
    
    -- Get discount if any
    SELECT COALESCE(discount_pct, 0) INTO v_discount
    FROM fact_billing
    WHERE customer_id = p_customer_id
    AND billing_month = DATE_TRUNC('month', p_month);
    
    -- Calculate estimated bill
    v_estimated_bill := (v_total_kwh * v_tariff_rate) * (1 - v_discount/100);
    
    RETURN v_estimated_bill;
END;
$$
LANGUAGE plpgsql;

-- Usage
SELECT customer_id, calculate_estimated_bill(customer_id, '2023-06-01') 
FROM dim_customer LIMIT 10;


-- 3. Custom Functions for Energy Analysis
-- Function to Calculate Estimated Bill

CREATE OR REPLACE FUNCTION calculate_estimated_bill(
    p_customer_id INT,
    p_month DATE
) RETURNS DECIMAL(10,2) AS 
$$
DECLARE
    v_total_kwh DECIMAL(10,2);
    v_tariff_rate DECIMAL(10,2);
    v_discount DECIMAL(5,2);
    v_estimated_bill DECIMAL(10,2);
BEGIN
    -- Get total usage for the month
    SELECT SUM(kwh_usage) INTO v_total_kwh
    FROM fact_meter_readings
    WHERE customer_id = p_customer_id
    AND DATE_TRUNC('month', timestamp) = DATE_TRUNC('month', p_month);
    
    -- Get tariff rate
    SELECT rate INTO v_tariff_rate
    FROM dim_tariff
    WHERE tariff_code = (
        SELECT tariff_type 
        FROM dim_customer 
        WHERE customer_id = p_customer_id
    );
    
    -- Get discount if any
    SELECT COALESCE(discount_pct, 0) INTO v_discount
    FROM fact_billing
    WHERE customer_id = p_customer_id
    AND billing_month = DATE_TRUNC('month', p_month);
    
    -- Calculate estimated bill
    v_estimated_bill := (v_total_kwh * v_tariff_rate) * (1 - v_discount/100);
    
    RETURN v_estimated_bill;
END;
$$ 
LANGUAGE plpgsql;

-- Usage
SELECT customer_id, calculate_estimated_bill(customer_id, '2023-06-01') 
FROM dim_customer LIMIT 10;


-- 4. Trigger for Usage Anomaly Detection
-- Trigger Function and Definition

CREATE OR REPLACE FUNCTION check_usage_anomaly()
RETURNS TRIGGER AS
$$
DECLARE
    v_avg_usage DECIMAL(10,2);
    v_stddev DECIMAL(10,2);
BEGIN
    -- Calculate customer's average and standard deviation
    SELECT 
        AVG(kwh_usage), 
        STDDEV(kwh_usage)
    INTO v_avg_usage, v_stddev
    FROM fact_meter_readings
    WHERE customer_id = NEW.customer_id;
    
    -- Check if current reading is 3 standard deviations above mean
    IF NEW.kwh_usage > (v_avg_usage + 3 * v_stddev) THEN
        INSERT INTO usage_anomalies (customer_id, reading_id, timestamp, kwh_usage, avg_usage)
        VALUES (NEW.customer_id, NEW.reading_id, NEW.timestamp, NEW.kwh_usage, v_avg_usage);
        
        RAISE NOTICE 'Anomaly detected for customer %: usage % (avg %)', 
            NEW.customer_id, NEW.kwh_usage, v_avg_usage;
    END IF;
    
    RETURN NEW;
END;
$$ 
LANGUAGE plpgsql;

CREATE TRIGGER usage_anomaly_check
AFTER INSERT ON fact_meter_readings
FOR EACH ROW EXECUTE FUNCTION check_usage_anomaly();


-- 5. Cursor for Batch Processing of Customer Data
-- Procedure with Cursor for Monthly Billing

CREATE OR REPLACE PROCEDURE generate_monthly_bills(p_month DATE)
AS 
$$
DECLARE
    v_customer RECORD;
    v_total_kwh DECIMAL(10,2);
    v_amount_due DECIMAL(10,2);
    v_discount DECIMAL(5,2);
    cur_customers CURSOR FOR
        SELECT customer_id, tariff_type 
        FROM dim_customer;
BEGIN
    OPEN cur_customers;
    LOOP
        FETCH cur_customers INTO v_customer;
        EXIT WHEN NOT FOUND;
        
        -- Calculate total usage
        SELECT SUM(kwh_usage) INTO v_total_kwh
        FROM fact_meter_readings
        WHERE customer_id = v_customer.customer_id
        AND DATE_TRUNC('month', timestamp) = DATE_TRUNC('month', p_month);
        
        -- Calculate amount due based on tariff
        SELECT 
            v_total_kwh * rate,
            COALESCE(discount_pct, 0)
        INTO v_amount_due, v_discount
        FROM dim_tariff
        WHERE tariff_code = v_customer.tariff_type;
        
        -- Apply discount if any
        v_amount_due := v_amount_due * (1 - v_discount/100);
        
        -- Insert billing record
        INSERT INTO fact_billing (
            billing_month, 
            customer_id, 
            total_kwh, 
            amount_due, 
            discount_applied
        ) VALUES (
            DATE_TRUNC('month', p_month),
            v_customer.customer_id,
            v_total_kwh,
            v_amount_due,
            v_discount
        );
    END LOOP;
    CLOSE cur_customers;
    
    COMMIT;
END;
$$ 
LANGUAGE plpgsql;

-- Execute the procedure
CALL generate_monthly_bills('2023-06-01');


-- 6. Advanced Materialized View with Automatic Refresh
-- Energy Usage by Weather Conditions (Auto-refreshed)

CREATE MATERIALIZED VIEW energy_weather_analysis AS
SELECT 
    f.region,
    DATE_TRUNC('day', f.timestamp) AS day,
    w.mean_temp_c,
    w.rainfall_mm,
    SUM(f.kwh_usage) AS total_usage,
    COUNT(DISTINCT f.customer_id) AS customer_count,
    SUM(f.kwh_usage) / COUNT(DISTINCT f.customer_id) AS usage_per_customer
FROM fact_meter_readings f
JOIN fact_weather_daily w ON f.region = w.region AND DATE(f.timestamp) = w.observation_date
GROUP BY f.region, day, w.mean_temp_c, w.rainfall_mm;

-- Create a function to refresh the materialized view
CREATE OR REPLACE FUNCTION refresh_energy_weather()
RETURNS TRIGGER AS 
$$
BEGIN
    REFRESH MATERIALIZED VIEW energy_weather_analysis;
    RETURN NULL;
END;
$$ 
LANGUAGE plpgsql;

-- Create a trigger to auto-refresh when new weather data arrives
CREATE TRIGGER refresh_energy_weather_trigger
AFTER INSERT OR UPDATE ON fact_weather_daily
FOR EACH STATEMENT EXECUTE FUNCTION refresh_energy_weather();


















