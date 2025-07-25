-- 1: Meter Reading Transformation Procedure
-- First, let's write a procedure to transform raw meter readings into usable formats 
-- for downstream processing. We will assume incoming readings include both real-time 
-- and historical snapshots. This transformation will cleanse, validate, and insert the
-- transformed data into our fact_meter_readings table.

CREATE OR REPLACE PROCEDURE process_meter_readings() AS $$
BEGIN
    -- Step 1: Delete existing rows for today's date (if necessary)
    DELETE FROM fact_meter_readings WHERE timestamp::date = CURRENT_DATE;
    
    -- Step 2: Insert transformed readings
    INSERT INTO fact_meter_readings(
        reading_id,
		meter_id,
		customer_id,
		timestamp,
		kwh_usage,
		voltage,
		current_amps,
		power_factor,
		region,
		is_peak_hours,
		weather_condition,
		temperature_c
    )
    SELECT
        md.reading_id,
		md.meter_id,
		c.customer_id,
		md.timestamp,
        CASE WHEN md.kwh_usage > 0 THEN md.kwh_usage ELSE 0 END AS kwh_usage,
        COALESCE(md.voltage, 0) AS voltage,
        COALESCE(md.current_amps, 0) AS current_amps,
        COALESCE(md.power_factor, 1) AS power_factor,
        r.region,
        CASE WHEN EXTRACT(HOUR FROM md.timestamp) BETWEEN 7 AND 21 THEN true ELSE false END AS
		is_peak_hours,
        w.weather_condition,
        w.temperature_c
    FROM staging_meter_data md
    JOIN dim_customer c ON md.customer_id = c.customer_id
    LEFT JOIN dim_region r ON c.region = r.region
    LEFT JOIN fact_weather_daily w
		ON w.observation_date = md.timestamp::date
		AND w.station_id IN ('StationA', 'StationB')
    WHERE md.timestamp::date = CURRENT_DATE;
END;
$$ LANGUAGE plpgsql;

-- 2: Billing Calculation Function
-- Next, we'll create a function to calculate billing totals based on customer tariffs
-- and meter readings. This function retrieves relevant tariff information and applies it
-- to generate billing records.

CREATE OR REPLACE FUNCTION calculate_billing(customer_id_in VARCHAR, billing_month_in DATE)
RETURNS SETOF RECORD AS $$
DECLARE
    rec RECORD;
BEGIN
    FOR rec IN
        WITH meter_data AS (
            SELECT
				mr.*,
				CASE
					WHEN mr.is_peak_hours THEN t.peak_rate_per_kwh
					ELSE t.offpeak_rate_per_kwh
				END AS rate_per_kwh
            FROM fact_meter_readings mr
            INNER JOIN dim_tariff t ON mr.customer_id = t.tariff_code
            WHERE mr.customer_id = customer_id_in
                AND mr.timestamp >= billing_month_in
                AND mr.timestamp < billing_month_in + INTERVAL '1 MONTH'
        ),
        billing_summary AS (
            SELECT SUM(kwh_usage * rate_per_kwh) AS total_cost,
                   SUM(CASE WHEN is_peak_hours THEN kwh_usage ELSE 0 END) AS peak_kwh,
                   SUM(CASE WHEN NOT is_peak_hours THEN kwh_usage ELSE 0 END) AS offpeak_kwh
            FROM meter_data
        )
        SELECT
			bs.total_cost,
			bs.peak_kwh,
			bs.offpeak_kwh,
            t.daily_fixed_charge,
			t.tariff_name
        FROM billing_summary bs
        INNER JOIN dim_tariff t ON t.tariff_code = ANY(
			(SELECT DISTINCT tariff_code FROM fact_meter_readings)
		)
    LOOP
        RETURN NEXT rec;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- 3: Trigger for Alerts Based on Thresholds
-- You may want to trigger alerts when certain threshold conditions are reached,
-- such as excessive consumption or anomalous readings. For example, here’s a simple trigger
-- that fires whenever new meter readings exceed predefined limits.

CREATE OR REPLACE FUNCTION check_anomalous_readings()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.kwh_usage > 1000 THEN
        RAISE NOTICE 'Anomalous High Usage Detected! Customer ID: %', NEW.customer_id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER detect_high_consumption_trigger
BEFORE INSERT ON fact_meter_readings
FOR EACH ROW EXECUTE FUNCTION check_anomalous_readings();


-- 4: Complex Analytics Query Using Window Functions
-- Here’s a sample analytic query using window functions to compute average daily consumption
-- grouped by region and compare it with previous years.

WITH avg_consumption AS (
    SELECT
    	region,
    	AVG(kwh_usage) OVER (
    		PARTITION BY region ORDER BY EXTRACT(YEAR FROM timestamp)
    	) AS avg_kwh_by_year
    FROM fact_meter_readings
    GROUP BY region, EXTRACT(YEAR FROM timestamp)
)
SELECT
	ac.region,
	ac.avg_kwh_by_year,
	LAG(ac.avg_kwh_by_year) OVER (ORDER BY EXTRACT(YEAR FROM timestamp)) AS prev_avg_kwh
FROM avg_consumption ac
WHERE ac.region = 'North';


-- 5: Cursor Loops for Batch Processing
-- When working with large datasets, it's often beneficial to loop through batches of records.
-- Here's an example using a cursor to iterate over meter readings and update the fact_billing
-- table dynamically.

DO $$
DECLARE
    cur CURSOR FOR 
		SELECT *
        FROM fact_meter_readings
        WHERE timestamp >= NOW() - INTERVAL '1 week'; -- Select recent readings
    rec RECORD;
BEGIN
    OPEN cur;
    LOOP
        FETCH cur INTO rec;
        EXIT WHEN NOT FOUND;
        
        -- Perform some operation (updating billing info)
        UPDATE fact_billing fb
        SET total_kwh = fb.total_kwh + rec.kwh_usage
        WHERE fb.billing_month = TO_CHAR(rec.timestamp, 'YYYY-MM-DD');
    END LOOP;
    CLOSE cur;
END $$;


-- 6: Conditional Branching with IF-THEN-ELSE
-- Sometimes, your logic requires executing different paths based on conditions.
-- Here’s an example of a PL/pgSQL block that calculates tariff-dependent charges differently
-- depending on whether the meter belongs to a commercial or residential category.

CREATE OR REPLACE FUNCTION calculate_charge(meter_id_in VARCHAR, kwh_used DECIMAL)
RETURNS DECIMAL AS $$
DECLARE
    charge DECIMAL := 0;
    tariff_type VARCHAR(20);
BEGIN
    SELECT tariff_type INTO tariff_type
    FROM dim_meter dm
    WHERE dm.meter_id = meter_id_in;

    IF tariff_type = 'Commercial' THEN
        charge := kwh_used * 0.15; -- Commercial rate per KWH
    ELSEIF tariff_type = 'Residential' THEN
        charge := kwh_used * 0.10; -- Residential rate per KWH
    ELSE
        RAISE EXCEPTION 'Unknown tariff type for meter: %', meter_id_in;
    END IF;

    RETURN charge;
END;
$$ LANGUAGE plpgsql;

-- 7: Error Handling and Transactions
-- It's important to handle exceptions gracefully, particularly when dealing with sensitive
-- operations like billing or updates. Here's an example illustrating transaction rollback and
-- error logging.

CREATE OR REPLACE FUNCTION log_and_update(address_id_in INT, new_email VARCHAR)
RETURNS VOID AS $$
BEGIN
    BEGIN
        -- Attempt to update the customer's email
        UPDATE dim_customer dc
        SET email = new_email
        WHERE dc.address_id = address_id_in;

        COMMIT;
    EXCEPTION WHEN OTHERS THEN
        ROLLBACK;
        RAISE WARNING 'Error occurred: %', SQLERRM;
    END;
END;
$$ LANGUAGE plpgsql;


-- 8: Aggregate Computations with Grouping Sets
-- For complex reporting needs, group-by sets allow you to analyze multi-dimensional data.
-- Here's an example that computes quarterly average consumption by region.

CREATE OR REPLACE VIEW v_quarterly_consumption AS
SELECT
    EXTRACT(QUARTER FROM fmr.timestamp) AS quarter,
    fmr.region,
    AVG(fmr.kwh_usage) AS avg_consumption
FROM fact_meter_readings fmr
GROUP BY GROUPING SETS ((quarter, region));


-- 9: Partitioning Large Tables
-- If your fact_meter_readings grows very large, consider horizontal partitioning by
-- time windows (monthly partitions).

CREATE TABLE fact_meter_readings_partitioned PARTITION BY RANGE(timestamp);

CREATE TABLE fact_meter_readings_january PARTITION OF fact_meter_readings_partitioned
    FOR VALUES FROM ('2023-01-01') TO ('2023-02-01');

CREATE TABLE fact_meter_readings_february PARTITION OF fact_meter_readings_partitioned
    FOR VALUES FROM ('2023-02-01') TO ('2023-03-01');


-- 10: Advanced Triggers for Auditing Changes
-- Triggers are useful for logging changes automatically. Suppose you want to audit
-- modifications to the dim_customer table.

CREATE TABLE customer_audit_log (
    id SERIAL PRIMARY KEY,
    customer_id VARCHAR(20),
    action_type VARCHAR(10),
    changed_at TIMESTAMPTZ DEFAULT NOW(),
    old_value JSONB,
    new_value JSONB
);

CREATE OR REPLACE FUNCTION log_customer_changes()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'UPDATE' THEN
        INSERT INTO customer_audit_log(customer_id, action_type, old_value, new_value)
        VALUES(NEW.customer_id, 'UPDATE', to_jsonb(OLD), to_jsonb(NEW));
    ELSIF TG_OP = 'INSERT' THEN
        INSERT INTO customer_audit_log(customer_id, action_type, new_value)
        VALUES(NEW.customer_id, 'INSERT', to_jsonb(NEW));
    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO customer_audit_log(customer_id, action_type, old_value)
        VALUES(OLD.customer_id, 'DELETE', to_jsonb(OLD));
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER customer_change_trigger
AFTER INSERT OR UPDATE OR DELETE ON dim_customer
FOR EACH ROW EXECUTE FUNCTION log_customer_changes();


-- 10: Dynamic Queries and EXECUTE Command
-- Dynamic SQL is helpful when constructing queries programmatically. Consider a case
-- where you need to filter by arbitrary regions.

DO $$
DECLARE
    region_filter text[] := ARRAY['Auckland', 'Dunedin'];
    sql_query text;
BEGIN
    sql_query := format(
		'SELECT * FROM fact_meter_readings WHERE region IN (%L)', array_to_string(region_filter, ',')
	);
    EXECUTE sql_query;
END $$;


-- 11: Recursive Common Table Expressions (CTEs)
-- Recursive CTEs are useful for hierarchical traversal. Imagine you want to recursively 
-- count parent-child relationships between meters.

WITH RECURSIVE meter_hierarchy AS (
    SELECT
    	m.meter_id,
    	m.parent_meter_id
    FROM dim_meter m
    WHERE m.parent_meter_id IS NULL

    UNION ALL

    SELECT
    	child.meter_id,
    	child.parent_meter_id
    FROM dim_meter child
    JOIN meter_hierarchy parent ON child.parent_meter_id = parent.meter_id
)
SELECT COUNT(*) FROM meter_hierarchy;


-- 12: Set Returning Functions
-- PL/pgSQL functions can return entire result sets instead of scalar values.
-- Here's a set returning function to retrieve customer information filtered by region.

CREATE OR REPLACE FUNCTION get_customers_by_region(region_in VARCHAR)
RETURNS SETOF dim_customer LANGUAGE plpgsql AS $$
BEGIN
    RETURN QUERY
    SELECT * FROM dim_customer
    WHERE region = region_in;
END $$;


-- 13: User-defined Types and Composite Values
-- User-defined composite types simplify passing complex objects between functions.

CREATE TYPE customer_details AS (
    customer_id VARCHAR(20),
    address TEXT,
    tariff_type VARCHAR(20)
);

CREATE OR REPLACE FUNCTION fetch_customer_info(customer_id_in VARCHAR)
RETURNS customer_details AS $$
DECLARE
    cust_rec customer_details;
BEGIN
    SELECT d.customer_id, d.address, dt.tariff_type
    INTO cust_rec
    FROM dim_customer d
    JOIN dim_tariff dt ON d.tariff_code = dt.tariff_code
    WHERE d.customer_id = customer_id_in;

    RETURN cust_rec;
END;
$$ LANGUAGE plpgsql;


















