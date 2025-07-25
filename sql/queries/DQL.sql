-- 1. Energy Usage + Customer Info

SELECT
    fmr.timestamp,
    fmr.kwh_usage,
    dc.customer_id,
    dc.office_address,
    dc.tariff_type
FROM fact_meter_readings fmr
JOIN dim_customer dc ON fmr.customer_id = dc.customer_id
LIMIT 100;


-- 2. Weather Impact on Energy Usage

SELECT
    fmr.timestamp,
    fmr.region,
    fmr.kwh_usage,
    fwd.mean_temp_c,
    fwd.rainfall_mm
FROM fact_meter_readings fmr
JOIN fact_weather_daily fwd
  ON fmr.region = fwd.region
 AND DATE(fmr.timestamp) = fwd.observation_date
LIMIT 100;


-- 3. Billing with Customer + Tariff Info

SELECT
    fb.billing_month,
    fb.customer_id,
    dc.tariff_type,
    fb.total_kwh,
    fb.amount_due,
    fb.discount_applied
FROM fact_billing fb
JOIN dim_customer dc ON fb.customer_id = dc.customer_id
LIMIT 100;


-- 4. Full Energy Profile per Customer (Meter, Tariff, Billing)

SELECT
    dc.customer_id,
    dm.meter_id,
    dm.meter_type,
    fb.billing_month,
    fb.total_kwh,
    fb.amount_due,
    dt.tariff_name
FROM dim_customer dc
JOIN dim_meter dm ON dc.customer_id = dm.customer_id
JOIN fact_billing fb ON dc.customer_id = fb.customer_id
JOIN dim_tariff dt ON dc.tariff_type = dt.tariff_code
LIMIT 100;


-- 5. Peak Hour Usage by Temperature Bands

SELECT
    DATE_TRUNC('hour', fmr.timestamp) AS hour,
    fmr.region,
    CASE
        WHEN fmr.temperature_c < 10 THEN 'Cold'
        WHEN fmr.temperature_c BETWEEN 10 AND 20 THEN 'Mild'
        ELSE 'Hot'
    END AS temp_band,
    SUM(fmr.kwh_usage) AS total_kwh
FROM fact_meter_readings fmr
WHERE fmr.is_peak_hours = TRUE
GROUP BY hour, fmr.region, temp_band
ORDER BY hour, fmr.region;


-- 6. Window Functions for Customer Usage Analysis

-- 6.1 Monthly Usage Trends with Running Totals

SELECT
    customer_id,
    DATE_TRUNC('month', timestamp) AS month,
    SUM(kwh_usage) AS monthly_usage,
    SUM(SUM(kwh_usage)) OVER (
        PARTITION BY customer_id 
        ORDER BY DATE_TRUNC('month', timestamp)
    ) AS running_total,
    RANK() OVER (
        PARTITION BY DATE_TRUNC('month', timestamp) 
        ORDER BY SUM(kwh_usage) DESC
    ) AS usage_rank
FROM fact_meter_readings
GROUP BY customer_id, month;

-- 6.2 Comparing Customers to Regional Averages

SELECT
    f.customer_id,
    DATE_TRUNC('day', f.timestamp) AS day,
    SUM(f.kwh_usage) AS daily_usage,
    AVG(SUM(f.kwh_usage)) OVER (
        PARTITION BY f.region, DATE_TRUNC('day', f.timestamp)
    ) AS regional_avg,
    SUM(f.kwh_usage) - AVG(SUM(f.kwh_usage)) OVER (
        PARTITION BY f.region, DATE_TRUNC('day', f.timestamp)
    ) AS diff_from_avg
FROM fact_meter_readings f
GROUP BY f.customer_id, f.region, day;


-- 7. Recursive Query for Customer Usage Hierarchies
-- Finding Usage Patterns Across Time Periods

WITH RECURSIVE usage_patterns AS (
    SELECT 
        customer_id,
        DATE_TRUNC('month', timestamp)::DATE AS month,
        SUM(kwh_usage) AS monthly_usage
    FROM fact_meter_readings
    WHERE DATE_TRUNC('month', timestamp)::DATE = DATE '2023-01-01'
    GROUP BY customer_id, month

    UNION ALL

    SELECT 
        f.customer_id,
        DATE_TRUNC('month', f.timestamp)::DATE AS month,
        SUM(f.kwh_usage) AS monthly_usage
    FROM fact_meter_readings f
    JOIN usage_patterns r
      ON f.customer_id = r.customer_id
     AND DATE_TRUNC('month', f.timestamp)::DATE = r.month + INTERVAL '1 month'
    GROUP BY f.customer_id, month
)
SELECT * FROM usage_patterns
ORDER BY customer_id, month;


SELECT 
    customer_id,
    DATE_TRUNC('month', timestamp) AS month,
    SUM(kwh_usage) AS monthly_usage
FROM fact_meter_readings
WHERE DATE_TRUNC('month', timestamp) = DATE '2023-01-01'
GROUP BY customer_id, DATE_TRUNC('month', timestamp);


WITH RECURSIVE nums(n) AS (
  SELECT 1
  UNION ALL
  SELECT n + 1 FROM nums WHERE n < 5
)
SELECT * FROM nums;


-- 8. Peak energy usage per hour in Auckland yesterday, grouped by temperature bands
-- hourly_peak_by_temp_band.sql

WITH weather AS (
    SELECT region, mean_temp_c
    FROM fact_weather_daily
    WHERE observation_date BETWEEN '2023-01-01' AND '2023-12-31'
),
readings AS (
    SELECT 
        DATE_TRUNC('hour', timestamp) AS hour,
        region,
        ROUND(temperature_c) AS rounded_temp,
        SUM(kwh_usage) AS total_kwh
    FROM fact_meter_readings
    WHERE timestamp::date BETWEEN '2023-01-01' AND '2023-12-31'
      AND region = 'Auckland'
    GROUP BY 1,2,3
)
SELECT 
    hour,
    CASE 
        WHEN rounded_temp < 10 THEN '<10°C'
        WHEN rounded_temp BETWEEN 10 AND 20 THEN '10-20°C'
        ELSE '>20°C'
    END AS temp_band,
    MAX(total_kwh) AS peak_kwh
FROM readings
JOIN weather USING(region)
GROUP BY hour, temp_band
ORDER BY hour;


-- 9. Weather impact on revenue
-- revenue_vs_weather.sql
SELECT 
    f.region,
    w.observation_date,
    SUM(f.amount_due) AS total_revenue,
    w.mean_temp_c,
    w.rainfall_mm
FROM fact_billing f
JOIN fact_weather_daily w 
  ON f.region = w.region AND f.billing_month = w.observation_date
GROUP BY 1,2,4,5
ORDER BY observation_date;


-- 10. Basic Subquery in WHERE Clause (Customers with above-average usage)

SELECT customer_id, SUM(kwh_usage) AS total_usage
FROM fact_meter_readings
GROUP BY customer_id
HAVING SUM(kwh_usage) > (
    SELECT AVG(daily_usage)
    FROM (
        SELECT customer_id, DATE(timestamp), SUM(kwh_usage) AS daily_usage
        FROM fact_meter_readings
        GROUP BY customer_id, DATE(timestamp)
    ) AS daily_usage
);


-- 11. Correlated Subquery (Customers with usage spikes)

SELECT r1.customer_id, r1.timestamp, r1.kwh_usage
FROM fact_meter_readings r1
WHERE r1.kwh_usage > 3 * (
    SELECT AVG(r2.kwh_usage)
    FROM fact_meter_readings r2
    WHERE r2.customer_id = r1.customer_id
);


-- 12. Subquery in FROM Clause (Top 10 highest usage days per region)

SELECT region, day, total_usage
FROM (
    SELECT 
        region,
        DATE(timestamp) AS day,
        SUM(kwh_usage) AS total_usage,
        RANK() OVER (PARTITION BY region ORDER BY SUM(kwh_usage) DESC) AS rank
    FROM fact_meter_readings
    GROUP BY region, DATE(timestamp)
) AS ranked_days
WHERE rank <= 10;


-- 13. Subquery in SELECT Clause (Customer usage vs regional average)

SELECT 
    customer_id,
    SUM(kwh_usage) AS customer_usage,
    SUM(kwh_usage) / (
        SELECT SUM(kwh_usage)
        FROM fact_meter_readings f2
        WHERE f2.region = f1.region
    ) * 100 AS percent_of_region
FROM fact_meter_readings f1
GROUP BY customer_id, region;


-- 14. EXISTS Subquery (Customers with peak hour usage)

SELECT DISTINCT c.customer_id, c.office_address
FROM dim_customer c
WHERE EXISTS (
    SELECT 1
    FROM fact_meter_readings r
    WHERE r.customer_id = c.customer_id
    AND r.is_peak_hours = TRUE
);


-- 15. IN Subquery (Customers on specific tariffs)

SELECT customer_id, SUM(kwh_usage) AS total_usage
FROM fact_meter_readings
WHERE customer_id IN (
    SELECT customer_id
    FROM dim_customer
    WHERE tariff_type IN ('PEAK', 'OFFPEAK')
)
GROUP BY customer_id;


-- 16. Subquery with JOIN (Weather impact analysis)

SELECT 
    w.observation_date,
    w.mean_temp_c,
    (
        SELECT SUM(kwh_usage)
        FROM fact_meter_readings r
        WHERE DATE(r.timestamp) = w.observation_date
    ) AS total_usage
FROM fact_weather_daily w
WHERE w.region = 'Auckland';


-- 17. Nested Subquery (Multi-level aggregation)

SELECT 
    region,
    AVG(daily_usage) AS avg_daily_usage
FROM (
    SELECT 
        region,
        DATE(timestamp) AS day,
        SUM(kwh_usage) AS daily_usage
    FROM fact_meter_readings
    GROUP BY region, DATE(timestamp)
) AS daily_totals
GROUP BY region;


-- 18. Subquery with Window Function (Usage percentiles)

SELECT 
    customer_id,
    total_usage,
    percentile
FROM (
    SELECT 
        customer_id,
        SUM(kwh_usage) AS total_usage,
        NTILE(4) OVER (ORDER BY SUM(kwh_usage)) AS percentile
    FROM fact_meter_readings
    GROUP BY customer_id
) AS ranked_customers
WHERE percentile = 4; -- Top quartile


-- 19. Subquery for Billing Calculation

UPDATE fact_billing fb
SET amount_due = (
    SELECT SUM(r.kwh_usage) * t.rate
    FROM fact_meter_readings r
    JOIN dim_customer c ON r.customer_id = c.customer_id
    JOIN dim_tariff t ON c.tariff_type = t.tariff_code
    WHERE r.customer_id = fb.customer_id
    AND DATE_TRUNC('month', r.timestamp) = fb.billing_month
)
WHERE fb.billing_month = '2023-06-01';


