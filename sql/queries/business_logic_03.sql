--1. Time-Series Gap Detection (Missing Readings)
--
--Use Case: Identify meters with irregular data submission.

WITH expected_readings AS (
    SELECT
        meter_id,
        generate_series(
            DATE_TRUNC('day', MIN(timestamp)),
            DATE_TRUNC('day', MAX(timestamp)),
            INTERVAL '1 day'
        ) AS expected_day
    FROM fact_meter_readings
    GROUP BY meter_id
),
actual_readings AS (
    SELECT 
        meter_id,
        DATE_TRUNC('day', timestamp) AS reading_day
    FROM fact_meter_readings
    GROUP BY meter_id, DATE_TRUNC('day', timestamp)
)
SELECT 
    e.meter_id,
    e.expected_day AS missing_day,
    m.meter_type
FROM expected_readings e
LEFT JOIN actual_readings a ON e.meter_id = a.meter_id AND e.expected_day = a.reading_day
LEFT JOIN dim_meter m ON e.meter_id = m.meter_id
WHERE a.reading_day IS NULL
ORDER BY e.meter_id, e.expected_day;


--2. Peak/Off-Peak Billing Validation
--
--Use Case: Verify billing calculations match actual usage.

WITH usage_by_period AS (
    SELECT 
        b.customer_id,
        b.billing_month,
        b.peak_kwh AS billed_peak,
        b.offpeak_kwh AS billed_offpeak,
        SUM(CASE WHEN r.is_peak_hours THEN r.kwh_usage ELSE 0 END) AS actual_peak,
        SUM(CASE WHEN NOT r.is_peak_hours THEN r.kwh_usage ELSE 0 END) AS actual_offpeak
    FROM fact_billing b
    JOIN fact_meter_readings r ON b.customer_id = r.customer_id
        AND DATE_TRUNC('month', r.timestamp) = b.billing_month
    GROUP BY b.customer_id, b.billing_month, b.peak_kwh, b.offpeak_kwh
)
SELECT 
    customer_id,
    billing_month,
    billed_peak,
    actual_peak,
    billed_offpeak,
    actual_offpeak,
    (billed_peak - actual_peak) AS peak_diff,
    (billed_offpeak - actual_offpeak) AS offpeak_diff
FROM usage_by_period
WHERE ABS(billed_peak - actual_peak) > 5 
	OR ABS(billed_offpeak - actual_offpeak) > 5;


--3. Weather Impact Analysis
--
--Use Case: Correlate energy usage with temperature extremes.

SELECT 
    r.region,
    DATE_TRUNC('month', r.timestamp) AS month,
    AVG(r.kwh_usage) AS avg_daily_usage,
    AVG(w.mean_temp_c) AS avg_temp,
    CORR(r.kwh_usage, w.mean_temp_c) AS temp_usage_correlation
FROM fact_meter_readings r
JOIN fact_weather_daily w ON r.region = w.region 
    AND DATE(r.timestamp) = w.observation_date
GROUP BY r.region, DATE_TRUNC('month', r.timestamp)
ORDER BY ABS(CORR(r.kwh_usage, w.mean_temp_c)) DESC;


--4. Meter Performance Benchmarking
--
--Use Case: Compare smart vs legacy meter reliability.

SELECT 
    m.meter_type,
    COUNT(DISTINCT r.reading_id) AS readings_count,
    COUNT(DISTINCT m.customer_id) AS customers_served,
    AVG(r.kwh_usage) AS avg_usage,
    STDDEV(r.kwh_usage) AS usage_volatility,
    AVG(r.power_factor) AS avg_power_factor
FROM dim_meter m
JOIN fact_meter_readings r ON m.meter_id = r.meter_id
GROUP BY ROLLUP(m.meter_type)
ORDER BY m.meter_type NULLS LAST;


--5. Customer Churn Risk Prediction
--
--Use Case: Identify customers likely to delay payments.

WITH payment_behavior AS (
    SELECT 
        customer_id,
        AVG(EXTRACT(DAY FROM (payment_date - billing_month))) AS avg_days_to_pay,
        COUNT(*) FILTER (WHERE payment_date IS NULL) AS missed_payments,
        COUNT(*) AS total_bills
    FROM fact_billing
    GROUP BY customer_id
)
SELECT 
    c.customer_id,
    c.tariff_type,
    pb.avg_days_to_pay,
    pb.missed_payments,
    pb.total_bills,
    CASE 
        WHEN pb.missed_payments > 0 OR pb.avg_days_to_pay > 30 THEN 'High Risk'
        WHEN pb.avg_days_to_pay BETWEEN 15 AND 30 THEN 'Medium Risk'
        ELSE 'Low Risk'
    END AS churn_risk
FROM dim_customer c
JOIN payment_behavior pb ON c.customer_id = pb.customer_id
ORDER BY churn_risk, pb.avg_days_to_pay DESC;


--6. Address Change Impact Analysis
--
--Use Case: Measure usage changes after customers move.

WITH address_changes AS (
    SELECT 
        customer_id,
        address,
        start_date,
        LEAD(start_date) OVER (PARTITION BY customer_id ORDER BY start_date) AS next_move_date
    FROM bridge_customer_address
),
usage_comparison AS (
    SELECT 
        ac.customer_id,
        ac.address AS old_address,
        LEAD(ac.address) OVER (PARTITION BY ac.customer_id ORDER BY ac.start_date) AS new_address,
        AVG(CASE WHEN r.timestamp BETWEEN ac.start_date AND ac.next_move_date - INTERVAL '1 day' 
            THEN r.kwh_usage END) AS old_usage,
        AVG(CASE WHEN r.timestamp >= ac.next_move_date 
            THEN r.kwh_usage END) AS new_usage
    FROM address_changes ac
    JOIN fact_meter_readings r ON ac.customer_id = r.customer_id
    WHERE ac.next_move_date IS NOT NULL
    GROUP BY ac.customer_id, ac.address, ac.start_date, ac.next_move_date
)
SELECT 
    customer_id,
    old_address,
    new_address,
    old_usage,
    new_usage,
    ((new_usage - old_usage) / old_usage) * 100 AS usage_change_percent
FROM usage_comparison
WHERE old_usage > 0 AND new_usage > 0
ORDER BY ABS(new_usage - old_usage) DESC;


--7. Materialized View for Billing Dashboard

CREATE MATERIALIZED VIEW mv_billing_analytics AS
SELECT 
    b.region,
    DATE_TRUNC('month', b.billing_month) AS month,
    COUNT(DISTINCT b.customer_id) AS active_customers,
    SUM(b.total_kwh) AS total_kwh,
    SUM(b.amount_due) AS revenue,
    SUM(b.late_fee) AS late_fees_collected,
    AVG(w.mean_temp_c) AS avg_temp
FROM fact_billing b
LEFT JOIN fact_weather_daily w ON b.region = w.region 
    AND DATE_TRUNC('month', b.billing_month) = DATE_TRUNC('month', w.observation_date)
GROUP BY b.region, DATE_TRUNC('month', b.billing_month);

-- Refresh nightly
REFRESH MATERIALIZED VIEW mv_billing_analytics;























