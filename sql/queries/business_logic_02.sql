--1. Monthly Billing Calculation
--
--Business Logic:
--
--    Core Functionality: Automates the critical process of generating customer bills based on their
--    energy consumption patterns.
--
--    Peak/Off-Peak Differentiation: Implements time-of-use pricing which incentivizes customers to
--    shift usage to off-peak times, helping balance grid load.
--
--    Regional Filtering: Allows for regional pricing variations (common when different areas
--    have different infrastructure costs).
--
--    Tariff Application: Dynamically applies the correct tariff rates based on customer
--    classification (residential/commercial/special rates).
--
--    Fixed Charges: Properly accounts for daily connection fees that are independent of usage.
--
--Business Impact: Ensures accurate, timely billing that complies with regulatory requirements
--and tariff structures while enabling demand management through pricing signals.

CREATE OR REPLACE FUNCTION calculate_monthly_billing(
    p_month DATE,
    p_region VARCHAR(50) DEFAULT NULL
)
RETURNS TABLE (
    customer_id VARCHAR(20),
    billing_month DATE,
    total_kwh DECIMAL(10,2),
    peak_kwh DECIMAL(10,2),
    offpeak_kwh DECIMAL(10,2),
    amount_due DECIMAL(10,2)
) AS $$
BEGIN
    RETURN QUERY
    WITH usage_data AS (
        SELECT
            fmr.customer_id,
            SUM(fmr.kwh_usage) AS total_kwh,
            SUM(CASE WHEN fmr.is_peak_hours THEN fmr.kwh_usage ELSE 0 END) AS peak_kwh,
            SUM(CASE WHEN NOT fmr.is_peak_hours THEN fmr.kwh_usage ELSE 0 END) AS offpeak_kwh
        FROM fact_meter_readings fmr
        WHERE DATE_TRUNC('month', fmr.timestamp) = DATE_TRUNC('month', p_month)
        	AND (p_region IS NULL OR fmr.region = p_region)
        GROUP BY fmr.customer_id
    )
    SELECT 
        ud.customer_id,
        DATE_TRUNC('month', p_month)::DATE AS billing_month,
        ud.total_kwh,
        ud.peak_kwh,
        ud.offpeak_kwh,
        (ud.peak_kwh * dt.peak_rate_per_kwh + 
         ud.offpeak_kwh * dt.offpeak_rate_per_kwh +
         EXTRACT(DAY FROM (DATE_TRUNC('month', p_month + INTERVAL '1 month') - DATE_TRUNC('month', p_month))) * dt.daily_fixed_charge
        )::DECIMAL(10,2) AS amount_due
    FROM usage_data ud
    JOIN dim_customer dc ON ud.customer_id = dc.customer_id
    JOIN dim_tariff dt ON dc.tariff_type = dt.tariff_code
    WHERE dt.is_current = TRUE;
END;
$$ LANGUAGE plpgsql

-- Usage example:
SELECT * FROM calculate_monthly_billing('2023-06-01', 'Auckland');



--2. Customer Consumption Analysis
--
--Business Logic:
--
--    Usage Pattern Analysis: Helps identify how customers consume energy across different time periods.
--
--    Weather Correlation: Reveals how temperature extremes affect energy use (heating in winter,
--    cooling in summer).
--
--    Peak Demand Insights: Shows what portion of usage occurs during expensive peak periods.
--
--    Billing Verification: Allows comparison between actual usage and billed amounts for discrepancy
--    detection.
--
--Business Impact: Supports customer engagement (showing usage patterns), helps optimize tariff plans
--for customers, and provides data for demand forecasting and infrastructure planning.

CREATE OR REPLACE PROCEDURE sp_get_customer_consumption_trends(
    IN customer_id VARCHAR(20),
    IN start_date DATE,
    IN end_date DATE
) AS $$
DECLARE
    result RECORD;
BEGIN
    -- Daily consumption with weather impact
    FOR result IN (
        SELECT 
            CAST(fmr.timestamp AS DATE) AS reading_date,
            SUM(fmr.kwh_usage) AS daily_consumption,
            AVG(fwd.mean_temp_c) AS avg_temperature,
            MAX(fwd.rainfall_mm) AS daily_rainfall,
            SUM(CASE WHEN fmr.is_peak_hours THEN fmr.kwh_usage ELSE 0 END) AS peak_consumption,
            SUM(CASE WHEN NOT fmr.is_peak_hours THEN fmr.kwh_usage ELSE 0 END) AS offpeak_consumption
        FROM fact_meter_readings fmr
        LEFT JOIN fact_weather_daily fwd ON 
            CAST(fmr.timestamp AS DATE) = fwd.observation_date
			AND fmr.region = fwd.region
        WHERE fmr.customer_id = customer_id
          AND fmr.timestamp BETWEEN start_date AND end_date
        GROUP BY CAST(fmr.timestamp AS DATE)
        ORDER BY reading_date
    ) LOOP
        RAISE INFO '%', row_to_json(result); -- Output each record as JSON
    END LOOP;

    -- Monthly summary
    FOR result IN (
        SELECT
            TO_CHAR(fmr.timestamp, 'YYYY-MM') AS month,
            SUM(fmr.kwh_usage) AS monthly_consumption,
            SUM(CASE WHEN fmr.is_peak_hours THEN fmr.kwh_usage ELSE 0 END) AS peak_consumption,
            SUM(CASE WHEN NOT fmr.is_peak_hours THEN fmr.kwh_usage ELSE 0 END) AS offpeak_consumption,
            (SELECT amount_due FROM fact_billing fb 
             WHERE fb.customer_id = customer_id 
               	AND fb.billing_month = MAKE_DATE(
					EXTRACT(YEAR FROM fmr.timestamp)::INT,
					EXTRACT(MONTH FROM fmr.timestamp)::INT, 1)
				) AS billed_amount
        FROM fact_meter_readings fmr
        WHERE fmr.customer_id = customer_id
          AND fmr.timestamp BETWEEN start_date AND end_date
        GROUP BY
			TO_CHAR(fmr.timestamp, 'YYYY-MM'),
			EXTRACT(YEAR FROM fmr.timestamp),
			EXTRACT(MONTH FROM fmr.timestamp)
        ORDER BY
			EXTRACT(YEAR FROM fmr.timestamp),
			EXTRACT(MONTH FROM fmr.timestamp)
    ) LOOP
        RAISE INFO '%', row_to_json(result); -- Output each record as JSON
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Call the procedure:
CALL sp_get_customer_consumption_trends('CUST12345', '2023-01-01'::DATE, '2023-12-31'::DATE);


--3. Meter Health Check
--
--Business Logic:
--
--    Device Monitoring: Identifies meters that aren't reporting data (potential hardware issues).
--
--    Usage Anomalies: Flags abnormally low usage (potential meter tampering or faults) or high usage
--    (potential leaks or faults).
--
--    Maintenance Prioritization: Helps field teams prioritize which meters need attention first.
--
--    Asset Management: Provides insights into meter fleet performance and longevity.
--
--Business Impact: Reduces revenue loss from faulty metering, improves operational efficiency in field
--services, and maintains data quality for billing and analytics.

CREATE OR REPLACE FUNCTION check_meter_health()
RETURNS TABLE (
    meter_id VARCHAR(20),
    customer_id VARCHAR(20),
    days_since_last_reading INT,
    avg_daily_consumption DECIMAL(10,4),
    health_status VARCHAR(20)
) AS $$
BEGIN
    RETURN QUERY
    WITH meter_stats AS (
        SELECT 
            dm.meter_id,
            dm.customer_id,
            CURRENT_DATE - MAX(CAST(fmr.timestamp AS DATE)) AS days_since_last_reading,
            AVG(fmr.kwh_usage) AS avg_hourly_consumption
        FROM dim_meter dm
        LEFT JOIN fact_meter_readings fmr ON dm.meter_id = fmr.meter_id
        WHERE dm.is_active = TRUE
        GROUP BY dm.meter_id, dm.customer_id
    )
    SELECT
        ms.meter_id,
        ms.customer_id,
        ms.days_since_last_reading,
        (ms.avg_hourly_consumption * 24)::DECIMAL(10,4) AS avg_daily_consumption,
        CASE
            WHEN ms.days_since_last_reading IS NULL THEN 'NO_READINGS'
            WHEN ms.days_since_last_reading > 7 THEN 'INACTIVE'
            WHEN (ms.avg_hourly_consumption * 24) < 0.5 THEN 'LOW_USAGE'
            WHEN (ms.avg_hourly_consumption * 24) > 50 THEN 'HIGH_USAGE'
            ELSE 'HEALTHY'
        END AS health_status
    FROM meter_stats ms
    ORDER BY 
        CASE 
            WHEN ms.days_since_last_reading IS NULL THEN 1
            WHEN ms.days_since_last_reading > 7 THEN 2
            ELSE 3
        END,
        ms.days_since_last_reading DESC;
END;
$$ LANGUAGE plpgsql;

-- Usage example:
SELECT * FROM check_meter_health();


--4. Weather Impact Analysis
--
--Business Logic:
--
--    Demand Forecasting: Quantifies how temperature changes affect energy demand.
--
--    Peak Load Planning: Helps predict when extreme weather will create system stress.
--
--    Pricing Strategy: Informs dynamic pricing models during weather events.
--
--    Infrastructure Planning: Shows which regions are most weather-sensitive for grid resilience
--    investments.
--
--Business Impact: Improves grid reliability during extreme weather, optimizes generation planning
--(reducing costly peak generation needs), and supports climate adaptation strategies.

CREATE OR REPLACE PROCEDURE analyze_weather_impact(
    region VARCHAR(50),
    start_date DATE,
    end_date DATE
) AS $$
DECLARE
    -- Declare variables if needed
BEGIN
    -- Analyze by temperature ranges
    WITH WeatherImpact AS (
        SELECT 
            CASE 
                WHEN fwd.mean_temp_c < 0 THEN 'Below 0°C'
                WHEN fwd.mean_temp_c BETWEEN 0 AND 10 THEN '0-10°C'
                WHEN fwd.mean_temp_c BETWEEN 10 AND 20 THEN '10-20°C'
                WHEN fwd.mean_temp_c BETWEEN 20 AND 30 THEN '20-30°C'
                ELSE 'Above 30°C'
            END AS temperature_range,
            AVG(fmr.kwh_usage) AS avg_kwh_usage,
            COUNT(*) AS reading_count,
            ROUND(
				(SUM(CASE WHEN fmr.is_peak_hours THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 2
			) AS peak_hours_ratio
        FROM fact_meter_readings fmr
        JOIN fact_weather_daily fwd ON 
            CAST(fmr.timestamp AS DATE) = fwd.observation_date AND
            fmr.region = fwd.region
        WHERE fmr.region = region
        	AND fmr.timestamp >= start_date
          	AND fmr.timestamp <= end_date
        GROUP BY 
            CASE 
                WHEN fwd.mean_temp_c < 0 THEN 'Below 0°C'
                WHEN fwd.mean_temp_c BETWEEN 0 AND 10 THEN '0-10°C'
                WHEN fwd.mean_temp_c BETWEEN 10 AND 20 THEN '10-20°C'
                WHEN fwd.mean_temp_c BETWEEN 20 AND 30 THEN '20-30°C'
                ELSE 'Above 30°C'
            END
    )
    -- Return results with additional analysis
    SELECT 
        temperature_range,
        avg_kwh_usage,
        reading_count,
        peak_hours_ratio,
        ROUND(avg_kwh_usage * peak_hours_ratio / 100, 2) AS peak_contribution,
        ROUND(avg_kwh_usage * (100 - peak_hours_ratio) / 100, 2) AS offpeak_contribution
    FROM WeatherImpact
    ORDER BY 
        CASE temperature_range
            WHEN 'Below 0°C' THEN 1
            WHEN '0-10°C' THEN 2
            WHEN '10-20°C' THEN 3
            WHEN '20-30°C' THEN 4
            ELSE 5
        END;
END;
$$ LANGUAGE plpgsql;

-- Usage example:
CALL analyze_weather_impact('Wellington', '2023-06-01', '2023-08-31');


--5. Customer Migration Between Tariffs
--
--Business Logic:
--
--    Tariff Effectiveness: Measures how tariff changes affect customer behavior and utility revenue.
--
--    Customer Segmentation: Identifies which customer groups benefit most from specific tariffs.
--
--    Consumption Shifts: Quantifies whether tariff changes actually shift usage patterns as intended.
--
--    Financial Impact: Calculates revenue changes from tariff migrations.
--
--Business Impact: Guides tariff design and marketing strategies, helps meet regulatory requirements
--for fair pricing, and balances customer satisfaction with revenue protection.

CREATE OR REPLACE FUNCTION analyze_tariff_migration(
    p_start_date DATE,
    p_end_date DATE
) 
RETURNS TABLE (
    from_tariff VARCHAR(20),
    to_tariff VARCHAR(20),
    customer_count INT,
    avg_consumption_change DECIMAL(10,2),
    avg_bill_change DECIMAL(10,2)
) AS $$
BEGIN
    RETURN QUERY
    WITH customer_changes AS (
        SELECT 
            customer_id,
            FIRST_VALUE(tariff_type) OVER (PARTITION BY customer_id ORDER BY effective_date) AS old_tariff,
            LAST_VALUE(tariff_type) OVER (PARTITION BY customer_id ORDER BY effective_date 
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS new_tariff,
            MIN(effective_date) AS change_date
        FROM (
            SELECT customer_id, tariff_type, start_date AS effective_date
            FROM bridge_customer_address
            UNION ALL
            SELECT customer_id, tariff_type, installation_date AS effective_date
            FROM dim_meter
        ) AS tariff_changes
        WHERE effective_date BETWEEN p_start_date AND p_end_date
        GROUP BY customer_id, tariff_type, effective_date
    ),
    consumption_data AS (
        SELECT 
            cc.customer_id,es 
            cc.old_tariff,
            cc.new_tariff,
            AVG(
				CASE WHEN fmr.timestamp < cc.change_date THEN fmr.kwh_usage ELSE NULL END
			) AS old_avg_usage,
            AVG(
				CASE WHEN fmr.timestamp >= cc.change_date THEN fmr.kwh_usage ELSE NULL END
			) AS new_avg_usage
        FROM customer_changes cc
        JOIN fact_meter_readings fmr ON cc.customer_id = fmr.customer_id
        WHERE fmr.timestamp BETWEEN (p_start_date - INTERVAL '3 months')
			AND (p_end_date + INTERVAL '3 months')
        GROUP BY cc.customer_id, cc.old_tariff, cc.new_tariff
    )
    SELECT 
        cd.old_tariff AS from_tariff,
        cd.new_tariff AS to_tariff,
        COUNT(*) AS customer_count,
        AVG(cd.new_avg_usage - cd.old_avg_usage)::DECIMAL(10,2) AS avg_consumption_change,
        AVG(
            (cd.new_avg_usage * new_t.peak_rate_per_kwh * 0.7 
				+ cd.new_avg_usage * new_t.offpeak_rate_per_kwh * 0.3 
				+ new_t.daily_fixed_charge * 30) -
            (cd.old_avg_usage * old_t.peak_rate_per_kwh * 0.7 
				+ cd.old_avg_usage * old_t.offpeak_rate_per_kwh * 0.3 
				+ old_t.daily_fixed_charge * 30)
        )::DECIMAL(10,2) AS avg_bill_change
    FROM consumption_data cd
    JOIN dim_tariff old_t ON cd.old_tariff = old_t.tariff_code
    JOIN dim_tariff new_t ON cd.new_tariff = new_t.tariff_code
    WHERE cd.old_tariff <> cd.new_tariff
    GROUP BY cd.old_tariff, cd.new_tariff
    ORDER BY customer_count DESC;
END;
$$ LANGUAGE plpgsql;

-- Usage example:
SELECT * FROM analyze_tariff_migration('2023-01-01', '2023-12-31');


--Common Threads in Business Logic:
--
--    Revenue Assurance: All examples ultimately help ensure accurate billing and identify revenue leakage.
--
--    Regulatory Compliance: Utilities operate under strict regulations - these analyses provide auditable
--    data for regulators.
--
--    Demand Management: Critical for balancing supply and demand in an industry where storage is limited.
--
--    Customer Experience: Provides data to offer personalized services and troubleshoot issues.
--
--    Asset Optimization: Helps maximize the value of expensive infrastructure (meters, grid equipment).
--
--These scenarios reflect real-world priorities where utilities must balance:
--
--    Financial viability (accurate billing, cost management)
--
--    Regulatory obligations (fair pricing, reporting)
--
--    System reliability (demand forecasting, infrastructure health)
--
--    Customer satisfaction (transparent billing, useful insights)




