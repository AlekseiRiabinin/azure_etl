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