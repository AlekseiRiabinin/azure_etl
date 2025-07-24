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