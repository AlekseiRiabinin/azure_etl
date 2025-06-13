-- dim_tariff table data
TRUNCATE TABLE dim_tariff;
INSERT INTO dim_tariff VALUES
('PEAK', 'Residential Peak/Off-Peak', 'Standard residential tariff with peak and off-peak rates', 0.28, 0.18, 1.2, '2020-01-01', NULL, TRUE),
('OFFPEAK', 'Residential Off-Peak Only', 'For customers with off-peak usage only', 0.32, 0.15, 1.0, '2021-04-01', NULL, TRUE),
('TIMEOFUSE', 'Time of Use', 'Variable rates based on time of day', 0.35, 0.12, 0.8, '2022-07-01', NULL, TRUE),
('LOWUSER', 'Low User', 'For low consumption households', 0.3, 0.2, 0.6, '2023-01-01', NULL, TRUE);
