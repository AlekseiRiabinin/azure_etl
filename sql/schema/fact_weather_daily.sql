-- public.fact_weather_daily definition

CREATE TABLE public.fact_weather_daily (
	observation_id VARCHAR(36) NOT NULL,
	station_id VARCHAR(10) NOT NULL,
	observation_date DATE NOT NULL,
	max_temp_c NUMERIC(4, 1) NULL,
	min_temp_c NUMERIC(4, 1) NULL,
	mean_temp_c NUMERIC(4, 1) NULL,
	rainfall_mm NUMERIC(6, 1) NULL,
	sunshine_hours NUMERIC(4, 1) NULL,
	wind_speed_kmh NUMERIC(5, 1) NULL,
	region VARCHAR(50) NOT NULL,
	CONSTRAINT fact_weather_daily_pkey PRIMARY KEY (observation_id)
);


-- public.fact_weather_daily foreign keys

ALTER TABLE public.fact_weather_daily
ADD CONSTRAINT fk_weather_station FOREIGN KEY (station_id) REFERENCES public.dim_weather_station(station_id);
