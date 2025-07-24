-- public.dim_weather_station definition

CREATE TABLE public.dim_weather_station (
	station_id VARCHAR(10) NOT NULL,
	station_name VARCHAR(100) NOT NULL,
	region VARCHAR(50) NOT NULL,
	latitude NUMERIC(9, 6) NULL,
	longitude NUMERIC(9, 6) NULL,
	elevation_m NUMERIC(6, 1) NULL,
	CONSTRAINT dim_weather_station_pkey PRIMARY KEY (station_id)
);
