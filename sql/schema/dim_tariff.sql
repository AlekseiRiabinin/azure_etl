-- public.dim_tariff definition

CREATE TABLE public.dim_tariff (
	tariff_code varchar(10) NOT NULL,
	tariff_name varchar(50) NOT NULL,
	description text NULL,
	peak_rate_per_kwh numeric(8, 4) NOT NULL,
	offpeak_rate_per_kwh numeric(8, 4) NOT NULL,
	daily_fixed_charge numeric(8, 2) NOT NULL,
	applicable_from date NOT NULL,
	applicable_to date NULL,
	is_current bool DEFAULT true NULL,
	CONSTRAINT dim_tariff_pkey PRIMARY KEY (tariff_code)
);
