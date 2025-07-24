-- public.dim_meter definition

CREATE TABLE public.dim_meter (
	meter_id varchar(20) NOT NULL,
	customer_id varchar(20) NULL,
	installation_date date DEFAULT CURRENT_DATE NOT NULL,
	meter_type varchar(30) DEFAULT 'SMART_V3'::character varying NOT NULL,
	manufacturer varchar(50) NULL,
	firmware_version varchar(20) NULL,
	last_calibration_date date NULL,
	is_active bool DEFAULT true NULL,
	grid_reference varchar(30) NULL,
	CONSTRAINT dim_meter_pkey PRIMARY KEY (meter_id)
);
