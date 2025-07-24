-- public.fact_meter_readings definition

CREATE TABLE public.fact_meter_readings (
	reading_id varchar(36) NOT NULL,
	meter_id varchar(20) NOT NULL,
	customer_id varchar(20) NOT NULL,
	"timestamp" timestamptz NOT NULL,
	kwh_usage numeric(10, 4) NOT NULL,
	voltage numeric(6, 2) NULL,
	current_amps numeric(6, 2) NULL,
	power_factor numeric(4, 2) NULL,
	region varchar(50) NOT NULL,
	is_peak_hours bool NOT NULL,
	weather_condition varchar(20) NULL,
	temperature_c numeric(4, 1) NULL,
	CONSTRAINT fact_meter_readings_pkey PRIMARY KEY (reading_id)
);
CREATE INDEX idx_meter_readings_customer ON public.fact_meter_readings USING btree (customer_id);
CREATE INDEX idx_meter_readings_timestamp ON public.fact_meter_readings USING btree ("timestamp");

-- Table Triggers
CREATE trigger usage_anomaly_check
AFTER INSERT ON
    public.fact_meter_readings for each row execute function check_usage_anomaly();
CREATE trigger detect_high_consumption_trigger BEFORE INSERT ON
    public.fact_meter_readings for each row execute function check_anomalous_readings();

ALTER TABLE public.fact_meter_readings
ADD CONSTRAINT fk_meter FOREIGN KEY (meter_id) REFERENCES public.dim_meter(meter_id);
