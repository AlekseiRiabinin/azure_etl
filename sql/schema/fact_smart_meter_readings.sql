-- public.fact_smart_meter_readings definition

-- Create sequence for auto-incrementing primary key
CREATE SEQUENCE IF NOT EXISTS smart_meter_reading_id_seq;

-- Create the fact_smart_meter_readings table with proper schema
CREATE TABLE IF NOT EXISTS public.fact_smart_meter_readings (
    reading_id BIGINT PRIMARY KEY DEFAULT nextval('smart_meter_reading_id_seq'),
    meter_id VARCHAR(50) NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    kwh_usage DECIMAL(10,4) NOT NULL,
    voltage INTEGER NOT NULL CHECK (voltage IN (230, 240)),
    customer_id VARCHAR(50) NOT NULL,
    region VARCHAR(50) NOT NULL,
    hour_of_day INTEGER NOT NULL CHECK (hour_of_day BETWEEN 0 AND 23),
    cost DECIMAL(10,4) NOT NULL,
    is_peak BOOLEAN NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    processing_time TIMESTAMP WITH TIME ZONE NOT NULL,
    date DATE NOT NULL,
    data_quality_flag VARCHAR(10) NOT NULL CHECK (data_quality_flag IN ('VALID', 'INVALID')),
    source_system VARCHAR(50) NOT NULL,
    CONSTRAINT fk_customer FOREIGN KEY (customer_id) REFERENCES public.dim_customer(customer_id),
    CONSTRAINT fk_meter FOREIGN KEY (meter_id) REFERENCES public.dim_meter(meter_id)
    CONSTRAINT unique_meter_timestamp UNIQUE (meter_id, timestamp)
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_smart_meter_timestamp ON public.fact_smart_meter_readings(timestamp);
CREATE INDEX IF NOT EXISTS idx_smart_meter_customer_id ON public.fact_smart_meter_readings(customer_id);
CREATE INDEX IF NOT EXISTS idx_smart_meter_region_date ON public.fact_smart_meter_readings(region, date);
CREATE INDEX IF NOT EXISTS idx_smart_meter_quality_flag ON public.fact_smart_meter_readings(data_quality_flag);

-- Create comment for documentation
COMMENT ON TABLE fact_smart_meter_readings IS 'Stores raw smart meter readings with quality flags and derived metrics';
COMMENT ON COLUMN fact_smart_meter_readings.reading_id IS 'Auto-incrementing primary key';
COMMENT ON COLUMN fact_smart_meter_readings.data_quality_flag IS 'VALID/INVALID based on data quality checks';
