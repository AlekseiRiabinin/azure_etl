-- public.daily_customer_summary definition

-- Create sequence for the summary table
CREATE SEQUENCE IF NOT EXISTS daily_customer_summary_id_seq;

-- Create the daily customer summary table
CREATE TABLE IF NOT EXISTS public.daily_customer_summary (
    summary_id BIGINT PRIMARY KEY DEFAULT nextval('daily_customer_summary_id_seq'),
    customer_id VARCHAR(50) NOT NULL,
    date DATE NOT NULL,
    total_kwh DECIMAL(12,4) NOT NULL,
    avg_voltage DECIMAL(10,2) NOT NULL,
    total_cost DECIMAL(12,4) NOT NULL,
    record_created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_customer FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id),
    CONSTRAINT unique_customer_date UNIQUE (customer_id, date)
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_daily_summary_customer ON public.daily_customer_summary(customer_id);
CREATE INDEX IF NOT EXISTS idx_daily_summary_date ON public.daily_customer_summary(date);
CREATE INDEX IF NOT EXISTS idx_daily_summary_customer_date ON public.daily_customer_summary(customer_id, date);

-- Add comments for documentation
COMMENT ON TABLE daily_customer_summary IS 'Daily aggregated smart meter readings by customer';
COMMENT ON COLUMN daily_customer_summary.total_kwh IS 'Total kWh consumption for the day';
COMMENT ON COLUMN daily_customer_summary.avg_voltage IS 'Average voltage for the day';
COMMENT ON COLUMN daily_customer_summary.total_cost IS 'Total energy cost for the day based on regional tariffs';
