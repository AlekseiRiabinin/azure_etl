-- public.fact_billing definition

CREATE TABLE public.fact_billing (
	bill_id varchar(36) NOT NULL,
	customer_id varchar(20) NOT NULL,
	billing_month date NOT NULL,
	total_kwh numeric(10, 2) NOT NULL,
	peak_kwh numeric(10, 2) NULL,
	offpeak_kwh numeric(10, 2) NULL,
	amount_due numeric(10, 2) NOT NULL,
	paid_amount numeric(10, 2) NULL,
	payment_date timestamptz NULL,
	late_fee numeric(10, 2) NULL,
	discount_applied numeric(10, 2) NULL,
	region varchar(50) NOT NULL,
	CONSTRAINT fact_billing_pkey PRIMARY KEY (bill_id)
);

ALTER TABLE public.fact_billing
ADD CONSTRAINT fk_billing_customer FOREIGN KEY (customer_id) REFERENCES public.dim_customer(customer_id);
