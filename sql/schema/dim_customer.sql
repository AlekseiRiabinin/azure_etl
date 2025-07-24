-- public.dim_customer definition

CREATE TABLE public.dim_customer (
	customer_id varchar(20) NOT NULL,
	office_address text NULL,
	tariff_type varchar(20) NULL,
	email varchar(50) NULL,
	created_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	CONSTRAINT dim_customer_pkey PRIMARY KEY (customer_id)
);
