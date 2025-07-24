-- public.bridge_customer_address definition

CREATE TABLE public.bridge_customer_address (
	address_id serial4 NOT NULL,
	customer_id varchar(20) NOT NULL,
	address text NOT NULL,
	start_date date NOT NULL,
	end_date date NULL,
	is_current bool DEFAULT false NULL,
	region varchar(50) NOT NULL,
	CONSTRAINT bridge_customer_address_pkey PRIMARY KEY (address_id)
);

ALTER TABLE public.bridge_customer_address
ADD CONSTRAINT fk_address_customer FOREIGN KEY (customer_id) REFERENCES public.dim_customer(customer_id);
