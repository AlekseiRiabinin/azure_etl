CREATE OR REPLACE FUNCTION mask_email(email TEXT) RETURNS TEXT AS
$$
BEGIN
    RETURN regexp_replace(email, '[^@]+', '****', 'g');
END;
$$
LANGUAGE plpgsql;

-- Use a view to simulate RBAC-controlled masking
CREATE VIEW secure_dim_customer AS
SELECT 
    customer_id,
    address,
    tariff_plan,
    CASE 
        WHEN current_user = 'data_analyst' THEN mask_email(masked_contact)
        ELSE masked_contact
    END AS masked_contact
FROM dim_customer;
