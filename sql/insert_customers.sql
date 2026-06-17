-- DML to extract unique customer profiles from staging customers table and load into customer_dim
INSERT INTO customer_dim (customer_id, customer_name, segment, city, state, region)
SELECT DISTINCT ON (customer_id) customer_id, customer_name, segment, city, state, region
FROM customers
ON CONFLICT (customer_id) DO UPDATE SET
    customer_name = EXCLUDED.customer_name,
    segment = EXCLUDED.segment,
    city = EXCLUDED.city,
    state = EXCLUDED.state,
    region = EXCLUDED.region;
