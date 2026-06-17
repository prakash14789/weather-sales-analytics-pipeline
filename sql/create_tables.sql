-- DDL to create customer_dim table if it doesn't exist
CREATE TABLE IF NOT EXISTS customer_dim (
    customer_id VARCHAR(50) PRIMARY KEY,
    customer_name VARCHAR(100),
    segment VARCHAR(50),
    city VARCHAR(100),
    state VARCHAR(100),
    region VARCHAR(50)
);
