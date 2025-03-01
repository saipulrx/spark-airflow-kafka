CREATE TABLE IF NOT EXISTS furniture (
    order_id VARCHAR(255),
    customer_id INTEGER,
    furniture VARCHAR(255),
    color VARCHAR(255),
    price INTEGER,
    ts BIGINT
);

SELECT * FROM furniture;