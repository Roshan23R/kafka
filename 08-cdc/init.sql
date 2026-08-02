-- Runs automatically the FIRST time the postgres container starts
-- (via Postgres's /docker-entrypoint-initdb.d/ convention).

CREATE TABLE orders (
    order_id      SERIAL PRIMARY KEY,
    customer_name VARCHAR(100) NOT NULL,
    status        VARCHAR(20)  NOT NULL DEFAULT 'PENDING',
    amount        NUMERIC(10, 2) NOT NULL,
    updated_at    TIMESTAMP DEFAULT NOW()
);

-- REPLICA IDENTITY FULL means Debezium captures the FULL "before" row on
-- UPDATE/DELETE, not just the primary key. Without this, an UPDATE event
-- would only tell you the new values, not what changed FROM.
ALTER TABLE orders REPLICA IDENTITY FULL;

INSERT INTO orders (customer_name, status, amount) VALUES
    ('Alice',  'PENDING', 120.50),
    ('Bob',    'PENDING', 75.00),
    ('Carla',  'PENDING', 340.25);
