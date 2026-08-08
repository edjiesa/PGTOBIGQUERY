-- PostgreSQL 10.4 Sample Database Initialization Script for Testing Migration

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Table 1: Users table with standard types & UUID
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    uuid_id UUID DEFAULT uuid_generate_v4(),
    username VARCHAR(50) NOT NULL,
    email TEXT NOT NULL,
    age INT,
    balance NUMERIC(15, 2) DEFAULT 0.00,
    is_active BOOLEAN DEFAULT true,
    tags text[],
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Table 2: Orders table with JSONB and timestamps
CREATE TABLE IF NOT EXISTS orders (
    order_id BIGSERIAL PRIMARY KEY,
    user_id INT REFERENCES users(id),
    order_number VARCHAR(100) NOT NULL,
    total_amount DECIMAL(12, 4) NOT NULL,
    metadata JSONB,
    order_date DATE DEFAULT CURRENT_DATE,
    shipped_at TIMESTAMP WITHOUT TIME ZONE
);

-- Table 3: Product inventory with double precision and bytea
CREATE TABLE IF NOT EXISTS products (
    product_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    stock SMALLINT DEFAULT 0,
    thumbnail BYTEA,
    attributes JSON
);

-- Populate Sample Data
INSERT INTO users (username, email, age, balance, is_active, tags) VALUES
('budi_santoso', 'budi@example.com', 32, 1550000.50, true, ARRAY['vip', 'indonesia', 'tech']),
('siti_aminah', 'siti@example.com', 28, 2750000.00, true, ARRAY['indonesia', 'finance']),
('john_doe', 'john@example.com', 45, 950.75, false, ARRAY['international']);

INSERT INTO orders (user_id, order_number, total_amount, metadata, order_date, shipped_at) VALUES
(1, 'ORD-2026-001', 500000.00, '{"payment_method": "qris", "items_count": 2, "discount": 10.5}', '2026-08-01', '2026-08-02 10:30:00'),
(2, 'ORD-2026-002', 1250000.00, '{"payment_method": "bank_transfer", "items_count": 1}', '2026-08-05', '2026-08-06 14:15:00'),
(1, 'ORD-2026-003', 75000.50, '{"payment_method": "credit_card", "notes": "Urgent delivery"}', '2026-08-08', NULL);

INSERT INTO products (name, price, stock, attributes) VALUES
('Laptop Gaming Ultra', 18500000.99, 15, '{"brand": "Asus", "ram": "32GB", "storage": "1TB SSD"}'),
('Mechanical Keyboard RGB', 850000.00, 50, '{"switch": "Red", "wireless": true}'),
('Wireless Mouse Ergonomic', 350000.50, 100, '{"dpi": 16000, "battery": "rechargeable"}');
