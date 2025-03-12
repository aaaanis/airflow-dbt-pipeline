-- Create schemas
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS mart;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS production;

-- Create raw tables for our data sources
CREATE TABLE IF NOT EXISTS raw.customers (
    customer_id INTEGER PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(100),
    gender VARCHAR(50),
    country VARCHAR(100),
    created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw.orders (
    order_id INTEGER PRIMARY KEY,
    customer_id INTEGER,
    order_date DATE,
    product_id INTEGER,
    quantity INTEGER,
    price DECIMAL(10, 2),
    total_amount DECIMAL(10, 2),
    status VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS raw.products (
    product_id INTEGER PRIMARY KEY,
    product_name VARCHAR(100),
    category VARCHAR(100),
    description TEXT,
    price DECIMAL(10, 2),
    created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw.weather_data (
    id SERIAL PRIMARY KEY,
    location VARCHAR(100),
    country VARCHAR(100),
    date DATE,
    temperature DECIMAL(5, 2),
    humidity INTEGER,
    weather_condition VARCHAR(100)
); 