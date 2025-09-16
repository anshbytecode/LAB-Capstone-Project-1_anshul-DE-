-- Dimension tables
CREATE TABLE IF NOT EXISTS dim_store (
    store_id TEXT PRIMARY KEY,
    store_name TEXT,
    store_city TEXT,
    store_state TEXT,
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_id TEXT PRIMARY KEY,
    product_name TEXT,
    category TEXT
);

CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id TEXT PRIMARY KEY,
    customer_name TEXT,
    customer_segment TEXT
);

-- Weather dimension
CREATE TABLE IF NOT EXISTS dim_weather (
    weather_id SERIAL PRIMARY KEY,
    store_id TEXT REFERENCES dim_store(store_id),
    weather_date DATE,
    temp_c DOUBLE PRECISION,
    humidity INTEGER,
    weather_main TEXT,
    weather_description TEXT,
    wind_speed DOUBLE PRECISION
);

-- Fact table
CREATE TABLE IF NOT EXISTS fact_sales (
    sale_id TEXT PRIMARY KEY,
    sale_date TIMESTAMP,
    store_id TEXT REFERENCES dim_store(store_id),
    customer_id TEXT REFERENCES dim_customer(customer_id),
    product_id TEXT REFERENCES dim_product(product_id),
    qty INTEGER,
    price NUMERIC(10,2),
    total NUMERIC(12,2),
    payment_method TEXT,
    weather_id INTEGER REFERENCES dim_weather(weather_id)
);

-- Audit logs
CREATE TABLE IF NOT EXISTS etl_audit (
    id SERIAL PRIMARY KEY,
    run_ts TIMESTAMP DEFAULT now(),
    source_file TEXT,
    rows_in INTEGER,
    rows_out INTEGER,
    rows_rejected INTEGER,
    notes TEXT
);
