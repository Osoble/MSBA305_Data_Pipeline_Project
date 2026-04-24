-- ============================================================
-- MSBA 305 — PostgreSQL Schema
-- Olist E-Commerce Dataset
-- ============================================================

CREATE TABLE geolocation (
  geolocation_zip_code_prefix BIGINT PRIMARY KEY,
  geolocation_lat DOUBLE PRECISION,
  geolocation_lng DOUBLE PRECISION,
  geolocation_city TEXT,
  geolocation_state TEXT
);

CREATE TABLE product_categories (
  product_category_name TEXT PRIMARY KEY,
  product_category_name_english TEXT
);

CREATE TABLE customers (
  customer_id TEXT PRIMARY KEY,
  customer_unique_id TEXT,
  customer_zip_code_prefix BIGINT,
  customer_city TEXT,
  customer_state TEXT,
  FOREIGN KEY (customer_zip_code_prefix)
    REFERENCES geolocation(geolocation_zip_code_prefix)
);

CREATE TABLE sellers (
  seller_id TEXT PRIMARY KEY,
  seller_zip_code_prefix BIGINT,
  seller_city TEXT,
  seller_state TEXT,
  FOREIGN KEY (seller_zip_code_prefix)
    REFERENCES geolocation(geolocation_zip_code_prefix)
);

CREATE TABLE products (
  product_id TEXT PRIMARY KEY,
  product_category_name TEXT,
  product_name_lenght INTEGER,
  product_description_lenght INTEGER,
  product_photos_qty INTEGER,
  product_weight_g INTEGER,
  product_length_cm INTEGER,
  product_height_cm INTEGER,
  product_width_cm INTEGER,
  FOREIGN KEY (product_category_name)
    REFERENCES product_categories(product_category_name)
);

CREATE TABLE orders (
  order_id TEXT PRIMARY KEY,
  customer_id TEXT,
  order_status TEXT,
  order_purchase_timestamp TIMESTAMP,
  order_approved_at TIMESTAMP,
  order_delivered_carrier_date TIMESTAMP,
  order_delivered_customer_date TIMESTAMP,
  order_estimated_delivery_date TIMESTAMP,
  is_delivered BOOLEAN,
  delivery_days INTEGER,
  delivery_delay INTEGER,
  order_year INTEGER,
  order_month TEXT,
  total_order_value REAL,
  is_high_value BOOLEAN,
  is_valid_delivery BOOLEAN,
  delivery_data_complete BOOLEAN,
  has_review BOOLEAN,
  FOREIGN KEY (customer_id)
    REFERENCES customers(customer_id)
);

CREATE TABLE order_items (
  order_id TEXT,
  order_item_id INTEGER,
  product_id TEXT,
  seller_id TEXT,
  shipping_limit_date TIMESTAMP,
  price REAL,
  freight_value REAL,
  PRIMARY KEY (order_id, order_item_id),
  FOREIGN KEY (order_id) REFERENCES orders(order_id),
  FOREIGN KEY (product_id) REFERENCES products(product_id),
  FOREIGN KEY (seller_id) REFERENCES sellers(seller_id)
);

CREATE TABLE order_payments (
  order_id TEXT,
  payment_sequential INTEGER,
  payment_type TEXT,
  payment_installments INTEGER,
  payment_value REAL,
  PRIMARY KEY (order_id, payment_sequential),
  FOREIGN KEY (order_id) REFERENCES orders(order_id)
);

CREATE TABLE order_reviews (
  review_id TEXT PRIMARY KEY,
  order_id TEXT,
  review_score INTEGER,
  review_comment_title TEXT,
  review_comment_message TEXT,
  review_creation_date TIMESTAMP,
  review_answer_timestamp TIMESTAMP,
  FOREIGN KEY (order_id) REFERENCES orders(order_id)
);

CREATE TABLE countries_gdp (
  year INTEGER PRIMARY KEY,
  gdp_per_capita_usd REAL
);

CREATE TABLE holidays (
  holiday_id SERIAL PRIMARY KEY,
  country_or_region TEXT,
  holiday_name TEXT,
  holiday_date DATE
);
