-- ============================================================
-- PT NUSANTARA DIGITAL COMMERCE (NDC)
-- OLTP Database Schema
-- Operation FRAUD BUSTER - DWDM Studi Kasus
-- ============================================================

DROP TABLE IF EXISTS ndc_promo_usage CASCADE;
DROP TABLE IF EXISTS ndc_refund_records CASCADE;
DROP TABLE IF EXISTS ndc_shipments CASCADE;
DROP TABLE IF EXISTS ndc_payments CASCADE;
DROP TABLE IF EXISTS ndc_order_items CASCADE;
DROP TABLE IF EXISTS ndc_orders CASCADE;
DROP TABLE IF EXISTS ndc_product_reviews CASCADE;
DROP TABLE IF EXISTS ndc_products CASCADE;
DROP TABLE IF EXISTS ndc_categories CASCADE;
DROP TABLE IF EXISTS ndc_customer_devices CASCADE;
DROP TABLE IF EXISTS ndc_customers CASCADE;
DROP TABLE IF EXISTS ndc_promo_codes CASCADE;

-- ============================================================
-- MASTER DATA
-- ============================================================

CREATE TABLE ndc_customers (
    customer_id       SERIAL PRIMARY KEY,
    customer_uuid     UUID DEFAULT gen_random_uuid() UNIQUE,
    full_name         VARCHAR(150) NOT NULL,
    email             VARCHAR(255) NOT NULL UNIQUE,
    phone             VARCHAR(20),
    address           TEXT,
    city              VARCHAR(100),
    province          VARCHAR(100),
    postal_code       VARCHAR(10),
    latitude          DECIMAL(10, 7),
    longitude         DECIMAL(10, 7),
    bank_account      VARCHAR(30),
    bank_name         VARCHAR(50),
    is_active         BOOLEAN DEFAULT TRUE,
    registered_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    account_type      VARCHAR(20) DEFAULT 'regular'  -- regular, premium, corporate
);

CREATE TABLE ndc_categories (
    category_id       SERIAL PRIMARY KEY,
    category_name     VARCHAR(100) NOT NULL,
    parent_category_id INT REFERENCES ndc_categories(category_id),
    is_active         BOOLEAN DEFAULT TRUE
);

CREATE TABLE ndc_products (
    product_id        SERIAL PRIMARY KEY,
    product_uuid      UUID DEFAULT gen_random_uuid() UNIQUE,
    product_name      VARCHAR(255) NOT NULL,
    category_id       INT REFERENCES ndc_categories(category_id),
    base_price        DECIMAL(15, 2) NOT NULL,
    cost_price        DECIMAL(15, 2),
    weight_kg         DECIMAL(8, 3),
    is_active         BOOLEAN DEFAULT TRUE,
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    discontinued_at   TIMESTAMP
);

CREATE TABLE ndc_promo_codes (
    promo_id          SERIAL PRIMARY KEY,
    promo_code        VARCHAR(50) NOT NULL UNIQUE,
    description       TEXT,
    discount_type     VARCHAR(20) NOT NULL, -- percentage, fixed
    discount_value    DECIMAL(15, 2) NOT NULL,
    min_purchase      DECIMAL(15, 2) DEFAULT 0,
    max_discount      DECIMAL(15, 2),
    usage_limit_total INT,
    usage_limit_per_user INT DEFAULT 1,
    valid_from        TIMESTAMP NOT NULL,
    valid_until       TIMESTAMP NOT NULL,
    is_active         BOOLEAN DEFAULT TRUE,
    can_stack         BOOLEAN DEFAULT FALSE,
    created_by        VARCHAR(100)
);

-- ============================================================
-- DEVICE TRACKING
-- ============================================================

CREATE TABLE ndc_customer_devices (
    device_id         SERIAL PRIMARY KEY,
    customer_id       INT REFERENCES ndc_customers(customer_id),
    device_fingerprint VARCHAR(255) NOT NULL,
    device_type       VARCHAR(50),   -- mobile, desktop, tablet
    os                VARCHAR(50),
    browser           VARCHAR(50),
    ip_address        INET,
    first_seen        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_seen         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_flagged        BOOLEAN DEFAULT FALSE
);

-- ============================================================
-- TRANSACTIONS
-- ============================================================

CREATE TABLE ndc_orders (
    order_id          SERIAL PRIMARY KEY,
    order_uuid        UUID DEFAULT gen_random_uuid() UNIQUE,
    customer_id       INT REFERENCES ndc_customers(customer_id),
    order_date        TIMESTAMP NOT NULL,
    status            VARCHAR(30) DEFAULT 'pending',
    total_amount      DECIMAL(15, 2) NOT NULL,
    discount_amount   DECIMAL(15, 2) DEFAULT 0,
    shipping_cost     DECIMAL(15, 2) DEFAULT 0,
    net_amount        DECIMAL(15, 2) NOT NULL,
    promo_id          INT REFERENCES ndc_promo_codes(promo_id),
    shipping_address  TEXT,
    shipping_city     VARCHAR(100),
    shipping_province VARCHAR(100),
    shipping_postal   VARCHAR(10),
    shipping_lat      DECIMAL(10, 7),
    shipping_lng      DECIMAL(10, 7),
    shipping_method   VARCHAR(30) DEFAULT 'regular',  -- regular, express, same_day
    notes             TEXT,
    device_fingerprint VARCHAR(255),
    ip_address        INET,
    is_flagged        BOOLEAN DEFAULT FALSE,
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE ndc_order_items (
    order_item_id     SERIAL PRIMARY KEY,
    order_id          INT REFERENCES ndc_orders(order_id),
    product_id        INT REFERENCES ndc_products(product_id),
    quantity          INT NOT NULL CHECK (quantity > 0),
    unit_price        DECIMAL(15, 2) NOT NULL,
    subtotal          DECIMAL(15, 2) NOT NULL,
    discount_per_item DECIMAL(15, 2) DEFAULT 0
);

CREATE TABLE ndc_payments (
    payment_id        SERIAL PRIMARY KEY,
    order_id          INT REFERENCES ndc_orders(order_id),
    payment_method    VARCHAR(30) NOT NULL, -- credit_card, bank_transfer, e_wallet, cod, virtual_account
    payment_provider  VARCHAR(50),          -- BCA, Mandiri, GoPay, OVO, etc.
    amount            DECIMAL(15, 2) NOT NULL,
    payment_status    VARCHAR(30) DEFAULT 'pending', -- pending, success, failed, refunded
    payment_ref       VARCHAR(100),
    source_account    VARCHAR(50),  -- rekening/e-wallet pengirim
    destination_account VARCHAR(50), -- rekening NDC
    paid_at           TIMESTAMP,
    is_flagged        BOOLEAN DEFAULT FALSE,
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- SHIPPING
-- ============================================================

CREATE TABLE ndc_shipments (
    shipment_id       SERIAL PRIMARY KEY,
    order_id          INT REFERENCES ndc_orders(order_id),
    courier           VARCHAR(50),
    tracking_number   VARCHAR(100),
    shipped_at        TIMESTAMP,
    delivered_at      TIMESTAMP,
    shipping_status   VARCHAR(30) DEFAULT 'preparing', -- preparing, shipped, delivered, returned, failed
    delivery_proof    TEXT,       -- URL foto bukti pengiriman
    recipient_name    VARCHAR(150),
    delivery_notes    TEXT,
    is_flagged        BOOLEAN DEFAULT FALSE
);

-- ============================================================
-- REVIEWS
-- ============================================================

CREATE TABLE ndc_product_reviews (
    review_id         SERIAL PRIMARY KEY,
    product_id        INT REFERENCES ndc_products(product_id),
    customer_id       INT REFERENCES ndc_customers(customer_id),
    order_id          INT REFERENCES ndc_orders(order_id),
    rating            INT NOT NULL CHECK (rating BETWEEN 1 AND 5),
    review_title      VARCHAR(255),
    review_text       TEXT,
    is_verified       BOOLEAN DEFAULT FALSE,
    review_source     VARCHAR(30) DEFAULT 'web', -- web, mobile, api_import
    device_fingerprint VARCHAR(255),
    ip_address        INET,
    is_flagged        BOOLEAN DEFAULT FALSE,
    reviewed_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- REFUNDS
-- ============================================================

CREATE TABLE ndc_refund_records (
    refund_id         SERIAL PRIMARY KEY,
    order_id          INT REFERENCES ndc_orders(order_id),
    customer_id       INT REFERENCES ndc_customers(customer_id),
    refund_amount     DECIMAL(15, 2) NOT NULL,
    refund_reason     VARCHAR(100),
    refund_category   VARCHAR(50), -- product_damage, wrong_item, not_received, duplicate, other
    refund_to_account VARCHAR(50),  -- rekening tujuan refund
    refund_to_bank    VARCHAR(50),
    refund_status     VARCHAR(30) DEFAULT 'pending', -- pending, approved, processed, rejected
    processed_by      VARCHAR(100),
    processed_at      TIMESTAMP,
    is_flagged        BOOLEAN DEFAULT FALSE,
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- PROMO USAGE LOG
-- ============================================================

CREATE TABLE ndc_promo_usage (
    usage_id          SERIAL PRIMARY KEY,
    promo_id          INT REFERENCES ndc_promo_codes(promo_id),
    order_id          INT REFERENCES ndc_orders(order_id),
    customer_id       INT REFERENCES ndc_customers(customer_id),
    device_fingerprint VARCHAR(255),
    ip_address        INET,
    used_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_flagged        BOOLEAN DEFAULT FALSE
);

-- ============================================================
-- INDEXES untuk fraud analysis
-- ============================================================

CREATE INDEX idx_orders_customer ON ndc_orders(customer_id);
CREATE INDEX idx_orders_date ON ndc_orders(order_date);
CREATE INDEX idx_orders_flagged ON ndc_orders(is_flagged) WHERE is_flagged = TRUE;
CREATE INDEX idx_orders_device ON ndc_orders(device_fingerprint);
CREATE INDEX idx_orders_ip ON ndc_orders(ip_address);
CREATE INDEX idx_payments_method ON ndc_payments(payment_method);
CREATE INDEX idx_payments_flagged ON ndc_payments(is_flagged) WHERE is_flagged = TRUE;
CREATE INDEX idx_shipments_status ON ndc_shipments(shipping_status);
CREATE INDEX idx_reviews_product ON ndc_product_reviews(product_id);
CREATE INDEX idx_reviews_flagged ON ndc_product_reviews(is_flagged) WHERE is_flagged = TRUE;
CREATE INDEX idx_refunds_customer ON ndc_refund_records(customer_id);
CREATE INDEX idx_refunds_flagged ON ndc_refund_records(is_flagged) WHERE is_flagged = TRUE;
CREATE INDEX idx_promo_usage_customer ON ndc_promo_usage(customer_id);
CREATE INDEX idx_devices_fingerprint ON ndc_customer_devices(device_fingerprint);
CREATE INDEX idx_devices_ip ON ndc_customer_devices(ip_address);
CREATE INDEX idx_devices_flagged ON ndc_customer_devices(is_flagged) WHERE is_flagged = TRUE;
