-- =============================================================================
-- Nusantara Digital Commerce (NDC) - OLAP Star Schema for Fraud Analytics
-- =============================================================================
-- This script creates a star schema optimized for fraud detection and analysis.
-- Source: OLTP tables (ndc_customers, ndc_categories, ndc_products, ndc_orders,
--         ndc_order_items, ndc_payments, ndc_shipments, ndc_product_reviews,
--         ndc_refund_records, ndc_promo_codes, ndc_promo_usage, ndc_customer_devices)
--
-- Schema Structure:
--   Dimensions : dim_customer, dim_product, dim_date, dim_location,
--                dim_payment_method, dim_device
--   Facts      : fact_transactions, fact_reviews, fact_refunds
--   Aggregate  : agg_daily_fraud_summary
-- =============================================================================

-- =============================================================================
-- DROP TABLES (reverse dependency order - facts first, then dimensions)
-- =============================================================================

DROP TABLE IF EXISTS agg_daily_fraud_summary CASCADE;
DROP TABLE IF EXISTS fact_refunds CASCADE;
DROP TABLE IF EXISTS fact_reviews CASCADE;
DROP TABLE IF EXISTS fact_transactions CASCADE;
DROP TABLE IF EXISTS dim_device CASCADE;
DROP TABLE IF EXISTS dim_payment_method CASCADE;
DROP TABLE IF EXISTS dim_location CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;
DROP TABLE IF EXISTS dim_product CASCADE;
DROP TABLE IF EXISTS dim_customer CASCADE;

-- =============================================================================
-- DIMENSION TABLES
-- =============================================================================

-- -----------------------------------------------------------------------------
-- dim_customer
-- Purpose : Customer dimension for analyzing fraud patterns by customer profile,
--           geography, account type, and registration age. Enables segmentation
--           of fraudulent activity by customer attributes.
-- Source  : ndc_customers
-- -----------------------------------------------------------------------------
CREATE TABLE dim_customer (
    customer_id         SERIAL          PRIMARY KEY,
    source_customer_id  INT             NOT NULL UNIQUE,
    full_name           VARCHAR(200)    NOT NULL,
    city                VARCHAR(100),
    province            VARCHAR(100),
    account_type        VARCHAR(20)     NOT NULL DEFAULT 'regular',
    is_active           BOOLEAN         NOT NULL DEFAULT TRUE,
    registered_date     DATE            NOT NULL
);

COMMENT ON TABLE dim_customer IS 'Customer dimension - denormalized from ndc_customers for slicing fraud metrics by customer profile and geography';

CREATE INDEX idx_dim_customer_city ON dim_customer (city);
CREATE INDEX idx_dim_customer_province ON dim_customer (province);
CREATE INDEX idx_dim_customer_account_type ON dim_customer (account_type);
CREATE INDEX idx_dim_customer_is_active ON dim_customer (is_active);
CREATE INDEX idx_dim_customer_registered_date ON dim_customer (registered_date);

-- -----------------------------------------------------------------------------
-- dim_product
-- Purpose : Product dimension for identifying which product categories are most
--           susceptible to fraud (e.g., fake reviews, refund abuse, price
--           manipulation). Denormalizes category hierarchy for simpler queries.
-- Source  : ndc_products JOIN ndc_categories
-- -----------------------------------------------------------------------------
CREATE TABLE dim_product (
    product_id          SERIAL          PRIMARY KEY,
    source_product_id   INT             NOT NULL UNIQUE,
    product_name        VARCHAR(300)    NOT NULL,
    category_name       VARCHAR(100),
    subcategory_name    VARCHAR(100),
    base_price          DECIMAL(12,2)   NOT NULL DEFAULT 0,
    is_active           BOOLEAN         NOT NULL DEFAULT TRUE
);

COMMENT ON TABLE dim_product IS 'Product dimension - denormalized from ndc_products and ndc_categories for analyzing fraud by product and category';

CREATE INDEX idx_dim_product_category ON dim_product (category_name);
CREATE INDEX idx_dim_product_subcategory ON dim_product (subcategory_name);
CREATE INDEX idx_dim_product_is_active ON dim_product (is_active);
CREATE INDEX idx_dim_product_base_price ON dim_product (base_price);

-- -----------------------------------------------------------------------------
-- dim_date
-- Purpose : Date dimension for time-series fraud analysis. Enables drill-down
--           by day, week, month, quarter, and year. Includes weekend and
--           holiday flags to detect temporal fraud patterns (e.g., fraud spikes
--           on weekends or holidays when monitoring is reduced).
-- Source  : Generated from all distinct dates in ndc_orders.order_date,
--           ndc_product_reviews.review_date, ndc_refund_records.refund_date
-- -----------------------------------------------------------------------------
CREATE TABLE dim_date (
    date_id             INT             PRIMARY KEY,    -- YYYYMMDD format
    full_date           DATE            NOT NULL UNIQUE,
    day_of_week         INT             NOT NULL,       -- 1=Monday .. 7=Sunday
    day_name            VARCHAR(10)     NOT NULL,       -- Monday .. Sunday
    month               INT             NOT NULL,       -- 1 .. 12
    month_name          VARCHAR(10)     NOT NULL,       -- January .. December
    quarter             INT             NOT NULL,       -- 1 .. 4
    year                INT             NOT NULL,
    is_weekend          BOOLEAN         NOT NULL DEFAULT FALSE,
    is_holiday          BOOLEAN         NOT NULL DEFAULT FALSE
);

COMMENT ON TABLE dim_date IS 'Date dimension - supports time-series fraud analysis with drill-down by day, month, quarter, year; includes weekend/holiday flags for temporal pattern detection';

CREATE INDEX idx_dim_date_full_date ON dim_date (full_date);
CREATE INDEX idx_dim_date_year_month ON dim_date (year, month);
CREATE INDEX idx_dim_date_quarter ON dim_date (year, quarter);
CREATE INDEX idx_dim_date_is_weekend ON dim_date (is_weekend);
CREATE INDEX idx_dim_date_is_holiday ON dim_date (is_holiday);

-- -----------------------------------------------------------------------------
-- dim_location
-- Purpose : Location dimension for geographic fraud analysis. Detects fraud
--           clusters by city/province and identifies suspicious locations via
--           invalid coordinates. Useful for spotting shipping address anomalies
--           and location-based fraud rings.
-- Source  : ndc_customers (city, province), ndc_shipments (address fields)
-- -----------------------------------------------------------------------------
CREATE TABLE dim_location (
    location_id         SERIAL          PRIMARY KEY,
    city                VARCHAR(100),
    province            VARCHAR(100),
    postal_code         VARCHAR(10),
    latitude            DECIMAL(9,6),
    longitude           DECIMAL(9,6),
    is_valid_coordinates BOOLEAN        NOT NULL DEFAULT TRUE
);

COMMENT ON TABLE dim_location IS 'Location dimension - enables geographic fraud analysis; is_valid_coordinates flags suspicious or missing geo data for fraud detection';

CREATE INDEX idx_dim_location_city ON dim_location (city);
CREATE INDEX idx_dim_location_province ON dim_location (province);
CREATE INDEX idx_dim_location_postal_code ON dim_location (postal_code);
CREATE INDEX idx_dim_location_is_valid ON dim_location (is_valid_coordinates);
CREATE INDEX idx_dim_location_coords ON dim_location (latitude, longitude);

-- -----------------------------------------------------------------------------
-- dim_payment_method
-- Purpose : Payment method dimension for analyzing fraud patterns by payment
--           channel. Helps identify which payment providers, methods, or
--           categories (e.g., e-wallet, COD, credit card) have higher fraud
--           rates and may require enhanced monitoring.
-- Source  : ndc_payments (denormalized payment method details)
-- -----------------------------------------------------------------------------
CREATE TABLE dim_payment_method (
    payment_method_id    SERIAL         PRIMARY KEY,
    payment_method       VARCHAR(50)    NOT NULL,       -- e.g., credit_card, e_wallet, cod
    payment_provider     VARCHAR(100),                  -- e.g., Midtrans, OVO, GoPay
    payment_type_category VARCHAR(50)   NOT NULL        -- e.g., digital, cash, card
);

COMMENT ON TABLE dim_payment_method IS 'Payment method dimension - analyzes fraud rates by payment channel, provider, and type category';

CREATE INDEX idx_dim_payment_method ON dim_payment_method (payment_method);
CREATE INDEX idx_dim_payment_provider ON dim_payment_method (payment_provider);
CREATE INDEX idx_dim_payment_category ON dim_payment_method (payment_type_category);

-- -----------------------------------------------------------------------------
-- dim_device
-- Purpose : Device dimension for detecting device-based fraud patterns such as
--           multiple accounts from the same device fingerprint, suspicious IP
--           ranges, and flagged devices. The ip_prefix (first 3 octets) enables
--   )       grouping by subnet to detect fraud rings operating from the same
--           network segment.
-- Source  : ndc_customer_devices
-- -----------------------------------------------------------------------------
CREATE TABLE dim_device (
    device_id           SERIAL          PRIMARY KEY,
    device_fingerprint  VARCHAR(64)     NOT NULL,
    device_type         VARCHAR(30),                    -- e.g., mobile, desktop, tablet
    os                  VARCHAR(50),                    -- e.g., Android 14, iOS 17, Windows 11
    browser             VARCHAR(50),                    -- e.g., Chrome 120, Safari 17
    ip_address          VARCHAR(45),                    -- IPv4 or IPv6
    ip_prefix           VARCHAR(15),                    -- First 3 octets for subnet grouping
    is_flagged          BOOLEAN         NOT NULL DEFAULT FALSE
);

COMMENT ON TABLE dim_device IS 'Device dimension - tracks device fingerprints, IP addresses, and browser signatures for device-based fraud detection; is_flagged marks previously suspicious devices';

CREATE INDEX idx_dim_device_fingerprint ON dim_device (device_fingerprint);
CREATE INDEX idx_dim_device_type ON dim_device (device_type);
CREATE INDEX idx_dim_device_ip_prefix ON dim_device (ip_prefix);
CREATE INDEX idx_dim_device_is_flagged ON dim_device (is_flagged);
CREATE INDEX idx_dim_device_ip_address ON dim_device (ip_address);

-- =============================================================================
-- FACT TABLES
-- =============================================================================

-- -----------------------------------------------------------------------------
-- fact_transactions
-- Purpose : Core transaction fact table - the central fact for fraud analytics.
--           Each row represents one order item (product line) with full
--           dimensional context. Supports analysis of transaction-level fraud
--           including suspicious pricing, promo abuse, unusual purchase
--           patterns, and device-linked anomalies.
-- Source  : ndc_order_items JOIN ndc_orders JOIN ndc_payments JOIN
--           ndc_customer_devices JOIN ndc_promo_usage
-- -----------------------------------------------------------------------------
CREATE TABLE fact_transactions (
    transaction_id      BIGSERIAL       PRIMARY KEY,
    order_id            INT             NOT NULL,
    customer_key        INT             NOT NULL REFERENCES dim_customer(customer_id),
    product_key         INT             REFERENCES dim_product(product_id),  -- NULL for ghost transactions
    date_key            INT             NOT NULL REFERENCES dim_date(date_id),
    location_key        INT             NOT NULL REFERENCES dim_location(location_id),
    payment_key         INT             NOT NULL REFERENCES dim_payment_method(payment_method_id),
    device_key          INT             REFERENCES dim_device(device_id),
    quantity            INT             NOT NULL DEFAULT 1,
    unit_price          DECIMAL(12,2)   DEFAULT 0,
    total_amount        DECIMAL(12,2)   NOT NULL,
    discount_amount     DECIMAL(12,2)   NOT NULL DEFAULT 0,
    shipping_cost       DECIMAL(12,2)   NOT NULL DEFAULT 0,
    net_amount          DECIMAL(12,2)   NOT NULL,
    has_promo           BOOLEAN         NOT NULL DEFAULT FALSE,
    promo_id            INT,                                    -- references ndc_promo_codes
    is_flagged          BOOLEAN         NOT NULL DEFAULT FALSE
);

COMMENT ON TABLE fact_transactions IS 'Core transaction fact table - each row is one order-item with full dimensional context for fraud pattern analysis';

CREATE INDEX idx_fact_txn_order_id ON fact_transactions (order_id);
CREATE INDEX idx_fact_txn_customer_key ON fact_transactions (customer_key);
CREATE INDEX idx_fact_txn_product_key ON fact_transactions (product_key);
CREATE INDEX idx_fact_txn_date_key ON fact_transactions (date_key);
CREATE INDEX idx_fact_txn_location_key ON fact_transactions (location_key);
CREATE INDEX idx_fact_txn_payment_key ON fact_transactions (payment_key);
CREATE INDEX idx_fact_txn_device_key ON fact_transactions (device_key);
CREATE INDEX idx_fact_txn_is_flagged ON fact_transactions (is_flagged);
CREATE INDEX idx_fact_txn_has_promo ON fact_transactions (has_promo);
CREATE INDEX idx_fact_txn_date_flagged ON fact_transactions (date_key, is_flagged);
CREATE INDEX idx_fact_txn_customer_date ON fact_transactions (customer_key, date_key);
CREATE INDEX idx_fact_txn_net_amount ON fact_transactions (net_amount);

-- -----------------------------------------------------------------------------
-- fact_reviews
-- Purpose : Review fact table for detecting fake reviews and review manipulation.
--           Each row represents one product review with dimensional context.
--           Supports analysis of review bombing, fake positive reviews from
--           flagged devices, unverified review patterns, and rating distortion.
-- Source  : ndc_product_reviews JOIN ndc_customer_devices
-- -----------------------------------------------------------------------------
CREATE TABLE fact_reviews (
    review_id           BIGSERIAL       PRIMARY KEY,
    product_key         INT             NOT NULL REFERENCES dim_product(product_id),
    customer_key        INT             NOT NULL REFERENCES dim_customer(customer_id),
    date_key            INT             NOT NULL REFERENCES dim_date(date_id),
    device_key          INT             REFERENCES dim_device(device_id),
    rating              INT             NOT NULL CHECK (rating BETWEEN 1 AND 5),
    review_source       VARCHAR(30)     NOT NULL DEFAULT 'organic',  -- organic, promo, incentivized
    is_verified         BOOLEAN         NOT NULL DEFAULT FALSE,
    is_flagged          BOOLEAN         NOT NULL DEFAULT FALSE
);

COMMENT ON TABLE fact_reviews IS 'Review fact table - each row is one product review for detecting fake reviews, review bombing, and rating manipulation fraud';

CREATE INDEX idx_fact_reviews_product_key ON fact_reviews (product_key);
CREATE INDEX idx_fact_reviews_customer_key ON fact_reviews (customer_key);
CREATE INDEX idx_fact_reviews_date_key ON fact_reviews (date_key);
CREATE INDEX idx_fact_reviews_device_key ON fact_reviews (device_key);
CREATE INDEX idx_fact_reviews_is_flagged ON fact_reviews (is_flagged);
CREATE INDEX idx_fact_reviews_rating ON fact_reviews (rating);
CREATE INDEX idx_fact_reviews_is_verified ON fact_reviews (is_verified);
CREATE INDEX idx_fact_reviews_product_flagged ON fact_reviews (product_key, is_flagged);

-- -----------------------------------------------------------------------------
-- fact_refunds
-- Purpose : Refund fact table for detecting refund fraud and abuse patterns.
--           Each row represents one refund record with dimensional context.
--           Key fraud indicators: refunds to different accounts, high-value
--           refund clustering, and frequent refunders. refund_category enables
--           segmentation of refund fraud types.
-- Source  : ndc_refund_records JOIN ndc_payments
-- -----------------------------------------------------------------------------
CREATE TABLE fact_refunds (
    refund_id                       BIGSERIAL       PRIMARY KEY,
    order_id                        INT             NOT NULL,
    customer_key                    INT             NOT NULL REFERENCES dim_customer(customer_id),
    date_key                        INT             NOT NULL REFERENCES dim_date(date_id),
    payment_key                     INT             NOT NULL REFERENCES dim_payment_method(payment_method_id),
    refund_amount                   DECIMAL(12,2)   NOT NULL,
    refund_category                 VARCHAR(50)     NOT NULL,       -- e.g., product_defect, not_received, buyer_remorse
    refund_reason                   TEXT,
    refund_to_different_account     BOOLEAN         NOT NULL DEFAULT FALSE,
    is_flagged                      BOOLEAN         NOT NULL DEFAULT FALSE
);

COMMENT ON TABLE fact_refunds IS 'Refund fact table - each row is one refund for detecting refund abuse, refund-to-different-account fraud, and serial refunder patterns';

CREATE INDEX idx_fact_refunds_order_id ON fact_refunds (order_id);
CREATE INDEX idx_fact_refunds_customer_key ON fact_refunds (customer_key);
CREATE INDEX idx_fact_refunds_date_key ON fact_refunds (date_key);
CREATE INDEX idx_fact_refunds_payment_key ON fact_refunds (payment_key);
CREATE INDEX idx_fact_refunds_is_flagged ON fact_refunds (is_flagged);
CREATE INDEX idx_fact_refunds_category ON fact_refunds (refund_category);
CREATE INDEX idx_fact_refunds_diff_account ON fact_refunds (refund_to_different_account);
CREATE INDEX idx_fact_refunds_date_flagged ON fact_refunds (date_key, is_flagged);
CREATE INDEX idx_fact_refunds_amount ON fact_refunds (refund_amount);

-- =============================================================================
-- AGGREGATE TABLE
-- =============================================================================

-- -----------------------------------------------------------------------------
-- agg_daily_fraud_summary
-- Purpose : Pre-computed daily aggregate for dashboard reporting and rapid
--           fraud trend analysis. Summarizes transaction counts, flagged
--           transactions, amounts, and fraud rates by date, city, and fraud
--           type. Enables fast OLAP drill-down without scanning fact tables.
-- Source  : Aggregated from fact_transactions (and optionally fact_reviews,
--           fact_refunds for combined fraud type reporting)
-- -----------------------------------------------------------------------------
CREATE TABLE agg_daily_fraud_summary (
    date_key                INT             NOT NULL REFERENCES dim_date(date_id),
    city                    VARCHAR(100),
    fraud_type              VARCHAR(50)     NOT NULL,   -- transaction_fraud, fake_review, refund_abuse
    total_transactions      INT             NOT NULL DEFAULT 0,
    flagged_transactions    INT             NOT NULL DEFAULT 0,
    total_amount            DECIMAL(15,2)   NOT NULL DEFAULT 0,
    fraud_amount            DECIMAL(15,2)   NOT NULL DEFAULT 0,
    fraud_rate              DECIMAL(5,4)    NOT NULL DEFAULT 0,  -- ratio: flagged / total
    PRIMARY KEY (date_key, city, fraud_type)
);

COMMENT ON TABLE agg_daily_fraud_summary IS 'Daily fraud aggregate - pre-computed summary for dashboard reporting with fraud rate by date, city, and fraud type';

CREATE INDEX idx_agg_fraud_date ON agg_daily_fraud_summary (date_key);
CREATE INDEX idx_agg_fraud_city ON agg_daily_fraud_summary (city);
CREATE INDEX idx_agg_fraud_type ON agg_daily_fraud_summary (fraud_type);
CREATE INDEX idx_agg_fraud_rate ON agg_daily_fraud_summary (fraud_rate);
CREATE INDEX idx_agg_fraud_date_type ON agg_daily_fraud_summary (date_key, fraud_type);

-- =============================================================================
-- END OF OLAP SCHEMA
-- =============================================================================
