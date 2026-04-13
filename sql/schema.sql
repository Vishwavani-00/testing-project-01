-- ============================================================
-- schema.sql — PostgreSQL Schema for Superstore Sales Pipeline
-- PRD: etl_project_requirements.md §6.2–6.4
-- ============================================================

-- ============================================================
-- STAGING LAYER
-- ============================================================

DROP TABLE IF EXISTS stg_superstore CASCADE;

CREATE TABLE stg_superstore (
    order_id        TEXT,
    order_date      DATE,
    ship_date       DATE,
    ship_mode       TEXT,
    customer_id     TEXT,
    customer_name   TEXT,
    segment         TEXT,
    country         TEXT,
    city            TEXT,
    state           TEXT,
    region          TEXT,
    product_id      TEXT,
    category        TEXT,
    sub_category    TEXT,
    product_name    TEXT,
    sales           FLOAT,
    quantity        INT,
    discount        FLOAT,
    profit          FLOAT,
    loaded_at       TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_stg_order_id ON stg_superstore (order_id);
CREATE INDEX idx_stg_customer_id ON stg_superstore (customer_id);
CREATE INDEX idx_stg_order_date ON stg_superstore (order_date);

-- ============================================================
-- DIMENSION TABLES
-- ============================================================

DROP TABLE IF EXISTS dim_customer CASCADE;

CREATE TABLE dim_customer (
    customer_sk     SERIAL PRIMARY KEY,
    customer_id     TEXT UNIQUE NOT NULL,
    customer_name   TEXT,
    segment         TEXT,
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_dim_customer_id ON dim_customer (customer_id);

-- ------------------------------------------------------------

DROP TABLE IF EXISTS dim_product CASCADE;

CREATE TABLE dim_product (
    product_sk      SERIAL PRIMARY KEY,
    product_id      TEXT UNIQUE NOT NULL,
    product_name    TEXT,
    category        TEXT,
    sub_category    TEXT,
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_dim_product_id ON dim_product (product_id);
CREATE INDEX idx_dim_category ON dim_product (category);

-- ------------------------------------------------------------

DROP TABLE IF EXISTS dim_region CASCADE;

CREATE TABLE dim_region (
    region_sk       SERIAL PRIMARY KEY,
    region          TEXT UNIQUE NOT NULL,
    state           TEXT,
    city            TEXT,
    country         TEXT,
    created_at      TIMESTAMP DEFAULT NOW()
);

-- ------------------------------------------------------------

DROP TABLE IF EXISTS dim_date CASCADE;

CREATE TABLE dim_date (
    date_sk         SERIAL PRIMARY KEY,
    order_date      DATE UNIQUE NOT NULL,
    day             INT,
    month           INT,
    year            INT,
    quarter         INT,
    day_of_week     TEXT
);

CREATE INDEX idx_dim_date ON dim_date (order_date);
CREATE INDEX idx_dim_year_month ON dim_date (year, month);

-- ============================================================
-- FACT TABLE
-- ============================================================

DROP TABLE IF EXISTS fact_sales CASCADE;

CREATE TABLE fact_sales (
    fact_id         SERIAL PRIMARY KEY,
    order_id        TEXT NOT NULL,
    customer_id     TEXT REFERENCES dim_customer (customer_id),
    product_id      TEXT REFERENCES dim_product (product_id),
    order_date      DATE REFERENCES dim_date (order_date),
    sales           FLOAT CHECK (sales >= 0),
    quantity        INT,
    profit          FLOAT,
    discount        FLOAT DEFAULT 0,
    order_month     INT,
    order_year      INT,
    profit_margin   FLOAT,
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_fact_order_id ON fact_sales (order_id);
CREATE INDEX idx_fact_customer ON fact_sales (customer_id);
CREATE INDEX idx_fact_product ON fact_sales (product_id);
CREATE INDEX idx_fact_date ON fact_sales (order_date);
CREATE INDEX idx_fact_year_month ON fact_sales (order_year, order_month);
CREATE INDEX idx_fact_region ON fact_sales (order_date);

-- ============================================================
-- AGGREGATED TABLE
-- ============================================================

DROP TABLE IF EXISTS agg_sales_summary CASCADE;

CREATE TABLE agg_sales_summary (
    agg_id          SERIAL PRIMARY KEY,
    order_date      DATE,
    region          TEXT,
    category        TEXT,
    total_sales     FLOAT,
    total_profit    FLOAT,
    total_orders    INT,
    avg_discount    FLOAT,
    refreshed_at    TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_agg_date ON agg_sales_summary (order_date);
CREATE INDEX idx_agg_region ON agg_sales_summary (region);
CREATE INDEX idx_agg_category ON agg_sales_summary (category);

-- ============================================================
-- DQ AUDIT LOG
-- ============================================================

DROP TABLE IF EXISTS dq_audit_log CASCADE;

CREATE TABLE dq_audit_log (
    audit_id        SERIAL PRIMARY KEY,
    run_date        TIMESTAMP DEFAULT NOW(),
    check_name      TEXT,
    passed          BOOLEAN,
    detail          TEXT,
    dq_score_pct    FLOAT
);

-- ============================================================
-- USEFUL ANALYTICAL VIEWS
-- ============================================================

-- Monthly Revenue Trend
CREATE OR REPLACE VIEW v_monthly_revenue AS
SELECT
    order_year,
    order_month,
    ROUND(SUM(sales)::NUMERIC, 2)  AS total_sales,
    ROUND(SUM(profit)::NUMERIC, 2) AS total_profit,
    COUNT(DISTINCT order_id)        AS total_orders
FROM fact_sales
GROUP BY order_year, order_month
ORDER BY order_year, order_month;

-- Top 10 Products by Sales
CREATE OR REPLACE VIEW v_top_products AS
SELECT
    p.product_name,
    p.category,
    ROUND(SUM(f.sales)::NUMERIC, 2) AS total_sales,
    ROUND(SUM(f.profit)::NUMERIC, 2) AS total_profit
FROM fact_sales f
JOIN dim_product p ON f.product_id = p.product_id
GROUP BY p.product_name, p.category
ORDER BY total_sales DESC
LIMIT 10;

-- Region-wise Performance
CREATE OR REPLACE VIEW v_region_performance AS
SELECT
    r.region,
    ROUND(SUM(f.sales)::NUMERIC, 2)  AS total_sales,
    ROUND(SUM(f.profit)::NUMERIC, 2) AS total_profit,
    COUNT(DISTINCT f.order_id)         AS total_orders,
    ROUND(AVG(f.discount)::NUMERIC, 4) AS avg_discount
FROM fact_sales f
JOIN dim_region r ON r.region = r.region
GROUP BY r.region
ORDER BY total_sales DESC;
