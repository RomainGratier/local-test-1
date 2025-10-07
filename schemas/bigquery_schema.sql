-- BigQuery Schema for E-Commerce Analytics Pipeline
-- Analytics Layer - Denormalized Design for Performance

-- Daily Sales Summary Table
CREATE TABLE `techmart_analytics.daily_sales_summary` (
    date DATE NOT NULL,
    total_revenue DECIMAL(12,2) NOT NULL,
    total_transactions INTEGER NOT NULL,
    unique_customers INTEGER NOT NULL,
    average_order_value DECIMAL(10,2) NOT NULL,
    revenue_by_currency STRUCT<
        usd DECIMAL(12,2),
        eur DECIMAL(12,2),
        gbp DECIMAL(12,2),
        cad DECIMAL(12,2)
    >,
    revenue_by_payment_method STRUCT<
        credit_card DECIMAL(12,2),
        debit_card DECIMAL(12,2),
        paypal DECIMAL(12,2),
        apple_pay DECIMAL(12,2),
        google_pay DECIMAL(12,2)
    >,
    top_products ARRAY<STRUCT<
        product_id STRING,
        product_name STRING,
        revenue DECIMAL(10,2),
        transaction_count INTEGER
    >>,
    data_quality_score FLOAT64,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY date
CLUSTER BY revenue_by_currency.usd;

-- User Analytics Table
CREATE TABLE `techmart_analytics.user_analytics` (
    user_id STRING NOT NULL,
    email STRING,
    registration_date DATE,
    country STRING,
    age_group STRING,
    customer_tier STRING,
    total_spent DECIMAL(12,2),
    total_orders INTEGER,
    average_order_value DECIMAL(10,2),
    last_order_date DATE,
    days_since_last_order INTEGER,
    customer_lifetime_value DECIMAL(12,2),
    preferred_payment_method STRING,
    preferred_product_category STRING,
    is_high_value_customer BOOLEAN,
    churn_risk_score FLOAT64,
    data_quality_score FLOAT64,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY registration_date
CLUSTER BY customer_tier, country;

-- Product Performance Table
CREATE TABLE `techmart_analytics.product_performance` (
    product_id STRING NOT NULL,
    product_name STRING,
    category STRING,
    supplier_name STRING,
    base_price DECIMAL(10,2),
    currency STRING,
    total_revenue DECIMAL(12,2),
    total_orders INTEGER,
    unique_customers INTEGER,
    average_order_value DECIMAL(10,2),
    inventory_turnover_ratio FLOAT64,
    days_in_inventory INTEGER,
    revenue_rank_in_category INTEGER,
    profit_margin_estimate FLOAT64,
    seasonal_trend STRING,
    performance_tier STRING,
    data_quality_score FLOAT64,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(last_updated)
CLUSTER BY category, performance_tier;

-- Financial Reports Table
CREATE TABLE `techmart_analytics.financial_reports` (
    report_date DATE NOT NULL,
    report_type STRING NOT NULL, -- 'daily', 'weekly', 'monthly', 'quarterly', 'yearly'
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    total_revenue DECIMAL(15,2) NOT NULL,
    total_cost_of_goods_sold DECIMAL(15,2),
    gross_profit DECIMAL(15,2),
    gross_profit_margin FLOAT64,
    operating_expenses DECIMAL(15,2),
    net_profit DECIMAL(15,2),
    net_profit_margin FLOAT64,
    revenue_by_region STRUCT<
        north_america DECIMAL(12,2),
        europe DECIMAL(12,2),
        asia_pacific DECIMAL(12,2),
        other DECIMAL(12,2)
    >,
    revenue_by_customer_tier STRUCT<
        standard DECIMAL(12,2),
        premium DECIMAL(12,2),
        vip DECIMAL(12,2)
    >,
    payment_method_distribution STRUCT<
        credit_card FLOAT64,
        debit_card FLOAT64,
        paypal FLOAT64,
        apple_pay FLOAT64,
        google_pay FLOAT64
    >,
    currency_breakdown STRUCT<
        usd DECIMAL(12,2),
        eur DECIMAL(12,2),
        gbp DECIMAL(12,2),
        cad DECIMAL(12,2)
    >,
    data_quality_score FLOAT64,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY report_date
CLUSTER BY report_type;

-- Data Quality Metrics Table
CREATE TABLE `techmart_analytics.data_quality_metrics` (
    table_name STRING NOT NULL,
    check_date DATE NOT NULL,
    check_type STRING NOT NULL, -- 'completeness', 'accuracy', 'consistency', 'timeliness'
    metric_name STRING NOT NULL,
    metric_value FLOAT64 NOT NULL,
    threshold_value FLOAT64,
    status STRING NOT NULL, -- 'PASS', 'WARN', 'FAIL'
    details STRING,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY check_date
CLUSTER BY table_name, check_type;

-- Pipeline Monitoring Table
CREATE TABLE `techmart_analytics.pipeline_monitoring` (
    pipeline_name STRING NOT NULL,
    run_id STRING NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    status STRING NOT NULL, -- 'RUNNING', 'SUCCESS', 'FAILED', 'CANCELLED'
    records_processed INTEGER,
    records_failed INTEGER,
    processing_time_seconds INTEGER,
    error_message STRING,
    data_quality_score FLOAT64,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(start_time)
CLUSTER BY pipeline_name, status;

-- Real-time Transaction Stream (for streaming analytics)
CREATE TABLE `techmart_analytics.transaction_stream` (
    transaction_id STRING NOT NULL,
    user_id STRING NOT NULL,
    product_id STRING NOT NULL,
    amount DECIMAL(10,2) NOT NULL,
    currency STRING NOT NULL,
    payment_method STRING NOT NULL,
    status STRING NOT NULL,
    transaction_timestamp TIMESTAMP NOT NULL,
    processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    -- Denormalized fields for faster queries
    user_country STRING,
    user_tier STRING,
    product_category STRING,
    product_name STRING,
    supplier_name STRING
)
PARTITION BY DATE(transaction_timestamp)
CLUSTER BY user_country, product_category, status;

-- Views for common analytical queries
CREATE VIEW `techmart_analytics.daily_kpis` AS
SELECT 
    date,
    total_revenue,
    total_transactions,
    unique_customers,
    average_order_value,
    revenue_by_currency.usd as usd_revenue,
    revenue_by_currency.eur as eur_revenue,
    revenue_by_currency.gbp as gbp_revenue,
    revenue_by_currency.cad as cad_revenue,
    ROUND(total_revenue / NULLIF(total_transactions, 0), 2) as calculated_aov,
    data_quality_score
FROM `techmart_analytics.daily_sales_summary`
WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
ORDER BY date DESC;

CREATE VIEW `techmart_analytics.top_customers` AS
SELECT 
    user_id,
    email,
    country,
    customer_tier,
    total_spent,
    total_orders,
    customer_lifetime_value,
    RANK() OVER (ORDER BY total_spent DESC) as revenue_rank,
    RANK() OVER (ORDER BY total_orders DESC) as order_rank
FROM `techmart_analytics.user_analytics`
WHERE total_spent > 0
ORDER BY total_spent DESC;

CREATE VIEW `techmart_analytics.product_rankings` AS
SELECT 
    product_id,
    product_name,
    category,
    total_revenue,
    total_orders,
    RANK() OVER (PARTITION BY category ORDER BY total_revenue DESC) as category_rank,
    RANK() OVER (ORDER BY total_revenue DESC) as overall_rank,
    performance_tier
FROM `techmart_analytics.product_performance`
WHERE total_revenue > 0
ORDER BY total_revenue DESC;