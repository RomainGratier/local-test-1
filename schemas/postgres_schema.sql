-- PostgreSQL Schema for E-Commerce Analytics Pipeline
-- Transactional Layer - Normalized Design

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";

-- Users table - Core user information
CREATE TABLE users (
    user_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    email VARCHAR(255) UNIQUE NOT NULL,
    registration_date TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    country VARCHAR(2) NOT NULL CHECK (LENGTH(country) = 2),
    age_group VARCHAR(20) CHECK (age_group IN ('18-24', '25-34', '35-44', '45-54', '55-64', '65+')),
    customer_tier VARCHAR(20) DEFAULT 'standard' CHECK (customer_tier IN ('standard', 'premium', 'vip')),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Suppliers table
CREATE TABLE suppliers (
    supplier_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(255) NOT NULL,
    contact_email VARCHAR(255),
    country VARCHAR(2) NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Product categories
CREATE TABLE categories (
    category_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(100) NOT NULL UNIQUE,
    parent_category_id UUID REFERENCES categories(category_id),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Products table
CREATE TABLE products (
    product_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(255) NOT NULL,
    description TEXT,
    category_id UUID NOT NULL REFERENCES categories(category_id),
    supplier_id UUID NOT NULL REFERENCES suppliers(supplier_id),
    base_price DECIMAL(10,2) NOT NULL CHECK (base_price > 0),
    currency VARCHAR(3) NOT NULL DEFAULT 'USD' CHECK (LENGTH(currency) = 3),
    inventory_count INTEGER NOT NULL DEFAULT 0 CHECK (inventory_count >= 0),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Transactions table
CREATE TABLE transactions (
    transaction_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(user_id),
    product_id UUID NOT NULL REFERENCES products(product_id),
    amount DECIMAL(10,2) NOT NULL CHECK (amount > 0),
    currency VARCHAR(3) NOT NULL DEFAULT 'USD' CHECK (LENGTH(currency) = 3),
    payment_method VARCHAR(50) NOT NULL CHECK (payment_method IN ('credit_card', 'debit_card', 'paypal', 'apple_pay', 'google_pay')),
    status VARCHAR(20) NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'completed', 'failed', 'refunded', 'cancelled')),
    transaction_timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Audit logs table
CREATE TABLE audit_logs (
    log_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    table_name VARCHAR(100) NOT NULL,
    record_id UUID NOT NULL,
    operation VARCHAR(20) NOT NULL CHECK (operation IN ('INSERT', 'UPDATE', 'DELETE')),
    old_values JSONB,
    new_values JSONB,
    changed_by VARCHAR(100),
    changed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes for performance optimization
CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_users_country ON users(country);
CREATE INDEX idx_users_customer_tier ON users(customer_tier);
CREATE INDEX idx_users_registration_date ON users(registration_date);

CREATE INDEX idx_products_category ON products(category_id);
CREATE INDEX idx_products_supplier ON products(supplier_id);
CREATE INDEX idx_products_name ON products(name);
CREATE INDEX idx_products_is_active ON products(is_active);

CREATE INDEX idx_transactions_user_id ON transactions(user_id);
CREATE INDEX idx_transactions_product_id ON transactions(product_id);
CREATE INDEX idx_transactions_timestamp ON transactions(transaction_timestamp);
CREATE INDEX idx_transactions_status ON transactions(status);
CREATE INDEX idx_transactions_amount ON transactions(amount);

CREATE INDEX idx_audit_logs_table_record ON audit_logs(table_name, record_id);
CREATE INDEX idx_audit_logs_changed_at ON audit_logs(changed_at);

-- Triggers for audit logging
CREATE OR REPLACE FUNCTION audit_trigger_function()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        INSERT INTO audit_logs (table_name, record_id, operation, old_values, changed_by)
        VALUES (TG_TABLE_NAME, OLD.user_id, TG_OP, row_to_json(OLD), current_user);
        RETURN OLD;
    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO audit_logs (table_name, record_id, operation, old_values, new_values, changed_by)
        VALUES (TG_TABLE_NAME, NEW.user_id, TG_OP, row_to_json(OLD), row_to_json(NEW), current_user);
        RETURN NEW;
    ELSIF TG_OP = 'INSERT' THEN
        INSERT INTO audit_logs (table_name, record_id, operation, new_values, changed_by)
        VALUES (TG_TABLE_NAME, NEW.user_id, TG_OP, row_to_json(NEW), current_user);
        RETURN NEW;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Apply audit triggers
CREATE TRIGGER users_audit_trigger
    AFTER INSERT OR UPDATE OR DELETE ON users
    FOR EACH ROW EXECUTE FUNCTION audit_trigger_function();

CREATE TRIGGER products_audit_trigger
    AFTER INSERT OR UPDATE OR DELETE ON products
    FOR EACH ROW EXECUTE FUNCTION audit_trigger_function();

CREATE TRIGGER transactions_audit_trigger
    AFTER INSERT OR UPDATE OR DELETE ON transactions
    FOR EACH ROW EXECUTE FUNCTION audit_trigger_function();

-- Function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Apply updated_at triggers
CREATE TRIGGER update_users_updated_at
    BEFORE UPDATE ON users
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_products_updated_at
    BEFORE UPDATE ON products
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Sample data insertion functions
CREATE OR REPLACE FUNCTION insert_sample_data()
RETURNS VOID AS $$
BEGIN
    -- Insert sample categories
    INSERT INTO categories (name) VALUES 
        ('Electronics'), ('Clothing'), ('Books'), ('Home & Garden'), ('Sports');
    
    -- Insert sample suppliers
    INSERT INTO suppliers (name, contact_email, country) VALUES 
        ('TechSupply Inc', 'contact@techsupply.com', 'US'),
        ('Global Electronics', 'info@globalelectronics.com', 'CN'),
        ('Fashion Forward', 'hello@fashionforward.com', 'IT'),
        ('BookWorld', 'orders@bookworld.com', 'GB'),
        ('Home Essentials', 'support@homeessentials.com', 'DE');
    
    -- Insert sample users
    INSERT INTO users (email, country, age_group, customer_tier) VALUES 
        ('john.doe@email.com', 'US', '25-34', 'premium'),
        ('jane.smith@email.com', 'CA', '35-44', 'standard'),
        ('mike.wilson@email.com', 'GB', '18-24', 'vip'),
        ('sarah.jones@email.com', 'AU', '45-54', 'standard'),
        ('david.brown@email.com', 'DE', '25-34', 'premium');
    
    -- Insert sample products
    INSERT INTO products (name, description, category_id, supplier_id, base_price, currency, inventory_count) 
    SELECT 
        p.name,
        p.description,
        c.category_id,
        s.supplier_id,
        p.base_price,
        'USD',
        p.inventory_count
    FROM (VALUES 
        ('iPhone 15 Pro', 'Latest Apple smartphone', 1, 1, 999.99, 100),
        ('Samsung Galaxy S24', 'Android flagship phone', 1, 2, 899.99, 150),
        ('Nike Air Max', 'Comfortable running shoes', 2, 3, 129.99, 200),
        ('Python Programming Book', 'Learn Python from scratch', 3, 4, 49.99, 50),
        ('Coffee Maker', 'Automatic drip coffee maker', 4, 5, 79.99, 75)
    ) AS p(name, description, category_id, supplier_id, base_price, inventory_count)
    CROSS JOIN categories c
    CROSS JOIN suppliers s
    WHERE c.category_id = p.category_id AND s.supplier_id = p.supplier_id;
    
END;
$$ LANGUAGE plpgsql;