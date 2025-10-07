"""
Configuration module for E-Commerce Analytics Pipeline
Contains all configuration settings and environment variables
"""

import os
from typing import Dict, Any
from dataclasses import dataclass

@dataclass
class DatabaseConfig:
    """PostgreSQL database configuration"""
    host: str = os.getenv('POSTGRES_HOST', 'localhost')
    port: int = int(os.getenv('POSTGRES_PORT', '5432'))
    database: str = os.getenv('POSTGRES_DB', 'techmart')
    username: str = os.getenv('POSTGRES_USER', 'postgres')
    password: str = os.getenv('POSTGRES_PASSWORD', 'password')
    
    @property
    def connection_string(self) -> str:
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"

@dataclass
class BigQueryConfig:
    """BigQuery configuration"""
    project_id: str = os.getenv('BIGQUERY_PROJECT_ID', 'techmart-analytics')
    dataset_id: str = os.getenv('BIGQUERY_DATASET_ID', 'techmart_analytics')
    credentials_path: str = os.getenv('BIGQUERY_CREDENTIALS_PATH', '')
    
    @property
    def dataset_reference(self) -> str:
        return f"{self.project_id}.{self.dataset_id}"

@dataclass
class PipelineConfig:
    """Pipeline configuration"""
    batch_size: int = int(os.getenv('BATCH_SIZE', '1000'))
    max_retries: int = int(os.getenv('MAX_RETRIES', '3'))
    retry_delay: int = int(os.getenv('RETRY_DELAY', '5'))
    data_quality_threshold: float = float(os.getenv('DATA_QUALITY_THRESHOLD', '0.95'))
    enable_monitoring: bool = os.getenv('ENABLE_MONITORING', 'true').lower() == 'true'
    log_level: str = os.getenv('LOG_LEVEL', 'INFO')

@dataclass
class DataSourceConfig:
    """Data source configuration"""
    transactions_path: str = os.getenv('TRANSACTIONS_PATH', 'data/sample_transactions.json')
    users_path: str = os.getenv('USERS_PATH', 'data/sample_users.csv')
    products_path: str = os.getenv('PRODUCTS_PATH', 'data/sample_products.json')
    api_base_url: str = os.getenv('API_BASE_URL', 'https://api.techmart.com')
    api_timeout: int = int(os.getenv('API_TIMEOUT', '30'))

class Config:
    """Main configuration class"""
    
    def __init__(self):
        self.database = DatabaseConfig()
        self.bigquery = BigQueryConfig()
        self.pipeline = PipelineConfig()
        self.data_sources = DataSourceConfig()
    
    def validate(self) -> bool:
        """Validate configuration"""
        required_env_vars = [
            'POSTGRES_HOST',
            'POSTGRES_DB',
            'POSTGRES_USER',
            'POSTGRES_PASSWORD',
            'BIGQUERY_PROJECT_ID'
        ]
        
        missing_vars = [var for var in required_env_vars if not os.getenv(var)]
        
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
        
        return True
    
    def get_database_config(self) -> Dict[str, Any]:
        """Get database configuration as dictionary"""
        return {
            'host': self.database.host,
            'port': self.database.port,
            'database': self.database.database,
            'username': self.database.username,
            'password': self.database.password
        }
    
    def get_bigquery_config(self) -> Dict[str, Any]:
        """Get BigQuery configuration as dictionary"""
        return {
            'project_id': self.bigquery.project_id,
            'dataset_id': self.bigquery.dataset_id,
            'credentials_path': self.bigquery.credentials_path
        }

# Global configuration instance
config = Config()

# Data quality thresholds
DATA_QUALITY_THRESHOLDS = {
    'completeness': 0.95,
    'accuracy': 0.99,
    'consistency': 0.98,
    'timeliness': 0.90
}

# Performance thresholds
PERFORMANCE_THRESHOLDS = {
    'transaction_processing': 1000,  # records per minute
    'data_loading': 10,             # MB per minute
    'query_response': 10,           # seconds max
    'memory_usage': 80,             # % max memory usage
    'cpu_usage': 90                 # % max CPU usage
}

# Business rules
BUSINESS_RULES = {
    'min_transaction_amount': 0.01,
    'max_transaction_amount': 100000.00,
    'valid_currencies': ['USD', 'EUR', 'GBP', 'CAD'],
    'valid_payment_methods': ['credit_card', 'debit_card', 'paypal', 'apple_pay', 'google_pay'],
    'valid_statuses': ['pending', 'completed', 'failed', 'refunded', 'cancelled'],
    'high_value_threshold': 500.00
}

# Table schemas
POSTGRES_TABLES = {
    'users': 'users',
    'products': 'products',
    'transactions': 'transactions',
    'suppliers': 'suppliers',
    'categories': 'categories',
    'audit_logs': 'audit_logs'
}

BIGQUERY_TABLES = {
    'daily_sales_summary': 'daily_sales_summary',
    'user_analytics': 'user_analytics',
    'product_performance': 'product_performance',
    'financial_reports': 'financial_reports',
    'data_quality_metrics': 'data_quality_metrics',
    'pipeline_monitoring': 'pipeline_monitoring',
    'transaction_stream': 'transaction_stream'
}