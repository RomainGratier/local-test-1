"""
Data Transformation Module for E-Commerce Analytics Pipeline
Handles data cleaning, enrichment, and business logic application
"""

import pandas as pd
import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
import uuid

from config import config, BUSINESS_RULES

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataTransformer:
    """Handles data transformation and business logic application"""
    
    def __init__(self):
        self.currency_rates = {
            'USD': 1.0,
            'EUR': 0.85,
            'GBP': 0.73,
            'CAD': 1.25
        }
    
    def transform_transactions(self, transactions: List[Dict[str, Any]], 
                            users: List[Dict[str, Any]], 
                            products: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transform transaction data with business logic
        
        Args:
            transactions: Raw transaction data
            users: User data for enrichment
            products: Product data for enrichment
            
        Returns:
            Transformed transaction data
        """
        logger.info("Starting transaction transformation")
        
        # Create lookup dictionaries
        user_lookup = {user['user_id']: user for user in users}
        product_lookup = {product['product_id']: product for product in products}
        
        transformed_transactions = []
        
        for txn in transactions:
            try:
                # Base transformation
                transformed_txn = self._transform_single_transaction(txn)
                
                # Enrich with user data
                user_data = user_lookup.get(txn['user_id'], {})
                transformed_txn.update({
                    'user_country': user_data.get('country'),
                    'user_tier': user_data.get('customer_tier', 'standard'),
                    'user_age_group': user_data.get('age_group'),
                    'user_registration_date': user_data.get('registration_date')
                })
                
                # Enrich with product data
                product_data = product_lookup.get(txn['product_id'], {})
                transformed_txn.update({
                    'product_name': product_data.get('name'),
                    'product_category': product_data.get('category'),
                    'product_supplier': product_data.get('supplier_id'),
                    'product_base_price': product_data.get('price')
                })
                
                # Apply business rules
                transformed_txn = self._apply_business_rules(transformed_txn)
                
                # Calculate derived fields
                transformed_txn = self._calculate_derived_fields(transformed_txn)
                
                transformed_transactions.append(transformed_txn)
                
            except Exception as e:
                logger.warning(f"Error transforming transaction {txn.get('transaction_id', 'unknown')}: {e}")
                continue
        
        logger.info(f"Successfully transformed {len(transformed_transactions)} transactions")
        return transformed_transactions
    
    def transform_users(self, users: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transform user data with enrichment
        
        Args:
            users: Raw user data
            
        Returns:
            Transformed user data
        """
        logger.info("Starting user transformation")
        
        transformed_users = []
        
        for user in users:
            try:
                # Base transformation
                transformed_user = self._transform_single_user(user)
                
                # Calculate derived fields
                transformed_user = self._calculate_user_derived_fields(transformed_user)
                
                transformed_users.append(transformed_user)
                
            except Exception as e:
                logger.warning(f"Error transforming user {user.get('user_id', 'unknown')}: {e}")
                continue
        
        logger.info(f"Successfully transformed {len(transformed_users)} users")
        return transformed_users
    
    def transform_products(self, products: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transform product data with enrichment
        
        Args:
            products: Raw product data
            
        Returns:
            Transformed product data
        """
        logger.info("Starting product transformation")
        
        transformed_products = []
        
        for product in products:
            try:
                # Base transformation
                transformed_product = self._transform_single_product(product)
                
                # Calculate derived fields
                transformed_product = self._calculate_product_derived_fields(transformed_product)
                
                transformed_products.append(transformed_product)
                
            except Exception as e:
                logger.warning(f"Error transforming product {product.get('product_id', 'unknown')}: {e}")
                continue
        
        logger.info(f"Successfully transformed {len(transformed_products)} products")
        return transformed_products
    
    def create_analytics_tables(self, transactions: List[Dict[str, Any]], 
                              users: List[Dict[str, Any]], 
                              products: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Create analytics tables for BigQuery
        
        Args:
            transactions: Transformed transaction data
            users: Transformed user data
            products: Transformed product data
            
        Returns:
            Dictionary containing analytics tables
        """
        logger.info("Creating analytics tables")
        
        analytics_tables = {}
        
        try:
            # Create daily sales summary
            analytics_tables['daily_sales_summary'] = self._create_daily_sales_summary(transactions)
            
            # Create user analytics
            analytics_tables['user_analytics'] = self._create_user_analytics(transactions, users)
            
            # Create product performance
            analytics_tables['product_performance'] = self._create_product_performance(transactions, products)
            
            # Create financial reports
            analytics_tables['financial_reports'] = self._create_financial_reports(transactions)
            
            logger.info("Successfully created analytics tables")
            return analytics_tables
            
        except Exception as e:
            logger.error(f"Error creating analytics tables: {e}")
            raise
    
    def _transform_single_transaction(self, txn: Dict[str, Any]) -> Dict[str, Any]:
        """Transform a single transaction"""
        return {
            'transaction_id': txn['transaction_id'],
            'user_id': txn['user_id'],
            'product_id': txn['product_id'],
            'amount': float(txn['amount']),
            'currency': txn['currency'],
            'payment_method': txn.get('payment_method', 'unknown'),
            'status': txn.get('status', 'pending'),
            'transaction_timestamp': txn['timestamp'],
            'amount_usd': self._convert_to_usd(float(txn['amount']), txn['currency']),
            'is_high_value': float(txn['amount']) > BUSINESS_RULES['high_value_threshold'],
            'is_international': txn['currency'] != 'USD'
        }
    
    def _transform_single_user(self, user: Dict[str, Any]) -> Dict[str, Any]:
        """Transform a single user"""
        return {
            'user_id': user['user_id'],
            'email': user['email'],
            'country': user['country'],
            'age_group': user.get('age_group'),
            'customer_tier': user.get('customer_tier', 'standard'),
            'registration_date': user.get('registration_date'),
            'is_active': user.get('is_active', True)
        }
    
    def _transform_single_product(self, product: Dict[str, Any]) -> Dict[str, Any]:
        """Transform a single product"""
        return {
            'product_id': product['product_id'],
            'name': product['name'],
            'category': product.get('category'),
            'price': float(product['price']),
            'currency': product.get('currency', 'USD'),
            'inventory_count': int(product.get('inventory_count', 0)),
            'supplier_id': product.get('supplier_id'),
            'price_usd': self._convert_to_usd(float(product['price']), product.get('currency', 'USD'))
        }
    
    def _apply_business_rules(self, txn: Dict[str, Any]) -> Dict[str, Any]:
        """Apply business rules to transaction"""
        # Rule 1: High-value transactions must use credit card or paypal
        if txn['is_high_value'] and txn['payment_method'] not in ['credit_card', 'paypal']:
            txn['payment_method_risk'] = 'high'
        else:
            txn['payment_method_risk'] = 'low'
        
        # Rule 2: International transactions have higher risk
        if txn['is_international']:
            txn['transaction_risk'] = 'medium'
        else:
            txn['transaction_risk'] = 'low'
        
        # Rule 3: VIP customers get priority processing
        if txn.get('user_tier') == 'vip':
            txn['processing_priority'] = 'high'
        elif txn.get('user_tier') == 'premium':
            txn['processing_priority'] = 'medium'
        else:
            txn['processing_priority'] = 'standard'
        
        return txn
    
    def _calculate_derived_fields(self, txn: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate derived fields for transaction"""
        # Calculate transaction date
        try:
            txn_date = datetime.fromisoformat(txn['transaction_timestamp'].replace('Z', '+00:00'))
            txn['transaction_date'] = txn_date.date()
            txn['transaction_hour'] = txn_date.hour
            txn['transaction_day_of_week'] = txn_date.strftime('%A')
        except (ValueError, AttributeError):
            txn['transaction_date'] = None
            txn['transaction_hour'] = None
            txn['transaction_day_of_week'] = None
        
        # Calculate profit margin estimate (simplified)
        base_price = txn.get('product_base_price', 0)
        if base_price > 0:
            txn['profit_margin_estimate'] = (txn['amount_usd'] - base_price) / txn['amount_usd']
        else:
            txn['profit_margin_estimate'] = 0.0
        
        return txn
    
    def _calculate_user_derived_fields(self, user: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate derived fields for user"""
        # Calculate customer lifetime (simplified)
        if user.get('registration_date'):
            try:
                reg_date = datetime.fromisoformat(user['registration_date'].replace('Z', '+00:00'))
                lifetime_days = (datetime.now() - reg_date).days
                user['customer_lifetime_days'] = lifetime_days
            except (ValueError, AttributeError):
                user['customer_lifetime_days'] = 0
        else:
            user['customer_lifetime_days'] = 0
        
        return user
    
    def _calculate_product_derived_fields(self, product: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate derived fields for product"""
        # Calculate inventory status
        inventory = product.get('inventory_count', 0)
        if inventory == 0:
            product['inventory_status'] = 'out_of_stock'
        elif inventory < 10:
            product['inventory_status'] = 'low_stock'
        else:
            product['inventory_status'] = 'in_stock'
        
        # Calculate price tier
        price = product.get('price_usd', 0)
        if price < 50:
            product['price_tier'] = 'budget'
        elif price < 200:
            product['price_tier'] = 'mid_range'
        else:
            product['price_tier'] = 'premium'
        
        return product
    
    def _convert_to_usd(self, amount: float, currency: str) -> float:
        """Convert amount to USD"""
        rate = self.currency_rates.get(currency, 1.0)
        return amount / rate
    
    def _create_daily_sales_summary(self, transactions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Create daily sales summary table"""
        # Group transactions by date
        daily_data = {}
        
        for txn in transactions:
            date = txn.get('transaction_date')
            if not date:
                continue
            
            if date not in daily_data:
                daily_data[date] = {
                    'date': date,
                    'total_revenue': 0,
                    'total_transactions': 0,
                    'unique_customers': set(),
                    'revenue_by_currency': {},
                    'revenue_by_payment_method': {}
                }
            
            daily_data[date]['total_revenue'] += txn['amount_usd']
            daily_data[date]['total_transactions'] += 1
            daily_data[date]['unique_customers'].add(txn['user_id'])
            
            # Currency breakdown
            currency = txn['currency']
            if currency not in daily_data[date]['revenue_by_currency']:
                daily_data[date]['revenue_by_currency'][currency] = 0
            daily_data[date]['revenue_by_currency'][currency] += txn['amount']
            
            # Payment method breakdown
            payment_method = txn['payment_method']
            if payment_method not in daily_data[date]['revenue_by_payment_method']:
                daily_data[date]['revenue_by_payment_method'][payment_method] = 0
            daily_data[date]['revenue_by_payment_method'][payment_method] += txn['amount_usd']
        
        # Convert to list format
        summary_list = []
        for date, data in daily_data.items():
            summary_list.append({
                'date': date,
                'total_revenue': data['total_revenue'],
                'total_transactions': data['total_transactions'],
                'unique_customers': len(data['unique_customers']),
                'average_order_value': data['total_revenue'] / data['total_transactions'] if data['total_transactions'] > 0 else 0,
                'revenue_by_currency': data['revenue_by_currency'],
                'revenue_by_payment_method': data['revenue_by_payment_method']
            })
        
        return summary_list
    
    def _create_user_analytics(self, transactions: List[Dict[str, Any]], users: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Create user analytics table"""
        # Group transactions by user
        user_data = {}
        
        for txn in transactions:
            user_id = txn['user_id']
            if user_id not in user_data:
                user_data[user_id] = {
                    'user_id': user_id,
                    'total_spent': 0,
                    'total_orders': 0,
                    'last_order_date': None,
                    'payment_methods': set(),
                    'product_categories': set()
                }
            
            user_data[user_id]['total_spent'] += txn['amount_usd']
            user_data[user_id]['total_orders'] += 1
            user_data[user_id]['payment_methods'].add(txn['payment_method'])
            user_data[user_id]['product_categories'].add(txn.get('product_category', 'unknown'))
            
            # Update last order date
            txn_date = txn.get('transaction_date')
            if txn_date and (not user_data[user_id]['last_order_date'] or txn_date > user_data[user_id]['last_order_date']):
                user_data[user_id]['last_order_date'] = txn_date
        
        # Enrich with user data
        user_lookup = {user['user_id']: user for user in users}
        
        analytics_list = []
        for user_id, data in user_data.items():
            user_info = user_lookup.get(user_id, {})
            
            analytics_list.append({
                'user_id': user_id,
                'email': user_info.get('email'),
                'country': user_info.get('country'),
                'customer_tier': user_info.get('customer_tier', 'standard'),
                'total_spent': data['total_spent'],
                'total_orders': data['total_orders'],
                'average_order_value': data['total_spent'] / data['total_orders'] if data['total_orders'] > 0 else 0,
                'last_order_date': data['last_order_date'],
                'preferred_payment_method': max(data['payment_methods'], key=list(data['payment_methods']).count) if data['payment_methods'] else None,
                'preferred_category': max(data['product_categories'], key=list(data['product_categories']).count) if data['product_categories'] else None,
                'is_high_value_customer': data['total_spent'] > 1000
            })
        
        return analytics_list
    
    def _create_product_performance(self, transactions: List[Dict[str, Any]], products: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Create product performance table"""
        # Group transactions by product
        product_data = {}
        
        for txn in transactions:
            product_id = txn['product_id']
            if product_id not in product_data:
                product_data[product_id] = {
                    'product_id': product_id,
                    'total_revenue': 0,
                    'total_orders': 0,
                    'unique_customers': set()
                }
            
            product_data[product_id]['total_revenue'] += txn['amount_usd']
            product_data[product_id]['total_orders'] += 1
            product_data[product_id]['unique_customers'].add(txn['user_id'])
        
        # Enrich with product data
        product_lookup = {product['product_id']: product for product in products}
        
        performance_list = []
        for product_id, data in product_data.items():
            product_info = product_lookup.get(product_id, {})
            
            performance_list.append({
                'product_id': product_id,
                'product_name': product_info.get('name'),
                'category': product_info.get('category'),
                'base_price': product_info.get('price_usd', 0),
                'total_revenue': data['total_revenue'],
                'total_orders': data['total_orders'],
                'unique_customers': len(data['unique_customers']),
                'average_order_value': data['total_revenue'] / data['total_orders'] if data['total_orders'] > 0 else 0,
                'inventory_count': product_info.get('inventory_count', 0),
                'performance_tier': 'high' if data['total_revenue'] > 5000 else 'medium' if data['total_revenue'] > 1000 else 'low'
            })
        
        return performance_list
    
    def _create_financial_reports(self, transactions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Create financial reports table"""
        # Calculate total revenue
        total_revenue = sum(txn['amount_usd'] for txn in transactions)
        total_transactions = len(transactions)
        
        # Calculate revenue by currency
        revenue_by_currency = {}
        for txn in transactions:
            currency = txn['currency']
            if currency not in revenue_by_currency:
                revenue_by_currency[currency] = 0
            revenue_by_currency[currency] += txn['amount']
        
        # Calculate revenue by payment method
        revenue_by_payment_method = {}
        for txn in transactions:
            payment_method = txn['payment_method']
            if payment_method not in revenue_by_payment_method:
                revenue_by_payment_method[payment_method] = 0
            revenue_by_payment_method[payment_method] += txn['amount_usd']
        
        return [{
            'report_date': datetime.now().date(),
            'report_type': 'daily',
            'period_start': datetime.now().date(),
            'period_end': datetime.now().date(),
            'total_revenue': total_revenue,
            'total_transactions': total_transactions,
            'average_order_value': total_revenue / total_transactions if total_transactions > 0 else 0,
            'revenue_by_currency': revenue_by_currency,
            'revenue_by_payment_method': revenue_by_payment_method
        }]

# Example usage
if __name__ == "__main__":
    transformer = DataTransformer()
    
    # This would be used with actual data
    print("Data transformer initialized successfully")