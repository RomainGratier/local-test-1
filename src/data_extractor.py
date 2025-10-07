"""
Data Extraction Module for E-Commerce Analytics Pipeline
Handles extraction from multiple data sources with error handling and retry logic
"""

import json
import pandas as pd
import requests
import logging
from typing import Dict, List, Any, Optional, Union
from datetime import datetime, timedelta
import time
from pathlib import Path

from config import config, BUSINESS_RULES

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataExtractor:
    """Handles data extraction from multiple sources"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.timeout = config.data_sources.api_timeout
        
    def extract_transactions(self, file_path: str) -> List[Dict[str, Any]]:
        """
        Extract transaction data from JSON file
        
        Args:
            file_path: Path to the transactions JSON file
            
        Returns:
            List of transaction dictionaries
            
        Raises:
            FileNotFoundError: If file doesn't exist
            json.JSONDecodeError: If file contains invalid JSON
        """
        try:
            logger.info(f"Extracting transactions from {file_path}")
            
            with open(file_path, 'r') as f:
                transactions = json.load(f)
            
            # Validate transaction data
            validated_transactions = self._validate_transactions(transactions)
            
            logger.info(f"Successfully extracted {len(validated_transactions)} transactions")
            return validated_transactions
            
        except FileNotFoundError:
            logger.error(f"Transaction file not found: {file_path}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in transaction file: {e}")
            raise
        except Exception as e:
            logger.error(f"Error extracting transactions: {e}")
            raise
    
    def extract_users(self, file_path: str) -> List[Dict[str, Any]]:
        """
        Extract user data from CSV file
        
        Args:
            file_path: Path to the users CSV file
            
        Returns:
            List of user dictionaries
            
        Raises:
            FileNotFoundError: If file doesn't exist
            pd.errors.EmptyDataError: If file is empty
        """
        try:
            logger.info(f"Extracting users from {file_path}")
            
            df = pd.read_csv(file_path)
            users = df.to_dict('records')
            
            # Validate user data
            validated_users = self._validate_users(users)
            
            logger.info(f"Successfully extracted {len(validated_users)} users")
            return validated_users
            
        except FileNotFoundError:
            logger.error(f"User file not found: {file_path}")
            raise
        except pd.errors.EmptyDataError:
            logger.error(f"User file is empty: {file_path}")
            raise
        except Exception as e:
            logger.error(f"Error extracting users: {e}")
            raise
    
    def extract_products(self, file_path: str) -> List[Dict[str, Any]]:
        """
        Extract product data from JSON file
        
        Args:
            file_path: Path to the products JSON file
            
        Returns:
            List of product dictionaries
            
        Raises:
            FileNotFoundError: If file doesn't exist
            json.JSONDecodeError: If file contains invalid JSON
        """
        try:
            logger.info(f"Extracting products from {file_path}")
            
            with open(file_path, 'r') as f:
                products = json.load(f)
            
            # Validate product data
            validated_products = self._validate_products(products)
            
            logger.info(f"Successfully extracted {len(validated_products)} products")
            return validated_products
            
        except FileNotFoundError:
            logger.error(f"Product file not found: {file_path}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in product file: {e}")
            raise
        except Exception as e:
            logger.error(f"Error extracting products: {e}")
            raise
    
    def extract_products_from_api(self, api_url: str, max_retries: int = None) -> List[Dict[str, Any]]:
        """
        Extract product data from API
        
        Args:
            api_url: API endpoint URL
            max_retries: Maximum number of retry attempts
            
        Returns:
            List of product dictionaries
            
        Raises:
            requests.RequestException: If API request fails
        """
        max_retries = max_retries or config.pipeline.max_retries
        
        for attempt in range(max_retries + 1):
            try:
                logger.info(f"Extracting products from API (attempt {attempt + 1})")
                
                response = self.session.get(api_url)
                response.raise_for_status()
                
                products = response.json()
                
                # Validate product data
                validated_products = self._validate_products(products)
                
                logger.info(f"Successfully extracted {len(validated_products)} products from API")
                return validated_products
                
            except requests.RequestException as e:
                logger.warning(f"API request failed (attempt {attempt + 1}): {e}")
                
                if attempt < max_retries:
                    wait_time = config.pipeline.retry_delay * (2 ** attempt)  # Exponential backoff
                    logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"All API retry attempts failed: {e}")
                    raise
    
    def extract_all_sources(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Extract data from all configured sources
        
        Returns:
            Dictionary containing all extracted data
        """
        logger.info("Starting data extraction from all sources")
        
        extracted_data = {}
        
        try:
            # Extract transactions
            extracted_data['transactions'] = self.extract_transactions(
                config.data_sources.transactions_path
            )
            
            # Extract users
            extracted_data['users'] = self.extract_users(
                config.data_sources.users_path
            )
            
            # Extract products
            extracted_data['products'] = self.extract_products(
                config.data_sources.products_path
            )
            
            logger.info("Successfully extracted data from all sources")
            return extracted_data
            
        except Exception as e:
            logger.error(f"Error during data extraction: {e}")
            raise
    
    def _validate_transactions(self, transactions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Validate transaction data"""
        validated_transactions = []
        
        for txn in transactions:
            try:
                # Check required fields
                required_fields = ['transaction_id', 'user_id', 'product_id', 'amount', 'currency', 'timestamp']
                if not all(field in txn for field in required_fields):
                    logger.warning(f"Transaction missing required fields: {txn.get('transaction_id', 'unknown')}")
                    continue
                
                # Validate amount
                amount = float(txn['amount'])
                if amount < BUSINESS_RULES['min_transaction_amount'] or amount > BUSINESS_RULES['max_transaction_amount']:
                    logger.warning(f"Transaction amount out of range: {txn['transaction_id']}")
                    continue
                
                # Validate currency
                if txn['currency'] not in BUSINESS_RULES['valid_currencies']:
                    logger.warning(f"Invalid currency: {txn['transaction_id']}")
                    continue
                
                # Validate payment method
                if txn.get('payment_method') not in BUSINESS_RULES['valid_payment_methods']:
                    logger.warning(f"Invalid payment method: {txn['transaction_id']}")
                    continue
                
                # Validate status
                if txn.get('status') not in BUSINESS_RULES['valid_statuses']:
                    logger.warning(f"Invalid status: {txn['transaction_id']}")
                    continue
                
                # Validate timestamp
                try:
                    datetime.fromisoformat(txn['timestamp'].replace('Z', '+00:00'))
                except (ValueError, AttributeError):
                    logger.warning(f"Invalid timestamp format: {txn['transaction_id']}")
                    continue
                
                validated_transactions.append(txn)
                
            except (ValueError, TypeError) as e:
                logger.warning(f"Error validating transaction {txn.get('transaction_id', 'unknown')}: {e}")
                continue
        
        return validated_transactions
    
    def _validate_users(self, users: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Validate user data"""
        validated_users = []
        
        for user in users:
            try:
                # Check required fields
                required_fields = ['user_id', 'email', 'country']
                if not all(field in user for field in required_fields):
                    logger.warning(f"User missing required fields: {user.get('user_id', 'unknown')}")
                    continue
                
                # Validate email format
                if '@' not in user['email']:
                    logger.warning(f"Invalid email format: {user['user_id']}")
                    continue
                
                # Validate country code
                if len(user['country']) != 2:
                    logger.warning(f"Invalid country code: {user['user_id']}")
                    continue
                
                # Validate customer tier
                if 'customer_tier' in user and user['customer_tier'] not in ['standard', 'premium', 'vip']:
                    logger.warning(f"Invalid customer tier: {user['user_id']}")
                    continue
                
                validated_users.append(user)
                
            except Exception as e:
                logger.warning(f"Error validating user {user.get('user_id', 'unknown')}: {e}")
                continue
        
        return validated_users
    
    def _validate_products(self, products: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Validate product data"""
        validated_products = []
        
        for product in products:
            try:
                # Check required fields
                required_fields = ['product_id', 'name', 'price']
                if not all(field in product for field in required_fields):
                    logger.warning(f"Product missing required fields: {product.get('product_id', 'unknown')}")
                    continue
                
                # Validate price
                price = float(product['price'])
                if price <= 0:
                    logger.warning(f"Invalid price: {product['product_id']}")
                    continue
                
                # Validate inventory count
                if 'inventory_count' in product:
                    inventory = int(product['inventory_count'])
                    if inventory < 0:
                        logger.warning(f"Invalid inventory count: {product['product_id']}")
                        continue
                
                validated_products.append(product)
                
            except (ValueError, TypeError) as e:
                logger.warning(f"Error validating product {product.get('product_id', 'unknown')}: {e}")
                continue
        
        return validated_products

# Example usage
if __name__ == "__main__":
    extractor = DataExtractor()
    
    try:
        # Extract all data
        data = extractor.extract_all_sources()
        
        print(f"Extracted {len(data['transactions'])} transactions")
        print(f"Extracted {len(data['users'])} users")
        print(f"Extracted {len(data['products'])} products")
        
    except Exception as e:
        print(f"Error during extraction: {e}")