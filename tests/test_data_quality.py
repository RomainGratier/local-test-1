"""
Data Quality Test Suite for E-Commerce Analytics Pipeline
Tests data completeness, accuracy, consistency, and timeliness
"""

import pytest
import pandas as pd
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any

class DataQualityTests:
    """Comprehensive data quality testing framework"""
    
    def __init__(self):
        self.quality_thresholds = {
            'completeness': 0.95,  # 95% of required fields must be present
            'accuracy': 0.99,      # 99% accuracy for financial data
            'consistency': 0.98,   # 98% consistency across related fields
            'timeliness': 0.90     # 90% of data must arrive within SLA
        }
    
    def test_transaction_completeness(self, transactions: List[Dict]) -> Dict[str, Any]:
        """Test transaction data completeness"""
        required_fields = ['transaction_id', 'user_id', 'product_id', 'amount', 'currency', 'timestamp', 'payment_method', 'status']
        
        total_records = len(transactions)
        completeness_scores = {}
        
        for field in required_fields:
            non_null_count = sum(1 for txn in transactions if txn.get(field) is not None and txn.get(field) != '')
            completeness_scores[field] = non_null_count / total_records if total_records > 0 else 0
        
        overall_completeness = sum(completeness_scores.values()) / len(required_fields)
        
        return {
            'test_name': 'transaction_completeness',
            'overall_score': overall_completeness,
            'field_scores': completeness_scores,
            'passed': overall_completeness >= self.quality_thresholds['completeness'],
            'threshold': self.quality_thresholds['completeness']
        }
    
    def test_transaction_accuracy(self, transactions: List[Dict]) -> Dict[str, Any]:
        """Test transaction data accuracy"""
        accuracy_issues = []
        
        for txn in transactions:
            # Check amount is positive
            if txn.get('amount', 0) <= 0:
                accuracy_issues.append(f"Invalid amount: {txn.get('amount')}")
            
            # Check currency format
            currency = txn.get('currency', '')
            if len(currency) != 3 or not currency.isupper():
                accuracy_issues.append(f"Invalid currency format: {currency}")
            
            # Check payment method validity
            valid_payment_methods = ['credit_card', 'debit_card', 'paypal', 'apple_pay', 'google_pay']
            if txn.get('payment_method') not in valid_payment_methods:
                accuracy_issues.append(f"Invalid payment method: {txn.get('payment_method')}")
            
            # Check status validity
            valid_statuses = ['pending', 'completed', 'failed', 'refunded', 'cancelled']
            if txn.get('status') not in valid_statuses:
                accuracy_issues.append(f"Invalid status: {txn.get('status')}")
        
        total_records = len(transactions)
        accuracy_score = 1 - (len(accuracy_issues) / total_records) if total_records > 0 else 0
        
        return {
            'test_name': 'transaction_accuracy',
            'overall_score': accuracy_score,
            'issues': accuracy_issues,
            'passed': accuracy_score >= self.quality_thresholds['accuracy'],
            'threshold': self.quality_thresholds['accuracy']
        }
    
    def test_data_consistency(self, transactions: List[Dict], users: List[Dict], products: List[Dict]) -> Dict[str, Any]:
        """Test data consistency across related entities"""
        consistency_issues = []
        
        # Create lookup dictionaries
        user_ids = {user['user_id'] for user in users}
        product_ids = {product['product_id'] for product in products}
        
        # Check referential integrity
        for txn in transactions:
            if txn.get('user_id') not in user_ids:
                consistency_issues.append(f"Transaction {txn.get('transaction_id')} references non-existent user {txn.get('user_id')}")
            
            if txn.get('product_id') not in product_ids:
                consistency_issues.append(f"Transaction {txn.get('transaction_id')} references non-existent product {txn.get('product_id')}")
        
        # Check currency consistency with products
        product_currencies = {product['product_id']: product.get('currency', 'USD') for product in products}
        for txn in transactions:
            product_id = txn.get('product_id')
            if product_id in product_currencies:
                expected_currency = product_currencies[product_id]
                if txn.get('currency') != expected_currency:
                    consistency_issues.append(f"Currency mismatch for transaction {txn.get('transaction_id')}")
        
        total_checks = len(transactions) * 3  # user_id, product_id, currency
        consistency_score = 1 - (len(consistency_issues) / total_checks) if total_checks > 0 else 0
        
        return {
            'test_name': 'data_consistency',
            'overall_score': consistency_score,
            'issues': consistency_issues,
            'passed': consistency_score >= self.quality_thresholds['consistency'],
            'threshold': self.quality_thresholds['consistency']
        }
    
    def test_data_timeliness(self, transactions: List[Dict], sla_hours: int = 24) -> Dict[str, Any]:
        """Test data timeliness based on SLA requirements"""
        current_time = datetime.now()
        sla_cutoff = current_time - timedelta(hours=sla_hours)
        
        timely_records = 0
        total_records = len(transactions)
        
        for txn in transactions:
            try:
                txn_time = datetime.fromisoformat(txn.get('timestamp', '').replace('Z', '+00:00'))
                if txn_time >= sla_cutoff:
                    timely_records += 1
            except (ValueError, TypeError):
                # Invalid timestamp format
                pass
        
        timeliness_score = timely_records / total_records if total_records > 0 else 0
        
        return {
            'test_name': 'data_timeliness',
            'overall_score': timeliness_score,
            'timely_records': timely_records,
            'total_records': total_records,
            'passed': timeliness_score >= self.quality_thresholds['timeliness'],
            'threshold': self.quality_thresholds['timeliness']
        }
    
    def test_business_rules(self, transactions: List[Dict]) -> Dict[str, Any]:
        """Test business rule compliance"""
        business_rule_violations = []
        
        # Rule 1: Completed transactions must have positive amounts
        for txn in transactions:
            if txn.get('status') == 'completed' and txn.get('amount', 0) <= 0:
                business_rule_violations.append(f"Completed transaction {txn.get('transaction_id')} has non-positive amount")
        
        # Rule 2: Refunded transactions should have original transaction
        refunded_txns = [txn for txn in transactions if txn.get('status') == 'refunded']
        for txn in refunded_txns:
            # This is a simplified check - in reality, you'd check against original transaction
            if not txn.get('transaction_id'):
                business_rule_violations.append(f"Refunded transaction missing transaction_id")
        
        # Rule 3: High-value transactions (>$500) should be credit card or paypal
        high_value_txns = [txn for txn in transactions if txn.get('amount', 0) > 500]
        for txn in high_value_txns:
            if txn.get('payment_method') not in ['credit_card', 'paypal']:
                business_rule_violations.append(f"High-value transaction {txn.get('transaction_id')} uses non-preferred payment method")
        
        total_rules_checked = len(transactions) * 3  # Simplified count
        business_rule_score = 1 - (len(business_rule_violations) / total_rules_checked) if total_rules_checked > 0 else 0
        
        return {
            'test_name': 'business_rules',
            'overall_score': business_rule_score,
            'violations': business_rule_violations,
            'passed': business_rule_score >= 0.95,  # 95% compliance required
            'threshold': 0.95
        }
    
    def run_all_tests(self, transactions: List[Dict], users: List[Dict], products: List[Dict]) -> Dict[str, Any]:
        """Run all data quality tests and return comprehensive results"""
        test_results = {
            'completeness': self.test_transaction_completeness(transactions),
            'accuracy': self.test_transaction_accuracy(transactions),
            'consistency': self.test_data_consistency(transactions, users, products),
            'timeliness': self.test_data_timeliness(transactions),
            'business_rules': self.test_business_rules(transactions)
        }
        
        # Calculate overall data quality score
        overall_score = sum(result['overall_score'] for result in test_results.values()) / len(test_results)
        
        # Determine if all tests passed
        all_passed = all(result['passed'] for result in test_results.values())
        
        return {
            'overall_score': overall_score,
            'all_tests_passed': all_passed,
            'test_results': test_results,
            'summary': {
                'total_tests': len(test_results),
                'passed_tests': sum(1 for result in test_results.values() if result['passed']),
                'failed_tests': sum(1 for result in test_results.values() if not result['passed'])
            }
        }

# Example usage and test runner
def run_data_quality_tests():
    """Example function to run data quality tests"""
    # Load sample data
    with open('data/sample_transactions.json', 'r') as f:
        transactions = json.load(f)
    
    users_df = pd.read_csv('data/sample_users.csv')
    users = users_df.to_dict('records')
    
    with open('data/sample_products.json', 'r') as f:
        products = json.load(f)
    
    # Run tests
    quality_tester = DataQualityTests()
    results = quality_tester.run_all_tests(transactions, users, products)
    
    # Print results
    print("=== Data Quality Test Results ===")
    print(f"Overall Score: {results['overall_score']:.2%}")
    print(f"All Tests Passed: {results['all_tests_passed']}")
    print(f"Passed Tests: {results['summary']['passed_tests']}/{results['summary']['total_tests']}")
    
    for test_name, result in results['test_results'].items():
        status = "PASS" if result['passed'] else "FAIL"
        print(f"{test_name.upper()}: {status} ({result['overall_score']:.2%})")
    
    return results

if __name__ == "__main__":
    run_data_quality_tests()