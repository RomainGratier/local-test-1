"""
Performance Test Suite for E-Commerce Analytics Pipeline
Tests processing speed, throughput, and query performance
"""

import time
import psutil
import pandas as pd
from typing import Dict, List, Any, Callable
from datetime import datetime, timedelta
import json

class PerformanceTests:
    """Performance testing framework for data pipeline"""
    
    def __init__(self):
        self.performance_thresholds = {
            'transaction_processing': 1000,  # records per minute
            'data_loading': 10,             # MB per minute
            'query_response': 10,           # seconds max
            'memory_usage': 80,             # % max memory usage
            'cpu_usage': 90                 # % max CPU usage
        }
    
    def measure_execution_time(self, func: Callable, *args, **kwargs) -> Dict[str, Any]:
        """Measure execution time of a function"""
        start_time = time.time()
        start_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
        start_cpu = psutil.cpu_percent()
        
        try:
            result = func(*args, **kwargs)
            success = True
            error = None
        except Exception as e:
            result = None
            success = False
            error = str(e)
        
        end_time = time.time()
        end_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
        end_cpu = psutil.cpu_percent()
        
        execution_time = end_time - start_time
        memory_used = end_memory - start_memory
        cpu_usage = end_cpu
        
        return {
            'execution_time': execution_time,
            'memory_used': memory_used,
            'cpu_usage': cpu_usage,
            'success': success,
            'error': error,
            'result': result
        }
    
    def test_transaction_processing_speed(self, process_transactions_func: Callable, transactions: List[Dict]) -> Dict[str, Any]:
        """Test transaction processing speed"""
        def process_batch():
            return process_transactions_func(transactions)
        
        metrics = self.measure_execution_time(process_batch)
        
        if metrics['success']:
            records_per_second = len(transactions) / metrics['execution_time']
            records_per_minute = records_per_second * 60
            
            passed = records_per_minute >= self.performance_thresholds['transaction_processing']
        else:
            records_per_minute = 0
            passed = False
        
        return {
            'test_name': 'transaction_processing_speed',
            'records_processed': len(transactions),
            'execution_time': metrics['execution_time'],
            'records_per_minute': records_per_minute,
            'memory_used': metrics['memory_used'],
            'cpu_usage': metrics['cpu_usage'],
            'passed': passed,
            'threshold': self.performance_thresholds['transaction_processing'],
            'success': metrics['success'],
            'error': metrics['error']
        }
    
    def test_data_loading_performance(self, load_data_func: Callable, data_size_mb: float) -> Dict[str, Any]:
        """Test data loading performance to BigQuery/PostgreSQL"""
        def load_data():
            return load_data_func()
        
        metrics = self.measure_execution_time(load_data)
        
        if metrics['success']:
            mb_per_second = data_size_mb / metrics['execution_time']
            mb_per_minute = mb_per_second * 60
            
            passed = mb_per_minute >= self.performance_thresholds['data_loading']
        else:
            mb_per_minute = 0
            passed = False
        
        return {
            'test_name': 'data_loading_performance',
            'data_size_mb': data_size_mb,
            'execution_time': metrics['execution_time'],
            'mb_per_minute': mb_per_minute,
            'memory_used': metrics['memory_used'],
            'cpu_usage': metrics['cpu_usage'],
            'passed': passed,
            'threshold': self.performance_thresholds['data_loading'],
            'success': metrics['success'],
            'error': metrics['error']
        }
    
    def test_query_performance(self, query_func: Callable, query_name: str) -> Dict[str, Any]:
        """Test query performance"""
        metrics = self.measure_execution_time(query_func)
        
        if metrics['success']:
            passed = metrics['execution_time'] <= self.performance_thresholds['query_response']
        else:
            passed = False
        
        return {
            'test_name': f'query_performance_{query_name}',
            'query_name': query_name,
            'execution_time': metrics['execution_time'],
            'memory_used': metrics['memory_used'],
            'cpu_usage': metrics['cpu_usage'],
            'passed': passed,
            'threshold': self.performance_thresholds['query_response'],
            'success': metrics['success'],
            'error': metrics['error']
        }
    
    def test_memory_usage(self, process_func: Callable, *args, **kwargs) -> Dict[str, Any]:
        """Test memory usage during processing"""
        initial_memory = psutil.virtual_memory().percent
        
        metrics = self.measure_execution_time(process_func, *args, **kwargs)
        
        peak_memory = psutil.virtual_memory().percent
        memory_increase = peak_memory - initial_memory
        
        passed = peak_memory <= self.performance_thresholds['memory_usage']
        
        return {
            'test_name': 'memory_usage',
            'initial_memory_percent': initial_memory,
            'peak_memory_percent': peak_memory,
            'memory_increase': memory_increase,
            'passed': passed,
            'threshold': self.performance_thresholds['memory_usage'],
            'success': metrics['success'],
            'error': metrics['error']
        }
    
    def test_concurrent_processing(self, process_func: Callable, data_batches: List[List[Dict]], max_workers: int = 5) -> Dict[str, Any]:
        """Test concurrent processing performance"""
        import concurrent.futures
        
        def process_batch(batch):
            return process_func(batch)
        
        start_time = time.time()
        
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [executor.submit(process_batch, batch) for batch in data_batches]
                results = [future.result() for future in concurrent.futures.as_completed(futures)]
            
            execution_time = time.time() - start_time
            success = True
            error = None
        except Exception as e:
            execution_time = time.time() - start_time
            results = []
            success = False
            error = str(e)
        
        total_records = sum(len(batch) for batch in data_batches)
        records_per_second = total_records / execution_time if execution_time > 0 else 0
        
        return {
            'test_name': 'concurrent_processing',
            'total_records': total_records,
            'execution_time': execution_time,
            'records_per_second': records_per_second,
            'max_workers': max_workers,
            'success': success,
            'error': error
        }
    
    def test_scalability(self, process_func: Callable, data_sizes: List[int]) -> Dict[str, Any]:
        """Test scalability with different data sizes"""
        scalability_results = []
        
        for size in data_sizes:
            # Generate test data of specified size
            test_data = [{'id': i, 'value': f'data_{i}'} for i in range(size)]
            
            metrics = self.measure_execution_time(process_func, test_data)
            
            if metrics['success']:
                records_per_second = size / metrics['execution_time']
                scalability_results.append({
                    'data_size': size,
                    'execution_time': metrics['execution_time'],
                    'records_per_second': records_per_second,
                    'memory_used': metrics['memory_used']
                })
        
        # Calculate scalability metrics
        if len(scalability_results) >= 2:
            # Check if performance degrades significantly with larger datasets
            small_dataset = scalability_results[0]
            large_dataset = scalability_results[-1]
            
            performance_ratio = (large_dataset['records_per_second'] / small_dataset['records_per_second']) * (small_dataset['data_size'] / large_dataset['data_size'])
            scalable = performance_ratio >= 0.8  # Should maintain at least 80% efficiency
        else:
            scalable = True
        
        return {
            'test_name': 'scalability',
            'scalable': scalable,
            'results': scalability_results,
            'performance_ratio': performance_ratio if len(scalability_results) >= 2 else 1.0
        }
    
    def run_performance_benchmark(self, pipeline_functions: Dict[str, Callable], test_data: Dict[str, Any]) -> Dict[str, Any]:
        """Run comprehensive performance benchmark"""
        benchmark_results = {}
        
        # Test transaction processing
        if 'process_transactions' in pipeline_functions:
            transactions = test_data.get('transactions', [])
            benchmark_results['transaction_processing'] = self.test_transaction_processing_speed(
                pipeline_functions['process_transactions'], transactions
            )
        
        # Test data loading
        if 'load_data' in pipeline_functions:
            data_size = test_data.get('data_size_mb', 1.0)
            benchmark_results['data_loading'] = self.test_data_loading_performance(
                pipeline_functions['load_data'], data_size
            )
        
        # Test query performance
        if 'queries' in pipeline_functions:
            for query_name, query_func in pipeline_functions['queries'].items():
                benchmark_results[f'query_{query_name}'] = self.test_query_performance(query_func, query_name)
        
        # Test memory usage
        if 'process_transactions' in pipeline_functions:
            transactions = test_data.get('transactions', [])
            benchmark_results['memory_usage'] = self.test_memory_usage(
                pipeline_functions['process_transactions'], transactions
            )
        
        # Calculate overall performance score
        passed_tests = sum(1 for result in benchmark_results.values() if result.get('passed', False))
        total_tests = len(benchmark_results)
        overall_score = passed_tests / total_tests if total_tests > 0 else 0
        
        return {
            'overall_score': overall_score,
            'passed_tests': passed_tests,
            'total_tests': total_tests,
            'benchmark_results': benchmark_results,
            'summary': {
                'performance_grade': self._calculate_performance_grade(overall_score),
                'recommendations': self._generate_recommendations(benchmark_results)
            }
        }
    
    def _calculate_performance_grade(self, score: float) -> str:
        """Calculate performance grade based on score"""
        if score >= 0.9:
            return 'A'
        elif score >= 0.8:
            return 'B'
        elif score >= 0.7:
            return 'C'
        elif score >= 0.6:
            return 'D'
        else:
            return 'F'
    
    def _generate_recommendations(self, results: Dict[str, Any]) -> List[str]:
        """Generate performance optimization recommendations"""
        recommendations = []
        
        for test_name, result in results.items():
            if not result.get('passed', False):
                if 'processing' in test_name:
                    recommendations.append(f"Optimize {test_name} - consider batch processing or parallelization")
                elif 'memory' in test_name:
                    recommendations.append(f"Reduce memory usage in {test_name} - consider streaming or chunking")
                elif 'query' in test_name:
                    recommendations.append(f"Optimize {test_name} - add indexes or rewrite query")
        
        return recommendations

# Example usage
def run_performance_tests():
    """Example function to run performance tests"""
    # This would be implemented based on your actual pipeline functions
    pass

if __name__ == "__main__":
    run_performance_tests()