"""
Main Pipeline Module for E-Commerce Analytics Pipeline
Orchestrates the complete data processing workflow
"""

import logging
import json
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import time

from data_extractor import DataExtractor
from data_transformer import DataTransformer
from database_manager import DatabaseManager
from config import config

# Configure logging
logging.basicConfig(
    level=getattr(logging, config.pipeline.log_level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ECommerceAnalyticsPipeline:
    """Main pipeline class for e-commerce analytics processing"""
    
    def __init__(self):
        self.extractor = DataExtractor()
        self.transformer = DataTransformer()
        self.db_manager = DatabaseManager()
        self.pipeline_start_time = None
        self.pipeline_end_time = None
        
    def run_pipeline(self, data_sources: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """
        Run the complete data pipeline
        
        Args:
            data_sources: Optional dictionary of data source paths
            
        Returns:
            Dictionary containing pipeline execution results
        """
        logger.info("Starting E-Commerce Analytics Pipeline")
        self.pipeline_start_time = datetime.now()
        
        pipeline_results = {
            'start_time': self.pipeline_start_time,
            'end_time': None,
            'duration_seconds': 0,
            'status': 'running',
            'stages': {},
            'errors': [],
            'data_quality_scores': {},
            'performance_metrics': {}
        }
        
        try:
            # Stage 1: Data Extraction
            logger.info("Stage 1: Data Extraction")
            extraction_results = self._run_extraction_stage(data_sources)
            pipeline_results['stages']['extraction'] = extraction_results
            
            if not extraction_results['success']:
                raise Exception("Data extraction failed")
            
            # Stage 2: Data Transformation
            logger.info("Stage 2: Data Transformation")
            transformation_results = self._run_transformation_stage(extraction_results['data'])
            pipeline_results['stages']['transformation'] = transformation_results
            
            if not transformation_results['success']:
                raise Exception("Data transformation failed")
            
            # Stage 3: Data Loading
            logger.info("Stage 3: Data Loading")
            loading_results = self._run_loading_stage(transformation_results['data'])
            pipeline_results['stages']['loading'] = loading_results
            
            if not loading_results['success']:
                raise Exception("Data loading failed")
            
            # Stage 4: Data Quality Validation
            logger.info("Stage 4: Data Quality Validation")
            quality_results = self._run_quality_validation_stage(transformation_results['data'])
            pipeline_results['stages']['quality_validation'] = quality_results
            pipeline_results['data_quality_scores'] = quality_results['scores']
            
            # Stage 5: Analytics Table Creation
            logger.info("Stage 5: Analytics Table Creation")
            analytics_results = self._run_analytics_creation_stage(transformation_results['data'])
            pipeline_results['stages']['analytics_creation'] = analytics_results
            
            # Calculate performance metrics
            self.pipeline_end_time = datetime.now()
            pipeline_results['end_time'] = self.pipeline_end_time
            pipeline_results['duration_seconds'] = (self.pipeline_end_time - self.pipeline_start_time).total_seconds()
            pipeline_results['status'] = 'completed'
            
            # Calculate performance metrics
            pipeline_results['performance_metrics'] = self._calculate_performance_metrics(pipeline_results)
            
            logger.info(f"Pipeline completed successfully in {pipeline_results['duration_seconds']:.2f} seconds")
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            pipeline_results['status'] = 'failed'
            pipeline_results['errors'].append(str(e))
            pipeline_results['end_time'] = datetime.now()
            pipeline_results['duration_seconds'] = (pipeline_results['end_time'] - self.pipeline_start_time).total_seconds()
            
        finally:
            # Cleanup
            self.db_manager.close_connections()
            
        return pipeline_results
    
    def _run_extraction_stage(self, data_sources: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """Run data extraction stage"""
        stage_start = datetime.now()
        
        try:
            # Extract data from all sources
            if data_sources:
                # Use provided data sources
                data = {}
                if 'transactions' in data_sources:
                    data['transactions'] = self.extractor.extract_transactions(data_sources['transactions'])
                if 'users' in data_sources:
                    data['users'] = self.extractor.extract_users(data_sources['users'])
                if 'products' in data_sources:
                    data['products'] = self.extractor.extract_products(data_sources['products'])
            else:
                # Use default data sources
                data = self.extractor.extract_all_sources()
            
            stage_end = datetime.now()
            stage_duration = (stage_end - stage_start).total_seconds()
            
            return {
                'success': True,
                'start_time': stage_start,
                'end_time': stage_end,
                'duration_seconds': stage_duration,
                'data': data,
                'records_extracted': {
                    'transactions': len(data.get('transactions', [])),
                    'users': len(data.get('users', [])),
                    'products': len(data.get('products', []))
                }
            }
            
        except Exception as e:
            logger.error(f"Extraction stage failed: {e}")
            return {
                'success': False,
                'start_time': stage_start,
                'end_time': datetime.now(),
                'duration_seconds': (datetime.now() - stage_start).total_seconds(),
                'error': str(e),
                'data': {}
            }
    
    def _run_transformation_stage(self, raw_data: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
        """Run data transformation stage"""
        stage_start = datetime.now()
        
        try:
            # Transform each data type
            transformed_data = {}
            
            # Transform transactions
            if 'transactions' in raw_data:
                transformed_data['transactions'] = self.transformer.transform_transactions(
                    raw_data['transactions'],
                    raw_data.get('users', []),
                    raw_data.get('products', [])
                )
            
            # Transform users
            if 'users' in raw_data:
                transformed_data['users'] = self.transformer.transform_users(raw_data['users'])
            
            # Transform products
            if 'products' in raw_data:
                transformed_data['products'] = self.transformer.transform_products(raw_data['products'])
            
            # Create analytics tables
            analytics_tables = self.transformer.create_analytics_tables(
                transformed_data.get('transactions', []),
                transformed_data.get('users', []),
                transformed_data.get('products', [])
            )
            transformed_data['analytics_tables'] = analytics_tables
            
            stage_end = datetime.now()
            stage_duration = (stage_end - stage_start).total_seconds()
            
            return {
                'success': True,
                'start_time': stage_start,
                'end_time': stage_end,
                'duration_seconds': stage_duration,
                'data': transformed_data,
                'records_transformed': {
                    'transactions': len(transformed_data.get('transactions', [])),
                    'users': len(transformed_data.get('users', [])),
                    'products': len(transformed_data.get('products', []))
                }
            }
            
        except Exception as e:
            logger.error(f"Transformation stage failed: {e}")
            return {
                'success': False,
                'start_time': stage_start,
                'end_time': datetime.now(),
                'duration_seconds': (datetime.now() - stage_start).total_seconds(),
                'error': str(e),
                'data': {}
            }
    
    def _run_loading_stage(self, transformed_data: Dict[str, Any]) -> Dict[str, Any]:
        """Run data loading stage"""
        stage_start = datetime.now()
        
        try:
            loading_results = {}
            
            # Load data into PostgreSQL
            if 'transactions' in transformed_data:
                loading_results['transactions_postgres'] = self.db_manager.insert_postgres_data(
                    'transactions', transformed_data['transactions']
                )
            
            if 'users' in transformed_data:
                loading_results['users_postgres'] = self.db_manager.insert_postgres_data(
                    'users', transformed_data['users']
                )
            
            if 'products' in transformed_data:
                loading_results['products_postgres'] = self.db_manager.insert_postgres_data(
                    'products', transformed_data['products']
                )
            
            # Load analytics tables into BigQuery
            if 'analytics_tables' in transformed_data:
                for table_name, table_data in transformed_data['analytics_tables'].items():
                    loading_results[f'{table_name}_bigquery'] = self.db_manager.insert_bigquery_data(
                        table_name, table_data
                    )
            
            stage_end = datetime.now()
            stage_duration = (stage_end - stage_start).total_seconds()
            
            return {
                'success': True,
                'start_time': stage_start,
                'end_time': stage_end,
                'duration_seconds': stage_duration,
                'loading_results': loading_results
            }
            
        except Exception as e:
            logger.error(f"Loading stage failed: {e}")
            return {
                'success': False,
                'start_time': stage_start,
                'end_time': datetime.now(),
                'duration_seconds': (datetime.now() - stage_start).total_seconds(),
                'error': str(e)
            }
    
    def _run_quality_validation_stage(self, transformed_data: Dict[str, Any]) -> Dict[str, Any]:
        """Run data quality validation stage"""
        stage_start = datetime.now()
        
        try:
            # Import quality tests
            from tests.test_data_quality import DataQualityTests
            
            quality_tester = DataQualityTests()
            
            # Run quality tests
            quality_results = quality_tester.run_all_tests(
                transformed_data.get('transactions', []),
                transformed_data.get('users', []),
                transformed_data.get('products', [])
            )
            
            stage_end = datetime.now()
            stage_duration = (stage_end - stage_start).total_seconds()
            
            return {
                'success': quality_results['all_tests_passed'],
                'start_time': stage_start,
                'end_time': stage_end,
                'duration_seconds': stage_duration,
                'scores': {
                    'overall_score': quality_results['overall_score'],
                    'completeness': quality_results['test_results']['completeness']['overall_score'],
                    'accuracy': quality_results['test_results']['accuracy']['overall_score'],
                    'consistency': quality_results['test_results']['consistency']['overall_score'],
                    'timeliness': quality_results['test_results']['timeliness']['overall_score']
                },
                'test_results': quality_results
            }
            
        except Exception as e:
            logger.error(f"Quality validation stage failed: {e}")
            return {
                'success': False,
                'start_time': stage_start,
                'end_time': datetime.now(),
                'duration_seconds': (datetime.now() - stage_start).total_seconds(),
                'error': str(e),
                'scores': {}
            }
    
    def _run_analytics_creation_stage(self, transformed_data: Dict[str, Any]) -> Dict[str, Any]:
        """Run analytics table creation stage"""
        stage_start = datetime.now()
        
        try:
            # This stage is already handled in transformation stage
            # But we can add additional analytics processing here
            
            stage_end = datetime.now()
            stage_duration = (stage_end - stage_start).total_seconds()
            
            return {
                'success': True,
                'start_time': stage_start,
                'end_time': stage_end,
                'duration_seconds': stage_duration,
                'analytics_tables_created': len(transformed_data.get('analytics_tables', {}))
            }
            
        except Exception as e:
            logger.error(f"Analytics creation stage failed: {e}")
            return {
                'success': False,
                'start_time': stage_start,
                'end_time': datetime.now(),
                'duration_seconds': (datetime.now() - stage_start).total_seconds(),
                'error': str(e)
            }
    
    def _calculate_performance_metrics(self, pipeline_results: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate performance metrics for the pipeline"""
        try:
            total_duration = pipeline_results['duration_seconds']
            total_records = sum(
                pipeline_results['stages']['extraction']['records_extracted'].values()
            )
            
            return {
                'total_duration_seconds': total_duration,
                'total_records_processed': total_records,
                'records_per_second': total_records / total_duration if total_duration > 0 else 0,
                'stage_durations': {
                    stage: results.get('duration_seconds', 0)
                    for stage, results in pipeline_results['stages'].items()
                }
            }
            
        except Exception as e:
            logger.error(f"Error calculating performance metrics: {e}")
            return {}
    
    def run_incremental_pipeline(self, since_timestamp: datetime) -> Dict[str, Any]:
        """
        Run incremental pipeline for data since given timestamp
        
        Args:
            since_timestamp: Process data since this timestamp
            
        Returns:
            Dictionary containing pipeline execution results
        """
        logger.info(f"Starting incremental pipeline since {since_timestamp}")
        
        # This would implement incremental processing logic
        # For now, just run the full pipeline
        return self.run_pipeline()
    
    def get_pipeline_status(self) -> Dict[str, Any]:
        """Get current pipeline status"""
        return {
            'is_running': self.pipeline_start_time is not None and self.pipeline_end_time is None,
            'start_time': self.pipeline_start_time,
            'end_time': self.pipeline_end_time,
            'duration_seconds': (self.pipeline_end_time - self.pipeline_start_time).total_seconds() if self.pipeline_end_time else None
        }

# Example usage
if __name__ == "__main__":
    pipeline = ECommerceAnalyticsPipeline()
    
    try:
        # Run the pipeline
        results = pipeline.run_pipeline()
        
        print("=== Pipeline Execution Results ===")
        print(f"Status: {results['status']}")
        print(f"Duration: {results['duration_seconds']:.2f} seconds")
        print(f"Data Quality Score: {results['data_quality_scores'].get('overall_score', 0):.2%}")
        
        if results['status'] == 'completed':
            print("Pipeline completed successfully!")
        else:
            print(f"Pipeline failed: {results['errors']}")
            
    except Exception as e:
        print(f"Error running pipeline: {e}")