#!/usr/bin/env python3
"""
Challenge Runner Script
Executes the E-Commerce Analytics Pipeline challenge
"""

import sys
import os
import logging
from datetime import datetime
import json

# Add src directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from pipeline import ECommerceAnalyticsPipeline
from config import config

def setup_logging():
    """Setup logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('pipeline_execution.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )

def validate_environment():
    """Validate environment setup"""
    logger = logging.getLogger(__name__)
    
    # Check required environment variables
    required_vars = [
        'POSTGRES_HOST',
        'POSTGRES_DB', 
        'POSTGRES_USER',
        'POSTGRES_PASSWORD',
        'BIGQUERY_PROJECT_ID'
    ]
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        logger.error("Please set the following environment variables:")
        for var in missing_vars:
            logger.error(f"  export {var}=your_value")
        return False
    
    # Check if data files exist
    data_files = [
        'data/sample_transactions.json',
        'data/sample_users.csv',
        'data/sample_products.json'
    ]
    
    missing_files = [f for f in data_files if not os.path.exists(f)]
    if missing_files:
        logger.error(f"Missing data files: {', '.join(missing_files)}")
        return False
    
    logger.info("Environment validation passed")
    return True

def run_challenge():
    """Run the complete challenge"""
    logger = logging.getLogger(__name__)
    
    logger.info("=" * 60)
    logger.info("E-Commerce Analytics Pipeline Challenge")
    logger.info("=" * 60)
    
    # Validate environment
    if not validate_environment():
        logger.error("Environment validation failed. Please fix the issues above.")
        return False
    
    try:
        # Initialize pipeline
        logger.info("Initializing pipeline...")
        pipeline = ECommerceAnalyticsPipeline()
        
        # Run pipeline
        logger.info("Starting pipeline execution...")
        start_time = datetime.now()
        
        results = pipeline.run_pipeline()
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        # Display results
        logger.info("=" * 60)
        logger.info("PIPELINE EXECUTION RESULTS")
        logger.info("=" * 60)
        
        logger.info(f"Status: {results['status']}")
        logger.info(f"Duration: {duration:.2f} seconds")
        
        if results['status'] == 'completed':
            logger.info("✅ Pipeline completed successfully!")
            
            # Display data quality scores
            if 'data_quality_scores' in results:
                scores = results['data_quality_scores']
                logger.info("\nData Quality Scores:")
                logger.info(f"  Overall Score: {scores.get('overall_score', 0):.2%}")
                logger.info(f"  Completeness: {scores.get('completeness', 0):.2%}")
                logger.info(f"  Accuracy: {scores.get('accuracy', 0):.2%}")
                logger.info(f"  Consistency: {scores.get('consistency', 0):.2%}")
                logger.info(f"  Timeliness: {scores.get('timeliness', 0):.2%}")
            
            # Display performance metrics
            if 'performance_metrics' in results:
                metrics = results['performance_metrics']
                logger.info("\nPerformance Metrics:")
                logger.info(f"  Total Records Processed: {metrics.get('total_records_processed', 0):,}")
                logger.info(f"  Records per Second: {metrics.get('records_per_second', 0):.2f}")
                
                stage_durations = metrics.get('stage_durations', {})
                if stage_durations:
                    logger.info("\nStage Durations:")
                    for stage, duration in stage_durations.items():
                        logger.info(f"  {stage}: {duration:.2f} seconds")
            
            # Display stage results
            logger.info("\nStage Results:")
            for stage_name, stage_results in results.get('stages', {}).items():
                status = "✅ PASS" if stage_results.get('success', False) else "❌ FAIL"
                duration = stage_results.get('duration_seconds', 0)
                logger.info(f"  {stage_name}: {status} ({duration:.2f}s)")
                
                if 'records_extracted' in stage_results:
                    records = stage_results['records_extracted']
                    logger.info(f"    Records: {records}")
                elif 'records_transformed' in stage_results:
                    records = stage_results['records_transformed']
                    logger.info(f"    Records: {records}")
                elif 'loading_results' in stage_results:
                    loading = stage_results['loading_results']
                    logger.info(f"    Loading: {loading}")
            
        else:
            logger.error("❌ Pipeline failed!")
            if 'errors' in results:
                logger.error("Errors:")
                for error in results['errors']:
                    logger.error(f"  - {error}")
        
        # Save results to file
        results_file = f"pipeline_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(results_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        logger.info(f"\nResults saved to: {results_file}")
        
        return results['status'] == 'completed'
        
    except Exception as e:
        logger.error(f"Challenge execution failed: {e}")
        return False

def main():
    """Main entry point"""
    setup_logging()
    
    try:
        success = run_challenge()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logging.getLogger(__name__).info("Challenge interrupted by user")
        sys.exit(1)
    except Exception as e:
        logging.getLogger(__name__).error(f"Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()