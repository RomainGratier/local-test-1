# E-Commerce Analytics Pipeline Challenge

## Overview
This is a comprehensive data engineering challenge designed for a senior data engineer with 5+ years of experience working with BigQuery and PostgreSQL. The challenge involves building a complete data pipeline for an e-commerce analytics platform.

## Challenge Description
You are tasked with building a robust data pipeline for TechMart, a growing e-commerce company that needs to process millions of daily transactions and generate real-time analytics. The pipeline must integrate data from multiple sources, perform real-time analytics, and maintain data quality standards.

## Technology Stack
- **Primary Database**: PostgreSQL (transactional data)
- **Data Warehouse**: Google BigQuery (analytics and reporting)
- **Cloud Platform**: Google Cloud Platform (GCP)
- **Languages**: Python (preferred) or SQL
- **Additional Tools**: Docker, Apache Airflow (optional)

## Challenge Duration
- **Time Limit**: 4 hours
- **Deliverables**: Complete pipeline code, documentation, and test results

## Project Structure
```
├── CHALLENGE_OVERVIEW.md          # Challenge overview and success criteria
├── REQUIREMENTS.md                # Detailed technical requirements
├── README.md                      # This file
├── requirements.txt               # Python dependencies
├── data/                          # Sample data files
│   ├── sample_transactions.json   # Transaction data
│   ├── sample_users.csv          # User data
│   └── sample_products.json      # Product data
├── schemas/                       # Database schemas
│   ├── postgres_schema.sql       # PostgreSQL schema
│   └── bigquery_schema.sql       # BigQuery schema
├── src/                          # Source code
│   ├── config.py                 # Configuration management
│   ├── data_extractor.py         # Data extraction module
│   ├── data_transformer.py       # Data transformation module
│   ├── database_manager.py       # Database operations
│   └── pipeline.py               # Main pipeline orchestration
└── tests/                        # Test suites
    ├── test_data_quality.py      # Data quality tests
    ├── test_performance.py       # Performance tests
    └── test_integration.py       # Integration tests
```

## Getting Started

### Prerequisites
1. Python 3.8 or higher
2. PostgreSQL 12 or higher
3. Google Cloud Platform account with BigQuery enabled
4. Docker (optional, for containerized deployment)

### Setup Instructions

1. **Clone and navigate to the project directory**
   ```bash
   cd /tmp/work/62f95808-5215-4006-b5d1-4a7ad0cfe448
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up environment variables**
   ```bash
   export POSTGRES_HOST=localhost
   export POSTGRES_DB=techmart
   export POSTGRES_USER=postgres
   export POSTGRES_PASSWORD=your_password
   export BIGQUERY_PROJECT_ID=your_project_id
   export BIGQUERY_DATASET_ID=techmart_analytics
   ```

4. **Set up PostgreSQL database**
   ```bash
   # Create database
   createdb techmart
   
   # Run schema
   psql -d techmart -f schemas/postgres_schema.sql
   ```

5. **Set up BigQuery dataset**
   ```bash
   # Create dataset in BigQuery console or using bq CLI
   bq mk --dataset your_project_id:techmart_analytics
   ```

### Running the Pipeline

1. **Basic pipeline execution**
   ```python
   from src.pipeline import ECommerceAnalyticsPipeline
   
   pipeline = ECommerceAnalyticsPipeline()
   results = pipeline.run_pipeline()
   print(f"Pipeline status: {results['status']}")
   ```

2. **Run with custom data sources**
   ```python
   data_sources = {
       'transactions': 'data/sample_transactions.json',
       'users': 'data/sample_users.csv',
       'products': 'data/sample_products.json'
   }
   
   results = pipeline.run_pipeline(data_sources)
   ```

3. **Run tests**
   ```bash
   # Run all tests
   python -m pytest tests/ -v
   
   # Run specific test suite
   python tests/test_data_quality.py
   python tests/test_performance.py
   python tests/test_integration.py
   ```

## Success Criteria

### ✅ Required Deliverables
1. **Data Pipeline**: Complete ETL pipeline processing all sample data
2. **Database Design**: Properly structured PostgreSQL and BigQuery schemas
3. **Data Quality**: Implemented data validation and quality checks
4. **Error Handling**: Robust error handling and retry logic
5. **Performance**: Meets specified performance benchmarks
6. **Documentation**: Clear code documentation and setup instructions

### ✅ Technical Requirements
1. **Data Sources**: Process JSON, CSV, and API data sources
2. **Data Transformation**: Apply business rules and data enrichment
3. **Database Operations**: Efficient data loading with proper indexing
4. **Analytics**: Generate daily sales summaries and user analytics
5. **Monitoring**: Implement data quality and performance monitoring
6. **Scalability**: Design for handling larger data volumes

### ✅ Performance Targets
- **Transaction Processing**: 1000 records/minute
- **Data Loading**: 10MB/minute to BigQuery
- **Query Performance**: < 10 seconds for standard reports
- **Data Quality**: > 95% completeness, > 99% accuracy

## Evaluation Criteria

### Technical Implementation (40%)
- Code quality and architecture
- Database design and optimization
- Error handling and robustness
- Performance optimization

### Data Engineering Best Practices (30%)
- Data quality implementation
- ETL pipeline design
- Monitoring and observability
- Scalability considerations

### Problem Solving (20%)
- Understanding of requirements
- Creative solutions to challenges
- Handling edge cases
- Performance optimization

### Documentation and Communication (10%)
- Code documentation
- Setup instructions
- Architecture decisions
- Performance analysis

## Tips for Success

1. **Start with the basics**: Get the pipeline working end-to-end first
2. **Focus on data quality**: Implement comprehensive validation
3. **Optimize for performance**: Use proper indexing and query optimization
4. **Handle errors gracefully**: Implement retry logic and error recovery
5. **Test thoroughly**: Use the provided test suites and add your own
6. **Document your decisions**: Explain your architectural choices

## Support

If you encounter any issues or have questions about the challenge:
1. Check the requirements document for detailed specifications
2. Review the sample data and schemas
3. Use the provided test suites to validate your implementation
4. Refer to the configuration examples in the source code

## Good Luck! 🚀

Remember, this challenge is designed to test your real-world data engineering skills. Focus on building a production-ready solution that demonstrates your expertise with BigQuery, PostgreSQL, and data pipeline architecture.