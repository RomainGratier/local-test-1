"""
Database Management Module for E-Commerce Analytics Pipeline
Handles PostgreSQL and BigQuery operations with connection management
"""

import psycopg2
import pandas as pd
import logging
from typing import Dict, List, Any, Optional, Union
from contextlib import contextmanager
import json
from datetime import datetime

try:
    from google.cloud import bigquery
    from google.oauth2 import service_account
    BIGQUERY_AVAILABLE = True
except ImportError:
    BIGQUERY_AVAILABLE = False
    bigquery = None
    service_account = None

from config import config, POSTGRES_TABLES, BIGQUERY_TABLES

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseManager:
    """Manages database connections and operations"""
    
    def __init__(self):
        self.postgres_conn = None
        self.bigquery_client = None
        self._initialize_connections()
    
    def _initialize_connections(self):
        """Initialize database connections"""
        try:
            # Initialize PostgreSQL connection
            self._connect_postgres()
            
            # Initialize BigQuery client
            if BIGQUERY_AVAILABLE:
                self._connect_bigquery()
            else:
                logger.warning("BigQuery not available - install google-cloud-bigquery")
                
        except Exception as e:
            logger.error(f"Error initializing database connections: {e}")
            # Don't raise exception if only BigQuery fails
            if "PostgreSQL" in str(e):
                raise
    
    def _connect_postgres(self):
        """Connect to PostgreSQL database"""
        try:
            self.postgres_conn = psycopg2.connect(
                host=config.database.host,
                port=config.database.port,
                database=config.database.database,
                user=config.database.username,
                password=config.database.password
            )
            self.postgres_conn.autocommit = True
            logger.info("Successfully connected to PostgreSQL")
            
        except Exception as e:
            logger.error(f"Error connecting to PostgreSQL: {e}")
            raise
    
    def _connect_bigquery(self):
        """Connect to BigQuery"""
        try:
            if config.bigquery.credentials_path:
                credentials = service_account.Credentials.from_service_account_file(
                    config.bigquery.credentials_path
                )
                self.bigquery_client = bigquery.Client(
                    credentials=credentials,
                    project=config.bigquery.project_id
                )
            else:
                self.bigquery_client = bigquery.Client(project=config.bigquery.project_id)
            
            logger.info("Successfully connected to BigQuery")
            
        except Exception as e:
            logger.warning(f"Could not connect to BigQuery: {e}")
            logger.warning("Continuing without BigQuery - analytics features will be limited")
            self.bigquery_client = None
    
    @contextmanager
    def get_postgres_cursor(self):
        """Get PostgreSQL cursor with proper error handling"""
        cursor = None
        try:
            if not self.postgres_conn or self.postgres_conn.closed:
                self._connect_postgres()
            
            cursor = self.postgres_conn.cursor()
            yield cursor
            
        except Exception as e:
            if self.postgres_conn and not self.postgres_conn.closed:
                self.postgres_conn.rollback()
            logger.error(f"PostgreSQL error: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
    
    def create_postgres_tables(self):
        """Create PostgreSQL tables from schema"""
        try:
            with open('schemas/postgres_schema.sql', 'r') as f:
                schema_sql = f.read()
            
            with self.get_postgres_cursor() as cursor:
                cursor.execute(schema_sql)
            
            logger.info("Successfully created PostgreSQL tables")
            
        except Exception as e:
            logger.error(f"Error creating PostgreSQL tables: {e}")
            raise
    
    def insert_postgres_data(self, table_name: str, data: List[Dict[str, Any]], 
                           batch_size: int = None) -> int:
        """
        Insert data into PostgreSQL table
        
        Args:
            table_name: Name of the table
            data: List of dictionaries containing data
            batch_size: Number of records to insert per batch
            
        Returns:
            Number of records inserted
        """
        if not data:
            return 0
        
        batch_size = batch_size or config.pipeline.batch_size
        total_inserted = 0
        
        try:
            with self.get_postgres_cursor() as cursor:
                # Get valid columns for the table
                valid_columns = self._get_table_columns(cursor, table_name)
                
                # Filter data to only include valid columns
                filtered_data = []
                for record in data:
                    filtered_record = {k: v for k, v in record.items() if k in valid_columns}
                    filtered_data.append(filtered_record)
                
                if not filtered_data:
                    logger.warning(f"No valid columns found for table {table_name}")
                    return 0
                
                # Get column names from first record
                columns = list(filtered_data[0].keys())
                placeholders = ', '.join(['%s'] * len(columns))
                columns_str = ', '.join(columns)
                
                # Insert data in batches
                for i in range(0, len(filtered_data), batch_size):
                    batch = filtered_data[i:i + batch_size]
                    
                    # Prepare batch data
                    batch_values = []
                    for record in batch:
                        values = [record.get(col) for col in columns]
                        batch_values.append(values)
                    
                    # Execute batch insert
                    insert_sql = f"""
                        INSERT INTO {table_name} ({columns_str})
                        VALUES ({placeholders})
                        ON CONFLICT DO NOTHING
                    """
                    
                    cursor.executemany(insert_sql, batch_values)
                    total_inserted += len(batch)
                    
                    logger.info(f"Inserted batch of {len(batch)} records into {table_name}")
            
            logger.info(f"Successfully inserted {total_inserted} records into {table_name}")
            return total_inserted
            
        except Exception as e:
            logger.error(f"Error inserting data into {table_name}: {e}")
            raise
    
    def upsert_postgres_data(self, table_name: str, data: List[Dict[str, Any]], 
                           conflict_columns: List[str]) -> int:
        """
        Upsert data into PostgreSQL table
        
        Args:
            table_name: Name of the table
            data: List of dictionaries containing data
            conflict_columns: Columns to check for conflicts
            
        Returns:
            Number of records processed
        """
        if not data:
            return 0
        
        try:
            with self.get_postgres_cursor() as cursor:
                # Get column names
                columns = list(data[0].keys())
                placeholders = ', '.join(['%s'] * len(columns))
                columns_str = ', '.join(columns)
                
                # Create conflict resolution clause
                conflict_clause = ', '.join(conflict_columns)
                update_clause = ', '.join([f"{col} = EXCLUDED.{col}" for col in columns if col not in conflict_columns])
                
                upsert_sql = f"""
                    INSERT INTO {table_name} ({columns_str})
                    VALUES ({placeholders})
                    ON CONFLICT ({conflict_clause})
                    DO UPDATE SET {update_clause}
                """
                
                # Prepare data
                values = []
                for record in data:
                    values.append([record.get(col) for col in columns])
                
                cursor.executemany(upsert_sql, values)
                
            logger.info(f"Successfully upserted {len(data)} records into {table_name}")
            return len(data)
            
        except Exception as e:
            logger.error(f"Error upserting data into {table_name}: {e}")
            raise
    
    def query_postgres(self, query: str, params: tuple = None) -> List[Dict[str, Any]]:
        """
        Execute query on PostgreSQL and return results
        
        Args:
            query: SQL query string
            params: Query parameters
            
        Returns:
            List of dictionaries containing query results
        """
        try:
            with self.get_postgres_cursor() as cursor:
                cursor.execute(query, params)
                
                # Get column names
                columns = [desc[0] for desc in cursor.description]
                
                # Fetch results
                results = cursor.fetchall()
                
                # Convert to list of dictionaries
                return [dict(zip(columns, row)) for row in results]
                
        except Exception as e:
            logger.error(f"Error executing PostgreSQL query: {e}")
            raise
    
    def create_bigquery_tables(self):
        """Create BigQuery tables from schema"""
        if not self.bigquery_client:
            logger.warning("BigQuery client not available")
            return
        
        try:
            with open('schemas/bigquery_schema.sql', 'r') as f:
                schema_sql = f.read()
            
            # Split schema into individual table creation statements
            statements = [stmt.strip() for stmt in schema_sql.split(';') if stmt.strip()]
            
            for statement in statements:
                if statement.upper().startswith('CREATE TABLE'):
                    job = self.bigquery_client.query(statement)
                    job.result()  # Wait for job to complete
            
            logger.info("Successfully created BigQuery tables")
            
        except Exception as e:
            logger.error(f"Error creating BigQuery tables: {e}")
            raise
    
    def insert_bigquery_data(self, table_name: str, data: List[Dict[str, Any]]) -> int:
        """
        Insert data into BigQuery table
        
        Args:
            table_name: Name of the table
            data: List of dictionaries containing data
            
        Returns:
            Number of records inserted
        """
        if not self.bigquery_client:
            logger.warning("BigQuery client not available")
            return 0
        
        if not data:
            return 0
        
        try:
            # Get table reference
            table_ref = self.bigquery_client.dataset(config.bigquery.dataset_id).table(table_name)
            
            # Convert data to DataFrame
            df = pd.DataFrame(data)
            
            # Configure job
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",
                create_disposition="CREATE_IF_NEEDED"
            )
            
            # Load data
            job = self.bigquery_client.load_table_from_dataframe(
                df, table_ref, job_config=job_config
            )
            
            # Wait for job to complete
            job.result()
            
            logger.info(f"Successfully inserted {len(data)} records into BigQuery {table_name}")
            return len(data)
            
        except Exception as e:
            logger.error(f"Error inserting data into BigQuery {table_name}: {e}")
            raise
    
    def query_bigquery(self, query: str) -> pd.DataFrame:
        """
        Execute query on BigQuery and return results
        
        Args:
            query: SQL query string
            
        Returns:
            DataFrame containing query results
        """
        if not self.bigquery_client:
            logger.warning("BigQuery client not available")
            return pd.DataFrame()
        
        try:
            # Execute query
            query_job = self.bigquery_client.query(query)
            results = query_job.result()
            
            # Convert to DataFrame
            df = results.to_dataframe()
            
            logger.info(f"Successfully executed BigQuery query, returned {len(df)} rows")
            return df
            
        except Exception as e:
            logger.error(f"Error executing BigQuery query: {e}")
            raise
    
    def get_table_count(self, table_name: str, database: str = 'postgres') -> int:
        """
        Get record count from table
        
        Args:
            table_name: Name of the table
            database: Database type ('postgres' or 'bigquery')
            
        Returns:
            Number of records in table
        """
        try:
            if database == 'postgres':
                query = f"SELECT COUNT(*) as count FROM {table_name}"
                results = self.query_postgres(query)
                return results[0]['count'] if results else 0
            
            elif database == 'bigquery':
                if not self.bigquery_client:
                    return 0
                
                query = f"SELECT COUNT(*) as count FROM `{config.bigquery.dataset_reference}.{table_name}`"
                df = self.query_bigquery(query)
                return df['count'].iloc[0] if not df.empty else 0
            
            else:
                raise ValueError(f"Unsupported database type: {database}")
                
        except Exception as e:
            logger.error(f"Error getting table count for {table_name}: {e}")
            return 0
    
    def close_connections(self):
        """Close all database connections"""
        try:
            if self.postgres_conn and not self.postgres_conn.closed:
                self.postgres_conn.close()
                logger.info("PostgreSQL connection closed")
            
            if self.bigquery_client:
                # BigQuery client doesn't need explicit closing
                self.bigquery_client = None
                logger.info("BigQuery client closed")
                
        except Exception as e:
            logger.error(f"Error closing database connections: {e}")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_connections()
    
    def _get_table_columns(self, cursor, table_name: str) -> set:
        """Get valid column names for a table"""
        try:
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = %s 
                AND table_schema = 'public'
            """, (table_name,))
            
            columns = {row[0] for row in cursor.fetchall()}
            return columns
            
        except Exception as e:
            logger.warning(f"Could not get columns for table {table_name}: {e}")
            return set()

# Example usage
if __name__ == "__main__":
    with DatabaseManager() as db:
        # Create tables
        db.create_postgres_tables()
        db.create_bigquery_tables()
        
        print("Database manager initialized successfully")