"""
PMG Issue AI Pipeline - Database Writer Module
Handles auto-discovery, connection management, and data insertion
"""

import os
import sys
import json
import logging
import traceback
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from settings import get_settings
from aws_database_discovery import AWSRDSDiscovery, discover_aws_mysql_database
from database_schema import MySQLSchemaManager

logger = logging.getLogger(__name__)


class DatabaseWriterError(Exception):
    """Custom exception for database writer errors"""
    pass


class MySQLConnectionManager:
    """
    MySQL connection manager with AWS RDS support
    """
    
    def __init__(self, connection_config: Dict[str, Any]):
        """
        Initialize MySQL connection manager
        
        Args:
            connection_config: Database connection configuration
        """
        self.config = connection_config
        self.host = connection_config.get('host')
        self.port = connection_config.get('port', 3306)
        self.database = connection_config.get('database')
        self.username = connection_config.get('username')
        self.password = connection_config.get('password')
        self.connection_params = connection_config.get('connection_params', {})
        
        # Set default connection parameters for production
        default_params = {
            'charset': 'utf8mb4',
            'connect_timeout': 30,
            'autocommit': True,
            'use_unicode': True,
            'sql_mode': 'STRICT_TRANS_TABLES,NO_ZERO_DATE,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO'
        }
        
        # Merge with provided parameters
        # self.connection_params = {**default_params, **self.connection_params}
        filtered_params = {k: v for k, v in self.connection_params.items() if k != 'sslmode'}
        self.connection_params = {**default_params, **filtered_params}
        
        logger.info(f"MySQLConnectionManager initialized for {self.host}:{self.port}/{self.database}")
    
    def get_connection(self):
        """
        Get MySQL database connection
        
        Returns:
            MySQL connection object
            
        Raises:
            DatabaseWriterError: If connection fails
        """
        try:
            import pymysql
            
            connection = pymysql.connect(
                host=self.host,
                port=self.port,
                user=self.username,
                password=self.password,
                database=self.database,
                **self.connection_params
            )
            
            return connection
            
        except Exception as e:
            logger.error(f"MySQL connection failed: {e}")
            raise DatabaseWriterError(f"Failed to connect to MySQL database: {e}")
    
    def test_connection(self) -> bool:
        """
        Test database connection
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            connection = self.get_connection()
            cursor = connection.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            connection.close()
            
            logger.info("MySQL connection test successful")
            return True
            
        except Exception as e:
            logger.error(f"MySQL connection test failed: {e}")
            return False


class DatabaseWriter:
    """
    Production-ready database writer for PMG Issue AI ML results
    """
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize the database writer
        
        Args:
            config_path: Path to configuration file
        """
        # Load configuration
        self.settings = get_settings(config_path)
        
        # Setup logging
        self._setup_logging()
        
        # Initialize database configuration
        self.db_config = self._get_database_config()
        
        # Initialize connection manager
        self.connection_manager = MySQLConnectionManager(self.db_config)
        
        # Initialize schema manager
        self.table_name = self.db_config.get('table_name', 'ml_results')
        self.schema_manager = MySQLSchemaManager(self.db_config, self.table_name)
        
        # Processing configuration
        self.batch_size = self.db_config.get('batch_size', 1000)
        self.update_existing = self.db_config.get('update_existing', True)
        
        # Data paths
        # self.final_json_file = self.settings.get('data_paths.final_results_file', 
        #                                         'data/outputs/final_results/final_output.json')
        self.final_json_file = self.settings.get('data_paths.final_results_file', 
                                        'data/outputs/final_results/final_output.json')
        
        self.logger.info("DatabaseWriter initialized successfully")
    
    def _setup_logging(self) -> None:
        """Setup logging configuration"""
        log_config = self.settings.get_logging_config()
        log_dir = log_config.get('logs_path', 'logs/')
        
        # Create logs directory
        Path(log_dir).mkdir(parents=True, exist_ok=True)
        
        # Setup file handler
        log_file = Path(log_dir) / "database_writer.log"
        
        # Configure logging
        logging.basicConfig(
            level=getattr(logging, log_config.get('level', 'INFO')),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler() if log_config.get('enable_console_logging', True) else logging.NullHandler()
            ]
        )
        
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"Logging initialized. Log file: {log_file}")
    
    def _get_database_config(self) -> Dict[str, Any]:
        """
        Get database configuration with AWS discovery fallback
        
        Returns:
            Database configuration dictionary
            
        Raises:
            DatabaseWriterError: If no valid database configuration found
        """
        db_config = self.settings.get_database_config()
        
        if not db_config.get('enabled'):
            raise DatabaseWriterError("Database is not enabled in configuration")
        
        if not db_config.get('host') or not db_config.get('database'):
            raise DatabaseWriterError(
                "Database configuration incomplete. Please provide host and database name."
            )
        
        self.logger.info("Using database configuration from settings")
        return db_config
        # First try to get from settings
        # db_config = self.settings.get_database_config()
        
        # if db_config.get('enabled') and db_config.get('host'):
        #     self.logger.info("Using database configuration from settings")
        #     return db_config
        
        # # Try AWS discovery
        # self.logger.info("Attempting AWS RDS discovery...")
        # try:
        #     aws_config = discover_aws_mysql_database()
        #     if aws_config:
        #         # Merge with settings for missing values
        #         merged_config = {**db_config, **aws_config}
                
        #         # Ensure required fields from settings
        #         merged_config['database'] = db_config.get('database', 'pmg_issue_ai')
        #         merged_config['username'] = db_config.get('username') or aws_config.get('username')
        #         merged_config['password'] = db_config.get('password', '')
                
        #         self.logger.info(f"Using AWS discovered MySQL instance: {aws_config.get('host')}")
        #         return merged_config
        # except Exception as e:
        #     self.logger.warning(f"AWS discovery failed: {e}")
        
        # # Check if we have minimum required configuration
        # if not db_config.get('host') or not db_config.get('database'):
        #     raise DatabaseWriterError(
        #         "No valid database configuration found. Please configure database settings "
        #         "or ensure AWS RDS MySQL instance is accessible."
        #     )
        
        # return db_config
    
    def _validate_inputs(self) -> None:
        """
        Validate that required input files exist
        
        Raises:
            DatabaseWriterError: If required inputs are missing
        """
        self.logger.info("Validating input requirements...")
        
        # Check if final JSON file exists
        if not os.path.exists(self.final_json_file):
            raise DatabaseWriterError(
                f"Final JSON file not found: {self.final_json_file}. "
                "Please run post processing stage first."
            )
        
        # Validate file content
        try:
            with open(self.final_json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
                # Handle both formats: direct list or nested with metadata
                if isinstance(data, dict) and 'data' in data:
                    records = data['data']
                elif isinstance(data, list):
                    records = data
                else:
                    raise DatabaseWriterError("Invalid JSON file format")
                
                if not records:
                    raise DatabaseWriterError("Final JSON file contains no records")
                
                self.logger.info(f"Found {len(records)} records for database insertion")
                
        except json.JSONDecodeError:
            raise DatabaseWriterError("Final JSON file contains invalid JSON")
        except Exception as e:
            raise DatabaseWriterError(f"Error reading final JSON file: {e}")
        
        self.logger.info("Input validation successful")
    
    def _load_data(self) -> List[Dict[str, Any]]:
        """
        Load final ML results data
        
        Returns:
            List of ML results records
            
        Raises:
            DatabaseWriterError: If loading fails
        """
        self.logger.info(f"Loading data from: {self.final_json_file}")
        
        try:
            with open(self.final_json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Handle both formats
            if isinstance(data, dict) and 'data' in data:
                records = data['data']
            elif isinstance(data, list):
                records = data
            else:
                raise DatabaseWriterError("Invalid JSON file format")
            
            self.logger.info(f"Loaded {len(records)} records for database insertion")
            return records
            
        except Exception as e:
            raise DatabaseWriterError(f"Failed to load data: {e}")
    
    def _prepare_record_for_db(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Prepare a single record for database insertion
        
        Args:
            record: Original record from JSON
            
        Returns:
            Record formatted for database
        """
        # Field mapping from Excel/JSON format to database format
        field_mapping = {
            'Project ID': 'project_id',
            'Issue ID': 'issue_id',
            'Short Description': 'short_description',
            'Long Description': 'long_description',
            'Project Issue class': 'project_issue_class',
            'Similar Issue 1 Project ID': 'similar_issue_1_project_id',
            'Similar Issue 1 Issue ID': 'similar_issue_1_issue_id',
            'Similar Issue 1 Name': 'similar_issue_1_name',
            'Similar Issue 1 Start Date': 'similar_issue_1_start_date',
            'Similar Issue 1 Resolution Time': 'similar_issue_1_resolution_time',
            'Similar Issue 2 Project ID': 'similar_issue_2_project_id',
            'Similar Issue 2 Issue ID': 'similar_issue_2_issue_id',
            'Similar Issue 2 Name': 'similar_issue_2_name',
            'Similar Issue 2 Start Date': 'similar_issue_2_start_date',
            'Similar Issue 2 Resolution Time': 'similar_issue_2_resolution_time',
            'Similar Issue 3 Project ID': 'similar_issue_3_project_id',
            'Similar Issue 3 Issue ID': 'similar_issue_3_issue_id',
            'Similar Issue 3 Name': 'similar_issue_3_name',
            'Similar Issue 3 Start Date': 'similar_issue_3_start_date',
            'Similar Issue 3 Resolution Time': 'similar_issue_3_resolution_time',
            'Time AI Predicted Timeline': 'time_ai_predicted_timeline',
            'Issue Start Date': 'issue_start_date',
            'Predicted End Date': 'predicted_end_date',
            'Timeline Resolution Rationale': 'timeline_resolution_rationale',
            'Immediate next steps': 'immediate_next_steps',
            'Learn from similar Issues': 'learn_from_similar_issues',
            'Strategic Best practice': 'strategic_best_practice'
        }
        
        # Convert record
        db_record = {}
        
        for original_field, db_field in field_mapping.items():
            value = record.get(original_field, '')
            
            # Handle None values and convert to string
            if value is None:
                value = ''
            elif not isinstance(value, str):
                value = str(value)
            
            # Clean and truncate if necessary
            value = value.strip()
            
            # Apply field-specific formatting
            if db_field in ['project_id', 'issue_id']:
                # Required fields - ensure not empty
                if not value:
                    raise DatabaseWriterError(f"Required field {db_field} is empty")
                value = value[:100]  # Truncate to field limit
            elif db_field == 'project_issue_class':
                value = value[:300] if value else None
            elif db_field in ['similar_issue_1_project_id', 'similar_issue_2_project_id', 'similar_issue_3_project_id',
                             'similar_issue_1_issue_id', 'similar_issue_2_issue_id', 'similar_issue_3_issue_id']:
                value = value[:100] if value else None
            elif db_field in ['similar_issue_1_start_date', 'similar_issue_2_start_date', 'similar_issue_3_start_date',
                             'similar_issue_1_resolution_time', 'similar_issue_2_resolution_time', 'similar_issue_3_resolution_time',
                             'time_ai_predicted_timeline', 'issue_start_date', 'predicted_end_date']:
                value = value[:50] if value else None
            # TEXT and LONGTEXT fields can be longer, but set None if empty
            elif not value:
                value = None
            
            db_record[db_field] = value
        
        # Add metadata fields
        db_record['ml_pipeline_version'] = '1.0.0'
        db_record['processing_status'] = 'completed'
        
        return db_record
    
    def _get_insert_sql(self) -> str:
        """
        Get INSERT SQL statement with ON DUPLICATE KEY UPDATE
        
        Returns:
            SQL INSERT statement
        """
        # Define field order (excluding auto-generated fields)
        fields = [
            'project_id', 'issue_id', 'short_description', 'long_description', 'project_issue_class',
            'similar_issue_1_project_id', 'similar_issue_1_issue_id', 'similar_issue_1_name',
            'similar_issue_1_start_date', 'similar_issue_1_resolution_time',
            'similar_issue_2_project_id', 'similar_issue_2_issue_id', 'similar_issue_2_name',
            'similar_issue_2_start_date', 'similar_issue_2_resolution_time',
            'similar_issue_3_project_id', 'similar_issue_3_issue_id', 'similar_issue_3_name',
            'similar_issue_3_start_date', 'similar_issue_3_resolution_time',
            'time_ai_predicted_timeline', 'issue_start_date', 'predicted_end_date',
            'timeline_resolution_rationale', 'immediate_next_steps', 'learn_from_similar_issues',
            'strategic_best_practice', 'ml_pipeline_version', 'processing_status'
        ]
        
        placeholders = ', '.join(['%s'] * len(fields))
        fields_str = ', '.join([f'`{field}`' for field in fields])
        
        if self.update_existing:
            # Create ON DUPLICATE KEY UPDATE clause
            update_clauses = []
            for field in fields:
                if field not in ['project_id', 'issue_id']:  # Don't update key fields
                    update_clauses.append(f"`{field}` = VALUES(`{field}`)")
            
            update_clauses.append("`updated_at` = CURRENT_TIMESTAMP")
            update_str = ', '.join(update_clauses)
            
            sql = f"""
            INSERT INTO `{self.table_name}` ({fields_str})
            VALUES ({placeholders})
            ON DUPLICATE KEY UPDATE {update_str}
            """
        else:
            sql = f"""
            INSERT INTO `{self.table_name}` ({fields_str})
            VALUES ({placeholders})
            """
        
        return sql
    
    def _insert_batch(self, records: List[Dict[str, Any]]) -> int:
        """
        Insert a batch of records into the database
        
        Args:
            records: List of database-ready records
            
        Returns:
            Number of records successfully processed
            
        Raises:
            DatabaseWriterError: If insertion fails
        """
        if not records:
            return 0
        
        try:
            connection = self.connection_manager.get_connection()
            cursor = connection.cursor()
            
            # Prepare data tuples
            field_order = [
                'project_id', 'issue_id', 'short_description', 'long_description', 'project_issue_class',
                'similar_issue_1_project_id', 'similar_issue_1_issue_id', 'similar_issue_1_name',
                'similar_issue_1_start_date', 'similar_issue_1_resolution_time',
                'similar_issue_2_project_id', 'similar_issue_2_issue_id', 'similar_issue_2_name',
                'similar_issue_2_start_date', 'similar_issue_2_resolution_time',
                'similar_issue_3_project_id', 'similar_issue_3_issue_id', 'similar_issue_3_name',
                'similar_issue_3_start_date', 'similar_issue_3_resolution_time',
                'time_ai_predicted_timeline', 'issue_start_date', 'predicted_end_date',
                'timeline_resolution_rationale', 'immediate_next_steps', 'learn_from_similar_issues',
                'strategic_best_practice', 'ml_pipeline_version', 'processing_status'
            ]
            
            data_tuples = []
            for record in records:
                data_tuple = tuple(record.get(field) for field in field_order)
                data_tuples.append(data_tuple)
            
            # Execute batch insert
            sql = self._get_insert_sql()
            cursor.executemany(sql, data_tuples)
            
            # Get affected rows
            affected_rows = cursor.rowcount
            
            connection.commit()
            connection.close()
            
            self.logger.info(f"Successfully processed {affected_rows} records in batch")
            return len(data_tuples)  # Return number of records processed
            
        except Exception as e:
            self.logger.error(f"Error inserting batch: {e}")
            raise DatabaseWriterError(f"Failed to insert batch: {e}")
    
    def _write_data_to_db(self, records: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Write all data to database in batches
        
        Args:
            records: List of records to write
            
        Returns:
            Dictionary with processing statistics
        """
        self.logger.info(f"Writing {len(records)} records to database in batches of {self.batch_size}")
        
        total_processed = 0
        total_errors = 0
        batch_count = 0
        
        # Process in batches
        for i in range(0, len(records), self.batch_size):
            batch = records[i:i + self.batch_size]
            batch_count += 1
            
            try:
                self.logger.info(f"Processing batch {batch_count} ({len(batch)} records)...")
                
                # Prepare batch for database
                db_batch = []
                for record in batch:
                    try:
                        db_record = self._prepare_record_for_db(record)
                        db_batch.append(db_record)
                    except Exception as e:
                        self.logger.error(f"Error preparing record: {e}")
                        total_errors += 1
                
                if db_batch:
                    # Insert batch
                    processed = self._insert_batch(db_batch)
                    total_processed += processed
                    self.logger.info(f"Batch {batch_count} completed: {processed} records processed")
                
            except Exception as e:
                self.logger.error(f"Error processing batch {batch_count}: {e}")
                total_errors += len(batch)
        
        return {
            'total_records': len(records),
            'total_processed': total_processed,
            'total_errors': total_errors,
            'batch_count': batch_count
        }
    
    def run(self) -> Dict[str, Any]:
        """
        Run the complete database writing process
        
        Returns:
            Dictionary containing execution results
            
        Raises:
            DatabaseWriterError: If any step fails
        """
        start_time = datetime.now()
        self.logger.info("="*80)
        self.logger.info("STARTING PMG ISSUE AI PIPELINE - DATABASE WRITER")
        self.logger.info("="*80)
        
        try:
            # Step 1: Test database connection
            self.logger.info("Testing database connection...")
            if not self.connection_manager.test_connection():
                raise DatabaseWriterError("Database connection test failed")
            
            # Step 2: Ensure database exists
            database_name = self.db_config['database']
            if not self.schema_manager.database_exists(database_name):
                self.logger.info(f"Creating database: {database_name}")
                if not self.schema_manager.create_database(database_name):
                    raise DatabaseWriterError(f"Failed to create database: {database_name}")
            
            # Step 3: Setup database schema
            self.logger.info("Setting up database schema...")
            if not self.schema_manager.table_exists():
                self.logger.info(f"Creating table: {self.table_name}")
                if not self.schema_manager.create_table():
                    raise DatabaseWriterError("Failed to create database table")
            else:
                self.logger.info(f"Table {self.table_name} already exists")
                
                # Validate schema
                validation_result = self.schema_manager.validate_table_schema()
                if not validation_result['valid']:
                    self.logger.warning(f"Schema validation issues: {validation_result}")
            
            # Step 4: Validate inputs
            self._validate_inputs()
            
            # Step 5: Load data
            records = self._load_data()
            
            # Step 6: Write data to database
            stats = self._write_data_to_db(records)
            
            # Calculate execution time
            execution_time = datetime.now() - start_time
            
            # Get table info for final summary
            table_info = self.schema_manager.get_table_info()
            
            # Prepare results
            results = {
                "status": "success",
                "execution_time": str(execution_time),
                "database_info": {
                    "type": "mysql",
                    "host": self.db_config['host'],
                    "database": self.db_config['database'],
                    "table": self.table_name,
                    "total_rows_in_table": table_info.get('rows', 0)
                },
                "processing_stats": stats,
                "success_rate": f"{stats['total_processed']}/{stats['total_records']} ({stats['total_processed']/stats['total_records']*100:.1f}%)" if stats['total_records'] > 0 else "0/0 (0%)"
            }
            
            self.logger.info("="*80)
            self.logger.info("DATABASE WRITING COMPLETED SUCCESSFULLY")
            self.logger.info(f"Database: {self.db_config['host']}/{self.db_config['database']}")
            self.logger.info(f"Table: {self.table_name}")
            self.logger.info(f"Total records: {stats['total_records']}")
            self.logger.info(f"Successfully processed: {stats['total_processed']}")
            self.logger.info(f"Errors: {stats['total_errors']}")
            self.logger.info(f"Success rate: {results['success_rate']}")
            self.logger.info(f"Total rows in table: {table_info.get('rows', 0)}")
            self.logger.info(f"Execution time: {execution_time}")
            self.logger.info("="*80)
            
            return results
            
        except Exception as e:
            execution_time = datetime.now() - start_time
            error_msg = f"Database writing failed after {execution_time}: {e}"
            self.logger.error(error_msg)
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            
            raise DatabaseWriterError(error_msg)


def main():
    """
    Main function to run database writing
    Can be called independently or as part of the pipeline
    """
    try:
        writer = DatabaseWriter()
        results = writer.run()
        
        print("\nDatabase Writing Results:")
        print("="*50)

        
        return 0
        
    except Exception as e:
        print(f"Error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())