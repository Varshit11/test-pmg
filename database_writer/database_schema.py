"""
Database Schema Management Module
Manages MySQL database schema for PMG Issue AI ML results
Handles table creation, validation, and migrations
"""

import logging
import pymysql
from typing import Dict, Any, List, Optional
from contextlib import contextmanager

logger = logging.getLogger(__name__)


class DatabaseSchemaError(Exception):
    """Custom exception for database schema errors"""
    pass


class MySQLSchemaManager:
    """
    MySQL database schema manager for ML results table
    """
    
    def __init__(self, connection_config: Dict[str, Any], table_name: str = 'ml_results'):
        """
        Initialize schema manager
        
        Args:
            connection_config: Database connection configuration
            table_name: Name of the ML results table
        """
        self.connection_config = connection_config
        self.table_name = table_name
        
        # Table schema definition
        self.schema_version = "1.0.0"
        self.table_columns = {
            'id': {
                'type': 'BIGINT',
                'auto_increment': True,
                'primary_key': True,
                'nullable': False,
                'comment': 'Auto-incrementing primary key'
            },
            'project_id': {
                'type': 'VARCHAR(100)',
                'nullable': False,
                'comment': 'Project identifier - part of business key'
            },
            'issue_id': {
                'type': 'VARCHAR(100)', 
                'nullable': False,
                'comment': 'Issue identifier - part of business key'
            },
            'short_description': {
                'type': 'TEXT',
                'nullable': True,
                'comment': 'AI-generated short description of the issue'
            },
            'long_description': {
                'type': 'LONGTEXT',
                'nullable': True,
                'comment': 'AI-generated detailed description of the issue'
            },
            'project_issue_class': {
                'type': 'VARCHAR(300)',
                'nullable': True,
                'comment': 'AI-classified issue category'
            },
            'similar_issue_1_project_id': {
                'type': 'VARCHAR(100)',
                'nullable': True,
                'comment': 'First similar issue project ID'
            },
            'similar_issue_1_issue_id': {
                'type': 'VARCHAR(100)',
                'nullable': True,
                'comment': 'First similar issue ID'
            },
            'similar_issue_1_name': {
                'type': 'TEXT',
                'nullable': True,
                'comment': 'First similar issue name/title'
            },
            'similar_issue_1_start_date': {
                'type': 'VARCHAR(50)',
                'nullable': True,
                'comment': 'First similar issue start date'
            },
            'similar_issue_1_resolution_time': {
                'type': 'VARCHAR(50)',
                'nullable': True,
                'comment': 'First similar issue resolution time'
            },
            'similar_issue_2_project_id': {
                'type': 'VARCHAR(100)',
                'nullable': True,
                'comment': 'Second similar issue project ID'
            },
            'similar_issue_2_issue_id': {
                'type': 'VARCHAR(100)',
                'nullable': True,
                'comment': 'Second similar issue ID'
            },
            'similar_issue_2_name': {
                'type': 'TEXT',
                'nullable': True,
                'comment': 'Second similar issue name/title'
            },
            'similar_issue_2_start_date': {
                'type': 'VARCHAR(50)',
                'nullable': True,
                'comment': 'Second similar issue start date'
            },
            'similar_issue_2_resolution_time': {
                'type': 'VARCHAR(50)',
                'nullable': True,
                'comment': 'Second similar issue resolution time'
            },
            'similar_issue_3_project_id': {
                'type': 'VARCHAR(100)',
                'nullable': True,
                'comment': 'Third similar issue project ID'
            },
            'similar_issue_3_issue_id': {
                'type': 'VARCHAR(100)',
                'nullable': True,
                'comment': 'Third similar issue ID'
            },
            'similar_issue_3_name': {
                'type': 'TEXT',
                'nullable': True,
                'comment': 'Third similar issue name/title'
            },
            'similar_issue_3_start_date': {
                'type': 'VARCHAR(50)',
                'nullable': True,
                'comment': 'Third similar issue start date'
            },
            'similar_issue_3_resolution_time': {
                'type': 'VARCHAR(50)',
                'nullable': True,
                'comment': 'Third similar issue resolution time'
            },
            'time_ai_predicted_timeline': {
                'type': 'VARCHAR(50)',
                'nullable': True,
                'comment': 'AI predicted timeline in days'
            },
            'issue_start_date': {
                'type': 'VARCHAR(50)',
                'nullable': True,
                'comment': 'Issue creation/start date'
            },
            'predicted_end_date': {
                'type': 'VARCHAR(50)',
                'nullable': True,
                'comment': 'AI predicted completion date'
            },
            'timeline_resolution_rationale': {
                'type': 'LONGTEXT',
                'nullable': True,
                'comment': 'AI explanation for timeline prediction'
            },
            'immediate_next_steps': {
                'type': 'LONGTEXT',
                'nullable': True,
                'comment': 'AI recommended immediate next steps'
            },
            'learn_from_similar_issues': {
                'type': 'LONGTEXT',
                'nullable': True,
                'comment': 'AI recommendations based on similar issues'
            },
            'strategic_best_practice': {
                'type': 'LONGTEXT',
                'nullable': True,
                'comment': 'AI recommended strategic best practices'
            },
            'created_at': {
                'type': 'TIMESTAMP',
                'default': 'CURRENT_TIMESTAMP',
                'nullable': False,
                'comment': 'Record creation timestamp'
            },
            'updated_at': {
                'type': 'TIMESTAMP',
                'default': 'CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP',
                'nullable': False,
                'comment': 'Record last update timestamp'
            },
            'ml_pipeline_version': {
                'type': 'VARCHAR(50)',
                'nullable': True,
                'default': "'1.0.0'",
                'comment': 'ML pipeline version used to generate results'
            },
            'processing_status': {
                'type': "ENUM('pending', 'completed', 'error')",
                'nullable': True,
                'default': "'completed'",
                'comment': 'Processing status of the ML pipeline'
            }
        }
        
        # Indexes definition
        self.indexes = {
            'idx_project_issue': {
                'columns': ['project_id', 'issue_id'],
                'unique': True,
                'comment': 'Unique constraint on project_id and issue_id combination'
            },
            'idx_created_at': {
                'columns': ['created_at'],
                'unique': False,
                'comment': 'Index on creation timestamp for time-based queries'
            },
            'idx_updated_at': {
                'columns': ['updated_at'],
                'unique': False,
                'comment': 'Index on update timestamp for change tracking'
            },
            'idx_issue_class': {
                'columns': ['project_issue_class'],
                'unique': False,
                'comment': 'Index on issue classification for filtering'
            },
            'idx_processing_status': {
                'columns': ['processing_status'],
                'unique': False,
                'comment': 'Index on processing status for monitoring'
            }
        }
        
        logger.info(f"MySQLSchemaManager initialized for table: {table_name}")
    
    @contextmanager
    def get_connection(self):
        """
        Context manager for database connections
        
        Yields:
            MySQL connection object
        """
        connection = None
        try:
            connection = pymysql.connect(
                host=self.connection_config['host'],
                port=self.connection_config['port'],
                user=self.connection_config['username'],
                password=self.connection_config['password'],
                database=self.connection_config.get('database', ''),
                charset='utf8mb4',
                **self.connection_config.get('connection_params', {})
            )
            
            yield connection
            
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise DatabaseSchemaError(f"Failed to connect to database: {e}")
        finally:
            if connection:
                connection.close()
    
    def database_exists(self, database_name: str) -> bool:
        """
        Check if database exists
        
        Args:
            database_name: Name of the database to check
            
        Returns:
            True if database exists, False otherwise
        """
        try:
            # Connect without specifying database
            temp_config = self.connection_config.copy()
            temp_config['database'] = ''
            
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SHOW DATABASES LIKE %s", (database_name,))
                result = cursor.fetchone()
                return result is not None
                
        except Exception as e:
            logger.error(f"Error checking database existence: {e}")
            return False
    
    def create_database(self, database_name: str) -> bool:
        """
        Create database if it doesn't exist
        
        Args:
            database_name: Name of the database to create
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Connect without specifying database
            temp_config = self.connection_config.copy()
            temp_config['database'] = ''
            
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Create database with proper charset
                cursor.execute(f"""
                    CREATE DATABASE IF NOT EXISTS `{database_name}` 
                    CHARACTER SET utf8mb4 
                    COLLATE utf8mb4_unicode_ci
                """)
                
                conn.commit()
                logger.info(f"Database '{database_name}' created successfully")
                return True
                
        except Exception as e:
            logger.error(f"Error creating database '{database_name}': {e}")
            return False
    
    def table_exists(self) -> bool:
        """
        Check if the ML results table exists
        
        Returns:
            True if table exists, False otherwise
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT COUNT(*) 
                    FROM information_schema.tables 
                    WHERE table_schema = DATABASE() 
                    AND table_name = %s
                """, (self.table_name,))
                
                result = cursor.fetchone()
                return result[0] > 0
                
        except Exception as e:
            logger.error(f"Error checking table existence: {e}")
            return False
    
    def get_create_table_sql(self) -> str:
        """
        Generate CREATE TABLE SQL statement
        
        Returns:
            SQL statement to create the ML results table
        """
        # Build column definitions
        column_definitions = []
        
        for column_name, column_config in self.table_columns.items():
            column_def = f"`{column_name}` {column_config['type']}"
            
            # Add nullable constraint
            if not column_config.get('nullable', True):
                column_def += " NOT NULL"
            else:
                column_def += " NULL"
            
            # Add auto increment
            if column_config.get('auto_increment'):
                column_def += " AUTO_INCREMENT"
            
            # Add default value
            if 'default' in column_config:
                column_def += f" DEFAULT {column_config['default']}"
            
            # Add comment
            if 'comment' in column_config:
                column_def += f" COMMENT '{column_config['comment']}'"
            
            column_definitions.append(column_def)
        
        # Add primary key
        primary_key_columns = [col for col, config in self.table_columns.items() 
                              if config.get('primary_key')]
        if primary_key_columns:
            column_definitions.append(f"PRIMARY KEY (`{'`, `'.join(primary_key_columns)}`)")
        
        # Add unique constraints from indexes
        for index_name, index_config in self.indexes.items():
            if index_config.get('unique'):
                columns_str = '`, `'.join(index_config['columns'])
                column_definitions.append(f"UNIQUE KEY `{index_name}` (`{columns_str}`)")
        
        # Create table SQL
        sql = f"""
        CREATE TABLE IF NOT EXISTS `{self.table_name}` (
            {',            '.join(column_definitions)}
        ) ENGINE=InnoDB 
        DEFAULT CHARSET=utf8mb4 
        COLLATE=utf8mb4_unicode_ci 
        COMMENT='PMG Issue AI ML Results - Schema Version {self.schema_version}'"""
        
        return sql
    
    def get_create_indexes_sql(self) -> List[str]:
        """
        Generate CREATE INDEX SQL statements
        
        Returns:
            List of SQL statements to create indexes
        """
        index_statements = []
        
        for index_name, index_config in self.indexes.items():
            if not index_config.get('unique'):  # Unique indexes already created with table
                columns_str = '`, `'.join(index_config['columns'])
                sql = f"""
                CREATE INDEX IF NOT EXISTS `{index_name}` 
                ON `{self.table_name}` (`{columns_str}`)
                """
                index_statements.append(sql)
        
        return index_statements
    
    def create_table(self) -> bool:
        """
        Create the ML results table and indexes
        
        Returns:
            True if successful, False otherwise
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Create table
                table_sql = self.get_create_table_sql()
                cursor.execute(table_sql)
                
                # Create additional indexes
                for index_sql in self.get_create_indexes_sql():
                    cursor.execute(index_sql)
                
                conn.commit()
                logger.info(f"Table '{self.table_name}' and indexes created successfully")
                return True
                
        except Exception as e:
            logger.error(f"Error creating table: {e}")
            return False
    
    def validate_table_schema(self) -> Dict[str, Any]:
        """
        Validate existing table schema against expected schema
        
        Returns:
            Dictionary with validation results
        """
        if not self.table_exists():
            return {
                'valid': False,
                'error': 'Table does not exist',
                'missing_columns': list(self.table_columns.keys()),
                'extra_columns': [],
                'schema_differences': []
            }
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Get current table structure
                cursor.execute(f"DESCRIBE `{self.table_name}`")
                current_columns = {row[0]: row for row in cursor.fetchall()}
                
                missing_columns = []
                extra_columns = []
                schema_differences = []
                
                # Check for missing columns
                for expected_column in self.table_columns.keys():
                    if expected_column not in current_columns:
                        missing_columns.append(expected_column)
                
                # Check for extra columns
                for current_column in current_columns.keys():
                    if current_column not in self.table_columns:
                        extra_columns.append(current_column)
                
                # Check for schema differences in existing columns
                for column_name, expected_config in self.table_columns.items():
                    if column_name in current_columns:
                        current_column = current_columns[column_name]
                        # This is a simplified check - you might want more detailed validation
                        if not self._column_types_compatible(expected_config['type'], current_column[1]):
                            schema_differences.append({
                                'column': column_name,
                                'expected': expected_config['type'],
                                'actual': current_column[1]
                            })
                
                is_valid = (not missing_columns and 
                           not extra_columns and 
                           not schema_differences)
                
                return {
                    'valid': is_valid,
                    'missing_columns': missing_columns,
                    'extra_columns': extra_columns,
                    'schema_differences': schema_differences,
                    'table_exists': True
                }
                
        except Exception as e:
            logger.error(f"Error validating table schema: {e}")
            return {
                'valid': False,
                'error': str(e),
                'missing_columns': [],
                'extra_columns': [],
                'schema_differences': []
            }
    
    def _column_types_compatible(self, expected_type: str, actual_type: str) -> bool:
        """
        Check if column types are compatible
        
        Args:
            expected_type: Expected column type
            actual_type: Actual column type from database
            
        Returns:
            True if compatible, False otherwise
        """
        # Normalize types for comparison
        expected_normalized = expected_type.upper().split('(')[0]
        actual_normalized = actual_type.upper().split('(')[0]
        
        # Simple compatibility check
        type_mappings = {
            'BIGINT': ['BIGINT', 'INT'],
            'INT': ['INT', 'BIGINT'],
            'VARCHAR': ['VARCHAR', 'CHAR'],
            'TEXT': ['TEXT', 'MEDIUMTEXT', 'LONGTEXT'],
            'LONGTEXT': ['LONGTEXT', 'TEXT', 'MEDIUMTEXT'],
            'TIMESTAMP': ['TIMESTAMP', 'DATETIME'],
            'ENUM': ['ENUM']
        }
        
        expected_compatible = type_mappings.get(expected_normalized, [expected_normalized])
        return actual_normalized in expected_compatible
    
    def get_table_info(self) -> Dict[str, Any]:
        """
        Get comprehensive table information
        
        Returns:
            Dictionary with table information
        """
        if not self.table_exists():
            return {'exists': False}
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Get table status
                cursor.execute(f"SHOW TABLE STATUS LIKE '{self.table_name}'")
                table_status = cursor.fetchone()
                
                # Get column information
                cursor.execute(f"SHOW FULL COLUMNS FROM `{self.table_name}`")
                columns = cursor.fetchall()
                
                # Get index information
                cursor.execute(f"SHOW INDEX FROM `{self.table_name}`")
                indexes = cursor.fetchall()
                
                return {
                    'exists': True,
                    'engine': table_status[1] if table_status else None,
                    'rows': table_status[4] if table_status else 0,
                    'data_length': table_status[6] if table_status else 0,
                    'index_length': table_status[8] if table_status else 0,
                    'collation': table_status[14] if table_status else None,
                    'comment': table_status[17] if table_status else None,
                    'columns': [
                        {
                            'name': col[0],
                            'type': col[1],
                            'nullable': col[3] == 'YES',
                            'default': col[5],
                            'comment': col[8]
                        } for col in columns
                    ],
                    'indexes': [
                        {
                            'name': idx[2],
                            'column': idx[4],
                            'unique': idx[1] == 0
                        } for idx in indexes
                    ]
                }
                
        except Exception as e:
            logger.error(f"Error getting table info: {e}")
            return {'exists': True, 'error': str(e)}


def create_schema_manager(connection_config: Dict[str, Any], 
                         table_name: str = 'ml_results') -> MySQLSchemaManager:
    """
    Factory function to create schema manager
    
    Args:
        connection_config: Database connection configuration
        table_name: Name of the ML results table
        
    Returns:
        MySQLSchemaManager instance
    """
    return MySQLSchemaManager(connection_config, table_name)
