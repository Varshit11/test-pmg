import sys
import logging
import json
import os
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from database_writer.database_writer import DatabaseWriter

def test_database_writer():
    """
    Test the database writer module
    Similar to test_post_processing.py but for database operations
    """
    print("="*80)
    print("TESTING PMG ISSUE AI PIPELINE - DATABASE WRITER")
    print("="*80)
    
    try:
        # Initialize the database writer
        print("Initializing DatabaseWriter...")
        writer = DatabaseWriter()
        
        # Run the database writing process
        print("Running database writing process...")
        results = writer.run()
        
        # Display results
        print("\nDatabase Writing Results:")
        print("="*50)
        print(f"Status: {results['status']}")
        print(f"Execution Time: {results['execution_time']}")
        
        # Database info
        db_info = results['database_info']
        print(f"\nDatabase Information:")
        print(f"  Type: {db_info['type']}")
        print(f"  Host: {db_info['host']}")
        print(f"  Database: {db_info['database']}")
        print(f"  Table: {db_info['table']}")
        print(f"  Total Rows in Table: {db_info['total_rows_in_table']}")
        
        # Processing stats
        stats = results['processing_stats']
        print(f"\nProcessing Statistics:")
        print(f"  Total Records: {stats['total_records']}")
        print(f"  Successfully Processed: {stats['total_processed']}")
        print(f"  Errors: {stats['total_errors']}")
        print(f"  Batches Processed: {stats['batch_count']}")
        print(f"  Success Rate: {results['success_rate']}")
        
        print("\n" + "="*50)
        print("DATABASE WRITER TEST COMPLETED SUCCESSFULLY")
        print("="*50)
        
        return results
        
    except Exception as e:
        print(f"\nERROR: Database writer test failed: {e}")
        print(f"Error Type: {type(e).__name__}")
        
        # Print more detailed error information if available
        if hasattr(e, '__traceback__'):
            import traceback
            print(f"Traceback: {traceback.format_exc()}")
        
        return None

def test_database_connection_only():
    """
    Test only the database connection without writing data
    Useful for verifying database connectivity
    """
    print("="*60)
    print("TESTING DATABASE CONNECTION ONLY")
    print("="*60)
    
    try:
        # Initialize the database writer
        writer = DatabaseWriter()
        
        # Test connection
        print("Testing database connection...")
        if writer.connection_manager.test_connection():
            print("✓ Database connection successful")
            
            # Get database info
            db_config = writer.db_config
            print(f"✓ Connected to: {db_config['host']}:{db_config['port']}/{db_config['database']}")
            
            # Check if table exists
            if writer.schema_manager.table_exists():
                print("✓ Table exists")
                table_info = writer.schema_manager.get_table_info()
                print(f"  - Table rows: {table_info.get('rows', 0)}")
                print(f"  - Engine: {table_info.get('engine', 'Unknown')}")
            else:
                print("ℹ Table does not exist (will be created on first run)")
            
            return True
        else:
            print("✗ Database connection failed")
            return False
            
    except Exception as e:
        print(f"✗ Connection test failed: {e}")
        return False

def test_schema_validation():
    """
    Test schema validation and table structure
    """
    print("="*60)
    print("TESTING SCHEMA VALIDATION")
    print("="*60)
    
    try:
        writer = DatabaseWriter()
        
        # Check if table exists
        if writer.schema_manager.table_exists():
            print("Table exists - validating schema...")
            validation_result = writer.schema_manager.validate_table_schema()
            
            if validation_result['valid']:
                print("✓ Schema validation passed")
            else:
                print(" Schema validation issues found:")
                if validation_result.get('missing_columns'):
                    print(f"  - Missing columns: {validation_result['missing_columns']}")
                if validation_result.get('extra_columns'):
                    print(f"  - Extra columns: {validation_result['extra_columns']}")
                if validation_result.get('schema_differences'):
                    print(f"  - Schema differences: {validation_result['schema_differences']}")
        else:
            print(" Table does not exist - schema will be created on first run")
            
        return True
        
    except Exception as e:
        print(f" Schema validation failed: {e}")
        return False

if __name__ == "__main__":
    # Configure logging to see detailed output
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Run tests
    print("Choose test mode:")
    print("1. Full database writer test (writes data)")
    print("2. Connection test only")
    print("3. Schema validation test")
    print("4. Run all tests")
    
    choice = input("\nEnter choice (1-4): ").strip()
    
    if choice == "1":
        test_database_writer()
    elif choice == "2":
        test_database_connection_only()
    elif choice == "3":
        test_schema_validation()
    elif choice == "4":
        print("\nRunning all tests...")
        print("\n1. Connection Test:")
        test_database_connection_only()
        print("\n2. Schema Validation Test:")
        test_schema_validation()
        print("\n3. Full Database Writer Test:")
        test_database_writer()
    else:
        print("Invalid choice. Running full test by default...")
        test_database_writer()
