"""
Setup Database Dependencies
Installs and configures all required dependencies for the MySQL database system
"""

import subprocess
import sys
import os
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def run_command(command: str, description: str) -> bool:
    """
    Run a system command and log the result
    
    Args:
        command: Command to run
        description: Description of what the command does
        
    Returns:
        True if successful, False otherwise
    """
    logger.info(f"Running: {description}")
    logger.info(f"Command: {command}")
    
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        
        if result.returncode == 0:
            logger.info(f"Success: {description}")
            if result.stdout:
                logger.info(f"Output: {result.stdout}")
            return True
        else:
            logger.error(f"Failed: {description}")
            logger.error(f"Error: {result.stderr}")
            return False
            
    except Exception as e:
        logger.error(f"Exception running command: {e}")
        return False


def install_python_dependencies():
    """Install required Python packages"""
    logger.info("Installing Python dependencies...")
    
    packages = [
        "pymysql",
        "boto3",
        "pandas",
        "openpyxl",
        "xlsxwriter",
        "pyyaml",
        "requests"
    ]
    
    success = True
    for package in packages:
        if not run_command(f"{sys.executable} -m pip install {package}", f"Installing {package}"):
            success = False
    
    return success


def create_sample_config():
    """Create sample configuration files"""
    logger.info("Creating sample configuration files...")
    
    # Sample config.yaml for database
    config_yaml = """
# Database configuration for PMG Issue AI Pipeline
database:
  enabled: true
  type: "mysql"
  host: "your-rds-endpoint.region.rds.amazonaws.com"
  port: 3306
  database: "pmg_issue_ai"
  username: "pmg_user"
  password: "your_secure_password"
  table_name: "ml_results"
  batch_size: 1000
  update_existing: true
  connection_params:
    charset: "utf8mb4"
    connect_timeout: 30
    autocommit: true

# Post-processing configuration
post_processing:
  validate_all_inputs: true
  generate_summary_report: true
  excel_formatting:
    auto_adjust_columns: true
    max_column_width: 50
    header_formatting: true

# AWS configuration (optional - will auto-discover if available)
aws:
  region: "us-east-1"
  # Will use IAM role if available, otherwise specify credentials
  # access_key_id: "your_access_key"
  # secret_access_key: "your_secret_key"

# Environment configuration
environment:
  current: "production"
  debug_mode: false

# Error handling
error_handling:
  retry_failed_operations: true
  max_retries: 3
  continue_on_errors: true
"""
    
    try:
        with open("database_config_sample.yaml", "w") as f:
            f.write(config_yaml)
        logger.info("Created database_config_sample.yaml")
        
        # Create .env sample
        env_sample = """
# Database Configuration
DB_HOST=your-rds-endpoint.region.rds.amazonaws.com
DB_PORT=3306
DB_NAME=pmg_issue_ai
DB_USER=pmg_user
DB_PASSWORD=your_secure_password

# AWS Configuration (optional)
AWS_DEFAULT_REGION=us-east-1
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
"""
        
        with open(".env.sample", "w") as f:
            f.write(env_sample)
        logger.info("Created .env.sample")
        
        return True
        
    except Exception as e:
        logger.error(f"Error creating config files: {e}")
        return False


def check_aws_credentials():
    """Check if AWS credentials are available"""
    logger.info("Checking AWS credentials...")
    
    try:
        import boto3
        session = boto3.Session()
        credentials = session.get_credentials()
        
        if credentials:
            logger.info("AWS credentials found")
            
            # Test RDS access
            rds_client = session.client('rds')
            try:
                response = rds_client.describe_db_instances()
                logger.info(f"RDS access confirmed. Found {len(response['DBInstances'])} RDS instances")
                return True
            except Exception as e:
                logger.warning(f"RDS access limited: {e}")
                return False
        else:
            logger.warning("AWS credentials not found")
            return False
            
    except ImportError:
        logger.error("boto3 not installed")
        return False
    except Exception as e:
        logger.error(f"Error checking AWS credentials: {e}")
        return False


def test_mysql_connection():
    """Test MySQL connection capabilities"""
    logger.info("Testing MySQL connection capabilities...")
    
    try:
        import pymysql
        logger.info("PyMySQL module available")
        
        # Test basic connection (will fail without real credentials, but confirms module works)
        try:
            connection = pymysql.connect(
                host='localhost',
                port=3306,
                user='test',
                password='test',
                database='test',
                connect_timeout=1
            )
            connection.close()
        except pymysql.Error:
            logger.info("PyMySQL connection test completed (expected to fail without real database)")
            
        return True
        
    except ImportError:
        logger.error("PyMySQL not available")
        return False
    except Exception as e:
        logger.error(f"Error testing MySQL connection: {e}")
        return False


def setup_directory_structure():
    """Create required directory structure"""
    logger.info("Setting up directory structure...")
    
    directories = [
        "logs",
        "data/outputs/final_results",
        "config"
    ]
    
    try:
        for directory in directories:
            Path(directory).mkdir(parents=True, exist_ok=True)
            logger.info(f"Created directory: {directory}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error creating directories: {e}")
        return False


def print_setup_instructions():
    """Print setup instructions for the user"""
    instructions = """
================================================================================
PMG Issue AI Database Setup Complete
================================================================================

NEXT STEPS:

1. CONFIGURE DATABASE:
   - Edit database_config_sample.yaml with your actual database credentials
   - Or set environment variables using .env.sample as template

2. AWS RDS MYSQL SETUP (if using AWS):
   
   a) Create RDS MySQL Instance:
      - Go to AWS RDS Console
      - Create MySQL 8.0 instance
      - Choose appropriate instance class (db.t3.micro for testing)
      - Configure security groups to allow EC2 access
   
   b) Database Setup:
      mysql -h your-rds-endpoint.region.rds.amazonaws.com -u admin -p
      CREATE DATABASE pmg_issue_ai;
      CREATE USER 'pmg_user'@'%' IDENTIFIED BY 'your_secure_password';
      GRANT ALL PRIVILEGES ON pmg_issue_ai.* TO 'pmg_user'@'%';
      FLUSH PRIVILEGES;

3. VERIFY SETUP:
   python3 -c "from aws_database_discovery import list_available_mysql_instances; print(list_available_mysql_instances())"

4. TEST DATABASE CONNECTION:
   python3 mysql_database_writer.py

5. USAGE:
   - Run post_processing.py first to combine ML outputs
   - Then run mysql_database_writer.py to write to database

SECURITY NOTES:
- Use strong passwords for database users
- Configure RDS security groups properly
- Consider using IAM database authentication
- Enable SSL/TLS for database connections

FILES CREATED:
- database_config_sample.yaml (configuration template)
- .env.sample (environment variables template)
- Required directories under data/ and logs/

================================================================================
"""
    
    print(instructions)


def main():
    """Main setup function"""
    logger.info("Starting PMG Issue AI Database Setup")
    logger.info("="*60)
    
    success = True
    
    # Install Python dependencies
    if not install_python_dependencies():
        logger.error("Failed to install Python dependencies")
        success = False
    
    # Setup directory structure
    if not setup_directory_structure():
        logger.error("Failed to setup directory structure")
        success = False
    
    # Create sample configuration
    if not create_sample_config():
        logger.error("Failed to create sample configuration")
        success = False
    
    # Test capabilities
    check_aws_credentials()
    test_mysql_connection()
    
    if success:
        logger.info("Setup completed successfully!")
        print_setup_instructions()
    else:
        logger.error("Setup completed with errors. Please check the logs above.")
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())