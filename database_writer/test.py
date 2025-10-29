"""
Quick Database Connection Test
Tests MySQL RDS connection with your credentials
"""

import pymysql
import sys

# Your database configuration
# DB_CONFIG = {
#     'host': 'gati-shakti-stag-mysql.chsie6404k54.ap-south-1.rds.amazonaws.com',
#     'port': 3306,
#     'user': 'pmg_user_varshit',
#     'password': '57bfdO0{e2g1de7',
#     'database': 'gatisakti-stag-db',
#     'charset': 'utf8mb4',
#     # 'connect_timeout': 30,
#     'autocommit': True
# }

DB_CONFIG = {
    'host': 'app-m5large-db-instance.chmgaq2c6rvl.ap-south-1.rds.amazonaws.com/',
    'port': 3306,
    'user': 'admin',
    'password': ']Xr<iuW4He<)[-ZN_3vFp<T:Q7$N"}',
    'database': 'pmg_issue_ai',
    'charset': 'utf8mb4',
    'connect_timeout': 30,
    'autocommit': True
}


def test_connection():
    """Test database connection"""
    print("Testing MySQL RDS Connection...")
    print("="*50)
    
    try:
        # Test connection
        connection = pymysql.connect(**DB_CONFIG)
        print("Connection successful!")
        
        # Test basic query
        cursor = connection.cursor()
        cursor.execute("SELECT 1 as test")
        result = cursor.fetchone()
        print(f"Query test successful: {result}")
        
        # Check database
        cursor.execute("SELECT DATABASE()")
        db_name = cursor.fetchone()
        print(f"Connected to database: {db_name[0]}")
        
        # Check if table exists
        table_name = "ml_results"
        cursor.execute("""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_schema = DATABASE() 
            AND table_name = %s
        """, (table_name,))
        
        table_exists = cursor.fetchone()[0] > 0
        if table_exists:
            print(f" Table '{table_name}' already exists")
            
            # Get row count
            cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
            row_count = cursor.fetchone()[0]
            print(f" Current rows in table: {row_count}")
        else:
            print(f" Table '{table_name}' does not exist (will be created)")
        
        # Test permissions
        cursor.execute("SHOW GRANTS FOR CURRENT_USER()")
        grants = cursor.fetchall()
        print(f" User permissions: {len(grants)} grants found")
        
        connection.close()
        
        print("\n" + "="*50)
        print(" DATABASE CONNECTION TEST PASSED!")
        print("You can now run the full pipeline:")
        print("1. python post_processing.py")
        print("2. python mysql_database_writer.py")
        
        return True
        
    except Exception as e:
        print(f" Connection failed: {e}")
        print("\nPossible issues:")
        print("- Check if your IP is whitelisted in RDS security group")
        print("- Verify username/password are correct")
        print("- Ensure RDS instance is running")
        print("- Check network connectivity")
        
        return False

if __name__ == "__main__":
    success = test_connection()
    sys.exit(0 if success else 1)