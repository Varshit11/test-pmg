#!/usr/bin/env python3
"""
Simple script to view contents of ml_results table
"""

import pymysql
import json

# Database connection config
DB_CONFIG = {
    'host': 'app-m5large-db-instance.chmgaq2c6rvl.ap-south-1.rds.amazonaws.com',
    'user': 'admin',
    'password': ']Xr<iuW4He<)[-ZN_3vFp<T:Q7$N', 
    'database': 'pmg_issue_ai',
    'charset': 'utf8mb4'
}

def view_table_contents():
    """View and print all contents of ml_results table"""
    try:
        # Connect to database
        connection = pymysql.connect(**DB_CONFIG)
        cursor = connection.cursor()
        
        # Get all records
        cursor.execute("SELECT * FROM ml_results")
        results = cursor.fetchall()
        
        # Get column names
        cursor.execute("DESCRIBE ml_results")
        columns = [row[0] for row in cursor.fetchall()]
        
        print(f"Found {len(results)} records in ml_results table\n")
        
        if results:
            # Print each record
            for i, row in enumerate(results, 1):
                print(f"Record {i}:")
                print("-" * 50)
                for col_name, value in zip(columns, row):
                    print(f"{col_name}: {value}")
                print()
        else:
            print("No records found in table")
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if 'connection' in locals():
            connection.close()

if __name__ == "__main__":
    view_table_contents()
