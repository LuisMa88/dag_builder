#!/usr/bin/env python3
"""Test script to verify DuckDB database functionality"""

import duckdb
import os

def test_duckdb():
    # Test database connection and data
    db_path = "data/posts_data_with_data.duckdb"
    
    if not os.path.exists(db_path):
        print(f"❌ Database file not found: {db_path}")
        return False
    
    try:
        # Connect to database
        conn = duckdb.connect(db_path)
        print(f"✅ Connected to database: {db_path}")
        
        # Check database size
        size_info = conn.execute("PRAGMA database_size").fetchall()
        print(f"📊 Database size info: {size_info}")
        
        # List all tables
        tables = conn.execute("SHOW TABLES").fetchall()
        print(f"📋 Tables found: {tables}")
        
        # If no tables, try to query directly
        if not tables:
            print("🔍 No tables found, trying direct query...")
            try:
                result = conn.execute("SELECT 1").fetchall()
                print(f"✅ Direct query successful: {result}")
            except Exception as e:
                print(f"❌ Direct query failed: {e}")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Database error: {e}")
        return False

if __name__ == "__main__":
    test_duckdb()
