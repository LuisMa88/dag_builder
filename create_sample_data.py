#!/usr/bin/env python3
"""Create sample DuckDB database with test data"""

import duckdb
import json

def create_sample_database():
    # Create sample data
    sample_posts = [
        {"id": 1, "userId": 1, "title": "Sample Post 1", "body": "This is a test post"},
        {"id": 2, "userId": 1, "title": "Sample Post 2", "body": "This is another test post"},
        {"id": 3, "userId": 1, "title": "Sample Post 3", "body": "This is a third test post"}
    ]
    
    # Connect and create database
    conn = duckdb.connect('data/sample_posts.duckdb')
    
    # Drop table if exists
    conn.execute("DROP TABLE IF EXISTS posts")
    
    # Create table
    conn.execute("""
        CREATE TABLE posts (
            id INTEGER,
            userId INTEGER,
            title VARCHAR,
            body VARCHAR
        )
    """)
    
    # Insert data
    for post in sample_posts:
        conn.execute("INSERT INTO posts VALUES (?, ?, ?, ?)", 
                  [post["id"], post["userId"], post["title"], post["body"]])
    
    # Verify data
    result = conn.execute("SELECT COUNT(*) FROM posts").fetchall()
    print(f"✅ Created sample database with {result[0][0]} records")
    
    # Sample query
    sample = conn.execute("SELECT * FROM posts LIMIT 2").fetchall()
    print(f"📋 Sample data: {sample}")
    
    conn.close()
    return True

if __name__ == "__main__":
    create_sample_database()
