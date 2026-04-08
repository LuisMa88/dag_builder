#!/usr/bin/env python3
"""
Simple example of using dag_builder to load REST API data into DuckDB.
This demonstrates the basic usage without any Airflow dependencies.
"""

import os
import sys
from pathlib import Path

# Add the parent directory to the path so we can import dag_builder
sys.path.insert(0, str(Path(__file__).parent.parent))

from dag_builder import run_pipeline

def main():
    """Run a simple REST API to DuckDB pipeline."""
    
    print("=== Simple REST API to DuckDB Example ===")
    
    # Set up environment variables if needed
    os.environ['APP_API_TOKEN'] = 'dummy_token_for_public_api'
    
    # Configuration file path
    config_path = "example/rest_api_dags/rest_api_config.yaml"
    
    # Check if config file exists
    if not os.path.exists(config_path):
        print(f"Error: Config file not found at {config_path}")
        print("Please make sure the config file exists.")
        return
    
    try:
        # Run the pipeline
        print(f"Running pipeline with config: {config_path}")
        
        result = run_pipeline(
            config_path=config_path,
            pipeline_type="rest_api_to_duckdb",
            working_dir=os.path.expanduser("~")  # Use home directory for DLT config
        )
        
        print("=== Pipeline Results ===")
        print(f"Pipeline Name: {result['pipeline_name']}")
        print(f"Table Name: {result['table_name']}")
        
        if result.get('database_path'):
            print(f"Database Path: {result['database_path']}")
            
            # Check if database exists and has data
            if os.path.exists(result['database_path']):
                import duckdb
                conn = duckdb.connect(result['database_path'])
                
                try:
                    # Check for staging tables
                    tables = conn.execute("SHOW TABLES").fetchall()
                    print(f"Tables found: {tables}")
                    
                    if tables:
                        for table_info in tables:
                            table_name = table_info[0]
                            count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchall()
                            print(f"Records in {table_name}: {count[0][0]}")
                            
                            if count[0][0] > 0:
                                print(f"Sample data from {table_name}:")
                                results = conn.execute(f"SELECT * FROM {table_name} LIMIT 3").fetchall()
                                for i, row in enumerate(results, 1):
                                    print(f"  Row {i}: {row}")
                
                except Exception as e:
                    print(f"Error checking database: {e}")
                finally:
                    conn.close()
            else:
                print("Database file not found - check DLT logs for actual location")
        
        print(f"Load Info: {result['load_info']}")
        print("=== Pipeline completed successfully! ===")
        
    except Exception as e:
        print(f"Error running pipeline: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
