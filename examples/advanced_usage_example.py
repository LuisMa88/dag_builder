#!/usr/bin/env python3
"""
Advanced example showing different ways to use dag_builder package.
This demonstrates custom configurations and different pipeline types.
"""

import os
import sys
from pathlib import Path

# Add the parent directory to the path so we can import dag_builder
# sys.path.insert(0, str(Path(__file__).parent.parent))

from dag_builder import DataPipeline, PipelineConfig, RestApiFetcher, DuckDBTarget
import dlt

def example_1_basic_pipeline():
    """Example 1: Basic pipeline using DataPipeline class."""
    print("=== Example 1: Basic Pipeline ===")
    
    # Set environment variable
    os.environ['APP_API_TOKEN'] = 'dummy_token_for_public_api'
    
    # Create pipeline instance
    pipeline = DataPipeline("example/rest_api_dags/rest_api_config.yaml")
    
    # Run the pipeline
    result = pipeline.run_rest_api_to_duckdb()
    
    print(f"Pipeline completed: {result['pipeline_name']}")
    print(f"Data loaded to table: {result['table_name']}")

# def example_2_custom_configuration():
#     """Example 2: Using custom configuration."""
#     print("\n=== Example 2: Custom Configuration ===")
    
#     # Create a custom config
#     config = PipelineConfig("example/rest_api_dags/rest_api_config.yaml")
    
#     # Create custom fetcher
#     fetcher = RestApiFetcher(
#         url="https://jsonplaceholder.typicode.com/comments",
#         token="dummy_token",
#         params={"postId": 1},  # Only get comments for post 1
#         headers={"Accept": "application/json"},
#         pagination_type="offset"
#     )
    
#     # Create custom DuckDB target
#     target = DuckDBTarget(database_path="./custom_comments.duckdb")
    
#     # Create DLT pipeline manually
#     pipeline = dlt.pipeline(
#         pipeline_name="custom_comments_pipeline",
#         destination="duckdb",
#         dataset_name="comments"
#     )
    
#     # Create resource
#     resource = dlt.resource(
#         fetcher.fetch_records(dlt.sources.incremental("id")),
#         name="comments",
#         write_disposition="merge",
#         primary_key="id"
#     )
    
#     # Run pipeline
#     load_info = pipeline.run(resource)
#     print(f"Custom pipeline completed: {load_info}")
    
#     # Check results
#     if os.path.exists("./custom_comments.duckdb"):
#         import duckdb
#         conn = duckdb.connect("./custom_comments.duckdb")
#         count = conn.execute("SELECT COUNT(*) FROM comments.comments").fetchall()
#         print(f"Loaded {count[0][0]} comments")
#         conn.close()

# def example_3_programmatic_config():
#     """Example 3: Creating configuration programmatically."""
#     print("\n=== Example 3: Programmatic Configuration ===")
    
#     # Create a simple configuration dictionary
#     config_data = {
#         'pipeline_name': 'programmatic_pipeline',
#         'api_url': 'https://jsonplaceholder.typicode.com/todos',
#         'table_name': 'todos',
#         'incremental_cursor': 'id',
#         'duckdb_config': {
#             'database_path': './todos.duckdb'
#         }
#     }
    
#     # You could save this to a YAML file and use it
#     import yaml
#     config_path = './temp_config.yaml'
#     with open(config_path, 'w') as f:
#         yaml.dump(config_data, f)
    
#     try:
#         # Use the temporary config
#         pipeline = DataPipeline(config_path)
#         result = pipeline.run_rest_api_to_duckdb()
        
#         print(f"Programmatic pipeline completed: {result['pipeline_name']}")
        
#         # Show some data
#         if os.path.exists('./todos.duckdb'):
#             import duckdb
#             conn = duckdb.connect('./todos.duckdb')
#             sample = conn.execute("SELECT * FROM staging.todos LIMIT 3").fetchall()
#             print("Sample todos:")
#             for i, row in enumerate(sample, 1):
#                 print(f"  {i}: {row}")
#             conn.close()
    
#     finally:
#         # Clean up temporary config
#         if os.path.exists(config_path):
#             os.remove(config_path)

# def example_4_multiple_sources():
#     """Example 4: Loading data from multiple sources."""
#     print("\n=== Example 4: Multiple Sources ===")
    
#     # Create a pipeline that loads from multiple endpoints
#     sources = [
#         {
#             'name': 'posts',
#             'url': 'https://jsonplaceholder.typicode.com/posts',
#             'primary_key': 'id'
#         },
#         {
#             'name': 'users', 
#             'url': 'https://jsonplaceholder.typicode.com/users',
#             'primary_key': 'id'
#         }
#     ]
    
#     for source in sources:
#         print(f"Loading {source['name']}...")
        
#         # Create fetcher for each source
#         fetcher = RestApiFetcher(
#             url=source['url'],
#             token='dummy_token',
#             params={},
#             headers={},
#             pagination_type='offset'
#         )
        
#         # Create pipeline
#         pipeline = dlt.pipeline(
#             pipeline_name=f"multi_source_{source['name']}",
#             destination='duckdb',
#             dataset_name=source['name']
#         )
        
#         # Create resource
#         resource = dlt.resource(
#             fetcher.fetch_records(dlt.sources.incremental(source['primary_key'])),
#             name=source['name'],
#             write_disposition='merge',
#             primary_key=source['primary_key']
#         )
        
#         # Run
#         load_info = pipeline.run(resource)
#         print(f"  {source['name']}: {load_info}")

def main():
    """Run all examples."""
    print("Running dag_builder examples...")
    
    # Set environment variable for all examples
    os.environ['APP_API_TOKEN'] = 'dummy_token_for_public_api'
    
    try:
        example_1_basic_pipeline()
        # example_2_custom_configuration()
        # example_3_programmatic_config()
        # example_4_multiple_sources()
        
        print("\n=== All examples completed! ===")
        
    except Exception as e:
        print(f"Error running examples: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
