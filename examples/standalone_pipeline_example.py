"""
Standalone pipeline example - Run without Airflow dependencies.

This script demonstrates how to run the dag_builder pipeline 
directly from a Python script without requiring Airflow.
"""

import os
from dag_builder.pipeline import DataPipeline, DbtTransformer


def run_rest_api_pipeline():
    """Run a REST API to DuckDB pipeline standalone."""
    
    # Path to your configuration file
    config_path = os.path.join(
        os.path.dirname(__file__),
        '../example/rest_api_dags/rest_api_config.yaml'
    )
    
    if not os.path.exists(config_path):
        print(f"Configuration file not found at {config_path}")
        return
    
    print("=" * 60)
    print("Running Standalone REST API to DuckDB Pipeline")
    print("=" * 60)
    
    try:
        # Initialize pipeline with your config
        pipeline = DataPipeline(config_path=config_path)
        
        # Run the pipeline
        result = pipeline.run_rest_api_to_duckdb()
        
        print("\n✓ Pipeline completed successfully!")
        print(f"  Pipeline: {result.get('pipeline_name')}")
        print(f"  Table: {result.get('table_name')}")
        if result.get('destination_name'):
            print(f"  Database: {result.get('destination_name')}")
        
        return result
        
    except Exception as e:
        print(f"\n✗ Pipeline failed: {e}")
        raise


def run_dbt_transformation(project_dir=None):
    """Run dbt transformations standalone."""
    
    print("\n" + "=" * 60)
    print("Running Standalone dbt Transformation")
    print("=" * 60)
    
    try:
        # Initialize dbt transformer
        dbt_config = {
            'project_dir': project_dir or os.getcwd(),
            # 'profiles_dir': '/path/to/profiles',  # Optional
        }
        
        transformer = DbtTransformer(dbt_config=dbt_config)
        
        # Run dbt
        result = transformer.run(command="run")
        
        print("\n✓ dbt transformation completed successfully!")
        print(f"  Project: {result.get('project_dir')}")
        
        return result
        
    except Exception as e:
        print(f"\n✗ dbt transformation failed: {e}")
        raise


if __name__ == "__main__":
    # Example 1: Run REST API pipeline
    pipeline_result = run_rest_api_pipeline()
    
    # Example 2: Run dbt transformation (optional)
    # Uncomment below if you have a dbt project
    # transformer_result = run_dbt_transformation()
    
    print("\n" + "=" * 60)
    print("All pipelines completed!")
    print("=" * 60)
