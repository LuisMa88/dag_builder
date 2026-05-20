"""
Example: Using DbtTransformer to run dbt transformations.

This example demonstrates how to use the DbtTransformer class with different configurations.
"""

import os
from dag_builder.pipeline import DbtTransformer


def example_basic_run():
    """Example 1: Basic dbt run using current working directory."""
    print("=== Example 1: Basic dbt run ===\n")
    
    # Create DbtTransformer without explicit config
    # It will use the current working directory as the dbt project directory
    transformer = DbtTransformer()
    
    try:
        # Run dbt compile
        result = transformer.run_compile()
        
        print(f"✓ dbt compile succeeded!")
        print(f"  Project dir: {result['dbt_project_dir']}")
        print(f"  Return code: {result['return_code']}\n")
        
    except Exception as e:
        print(f"✗ Error: {e}\n")


def example_with_project_dir():
    """Example 2: Specify custom dbt project directory."""
    print("=== Example 2: Custom project directory ===\n")
    
    project_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "transformations"))
    dbt_config = {
        "project_dir": project_dir,
        "target": "dev",
        "threads": 4
    }
    
    transformer = DbtTransformer(dbt_config)
    
    try:
        # Run dbt build (compile + run + test)
        result = transformer.run_build()
        
        print(f"✓ dbt build succeeded!")
        print(f"  Project dir: {result['dbt_project_dir']}")
        print(f"  Return code: {result['return_code']}\n")
        
    except Exception as e:
        print(e)
        print(f"✗ Error: {e}\n")


def example_with_model_selection():
    """Example 3: Select specific models to run."""
    print("=== Example 3: Model selection ===\n")
    
    dbt_config = {
        "project_dir": "/path/to/my/dbt/project",
        "select": "+tag:daily",  # Select models tagged with 'daily'
        "threads": 8
    }
    
    transformer = DbtTransformer(dbt_config)
    
    try:
        # Run dbt run with specific model selection
        result = transformer.run("run")
        
        print(f"✓ dbt run succeeded!")
        print(f"  Models selected: tag:daily")
        print(f"  Return code: {result['return_code']}\n")
        
    except Exception as e:
        print(f"✗ Error: {e}\n")


def example_with_variables():
    """Example 4: Pass variables to dbt."""
    print("=== Example 4: dbt variables ===\n")
    
    dbt_config = {
        "project_dir": "/path/to/my/dbt/project",
        "vars": {
            "start_date": "2024-01-01",
            "end_date": "2024-12-31",
            "environment": "production"
        },
        "target": "prod"
    }
    
    transformer = DbtTransformer(dbt_config)
    
    try:
        result = transformer.run("run")
        
        print(f"✓ dbt run with variables succeeded!")
        print(f"  Variables passed: {dbt_config['vars']}")
        print(f"  Return code: {result['return_code']}\n")
        
    except Exception as e:
        print(f"✗ Error: {e}\n")


def example_run_with_working_dir_override():
    """Example 5: Override working directory for execution."""
    print("=== Example 5: Working directory override ===\n")
    
    dbt_config = {
        "project_dir": "/path/to/my/dbt/project",
    }
    
    transformer = DbtTransformer(dbt_config)
    
    try:
        # Execute in different working directory (useful for Airflow/orchestrators)
        result = transformer.run(
            command="test",
            working_dir="/some/other/directory"
        )
        
        print(f"✓ dbt test succeeded!")
        print(f"  Project dir: {result['dbt_project_dir']}")
        print(f"  Command: {result['command']}")
        print(f"  Return code: {result['return_code']}\n")
        
    except Exception as e:
        print(f"✗ Error: {e}\n")


def example_full_pipeline():
    """Example 6: Complete pipeline with error handling."""
    print("=== Example 6: Full pipeline ===\n")
    
    dbt_config = {
        "project_dir": "/path/to/my/dbt/project",
        "target": "dev",
        "threads": 4,
        "select": "state:modified+",  # Only modified models
        "debug": False
    }
    
    transformer = DbtTransformer(dbt_config)
    
    try:
        # Step 1: Parse project
        print("Step 1: Parsing project...")
        result = transformer.run_compile()
        print(f"  ✓ Compile succeeded (code: {result['return_code']})\n")
        
        # Step 2: Run transformations
        print("Step 2: Running transformations...")
        result = transformer.run("run")
        print(f"  ✓ Run succeeded (code: {result['return_code']})\n")
        
        # Step 3: Run tests
        print("Step 3: Testing models...")
        result = transformer.run_test()
        print(f"  ✓ Tests succeeded (code: {result['return_code']})\n")
        
        print("✓ Full pipeline completed successfully!\n")
        
    except RuntimeError as e:
        print(f"✗ Pipeline failed: {e}\n")


if __name__ == "__main__":
    print("dbt Transformer Examples\n" + "=" * 50 + "\n")
    
    # Note: These examples assume dbt is installed and projects exist at specified paths
    # Uncomment the examples you want to run:
    
    # example_basic_run()
    example_with_project_dir()
    # example_with_model_selection()
    # example_with_variables()
    # example_run_with_working_dir_override()
    # example_full_pipeline()
    
    # print("\nTo use these examples:")
    # print("1. Install dbt: pip install dbt-core dbt-<adapter>")
    # print("2. Update paths to point to your actual dbt projects")
    # print("3. Uncomment the examples you want to run")
