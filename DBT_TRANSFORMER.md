# dbt Transformer Documentation

## Overview

The `DbtTransformer` class provides a Python interface to run dbt (data build tool) transformations with configurable project locations. It's designed to integrate seamlessly with the dag_builder pipeline orchestration system.

## Features

- ✅ **Configurable Project Location**: Specify dbt project directory via config or use current working directory as default
- ✅ **Subprocess-based Execution**: Uses Python's `subprocess` module to run dbt commands
- ✅ **Flexible Model Selection**: Support for model selection (`--select`) and exclusion (`--exclude`)
- ✅ **Variable Passing**: Pass dbt variables programmatically
- ✅ **Multi-threaded Execution**: Control parallelism with thread count
- ✅ **JSON Output Parsing**: Parse dbt command output for structured results
- ✅ **Working Directory Management**: Change directories during execution with automatic restoration
- ✅ **Comprehensive Logging**: Integrated with dag_builder's logging system
- ✅ **Error Handling**: Validation and helpful error messages

## Installation

Ensure dbt is installed:

```bash
pip install dbt-core dbt-postgres  # or dbt-snowflake, dbt-bigquery, etc.
```

## Configuration

### DbtSchema (Pydantic Model)

The `DbtSchema` class defines all available configuration options:

```python
class DbtSchema(BaseModel):
    project_dir: Optional[str] = None       # Path to dbt project (default: current working directory)
    profiles_dir: Optional[str] = None      # Path to dbt profiles directory
    select: Optional[str] = None            # Model selection (e.g. "+tag:daily", "my_model")
    exclude: Optional[str] = None           # Models to exclude
    target: Optional[str] = None            # dbt target to use from profiles.yml
    threads: Optional[int] = 1              # Number of threads for parallel execution
    vars: Optional[Dict[str, Any]] = {}     # dbt variables to pass
    debug: Optional[bool] = False           # Enable debug mode
```

### Configuration Examples

#### Example 1: Basic Configuration (Uses Current Directory)

```python
from dag_builder.pipeline import DbtTransformer

# Uses current working directory as project_dir
transformer = DbtTransformer()
result = transformer.run("run")
```

#### Example 2: Explicit Project Directory

```python
dbt_config = {
    "project_dir": "/path/to/my/dbt/project",
    "target": "dev",
    "threads": 4
}
transformer = DbtTransformer(dbt_config)
result = transformer.run("run")
```

#### Example 3: With Model Selection

```python
dbt_config = {
    "project_dir": "/path/to/my/dbt/project",
    "select": "+tag:daily",  # Select models tagged 'daily' and their dependents
    "exclude": "tag:deprecated",
    "threads": 8
}
transformer = DbtTransformer(dbt_config)
result = transformer.run("run")
```

#### Example 4: With Variables

```python
dbt_config = {
    "project_dir": "/path/to/my/dbt/project",
    "vars": {
        "start_date": "2024-01-01",
        "end_date": "2024-12-31",
        "environment": "production"
    }
}
transformer = DbtTransformer(dbt_config)
result = transformer.run("run")
```

## Usage

### Basic Commands

```python
from dag_builder.pipeline import DbtTransformer

transformer = DbtTransformer({
    "project_dir": "/path/to/dbt/project",
    "target": "dev"
})

# Compile (parse project)
result = transformer.run_compile()

# Run transformations
result = transformer.run("run")

# Test models
result = transformer.run_test()

# Build (compile + run + test)
result = transformer.run_build()

# Snapshot
result = transformer.run_snapshot()

# Custom command
result = transformer.run("snapshot")
```

### Return Value

All `run()` methods return a dictionary:

```python
{
    'success': bool,                    # True if command succeeded
    'return_code': int,                 # Exit code (0 = success)
    'stdout': str,                      # Command output
    'stderr': str,                      # Error output (if any)
    'dbt_project_dir': str,             # Project directory used
    'command': str,                     # Command executed
    'dbt_metadata': dict                # Parsed JSON output from dbt
}
```

### Error Handling

```python
from dag_builder.pipeline import DbtTransformer

transformer = DbtTransformer({"project_dir": "/path/to/project"})

try:
    result = transformer.run("run")
    if result['success']:
        print(f"✓ dbt run succeeded")
    else:
        print(f"✗ dbt run failed: {result['stderr']}")
except FileNotFoundError as e:
    print(f"Project directory not found: {e}")
except ValueError as e:
    print(f"Invalid configuration: {e}")
except RuntimeError as e:
    print(f"dbt command failed: {e}")
```

## Working Directory Management

The `DbtTransformer` handles working directory changes automatically:

```python
# Run in different working directory (useful for Airflow/orchestrators)
transformer = DbtTransformer({
    "project_dir": "/path/to/dbt/project"
})

# This changes to /some/directory, runs dbt there, then restores
result = transformer.run("run", working_dir="/some/directory")
```

## Integration with dag_builder

### As Part of a Larger Pipeline

```python
from dag_builder.pipeline import DataPipeline, DbtTransformer

# Step 1: Ingest data via REST API to DuckDB
api_pipeline = DataPipeline("/path/to/config.yaml")
load_result = api_pipeline.run_rest_api_to_duckdb()

# Step 2: Transform data with dbt
dbt_config = {
    "project_dir": "/path/to/dbt/project",
    "target": "dev"
}
transformer = DbtTransformer(dbt_config)
dbt_result = transformer.run("run")

if dbt_result['success']:
    print(f"✓ Pipeline completed successfully")
else:
    print(f"✗ Pipeline failed at dbt step")
```

## Using with Airflow

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from dag_builder.pipeline import DbtTransformer

def run_dbt_transform():
    transformer = DbtTransformer({
        "project_dir": "/path/to/dbt/project",
        "target": "prod"
    })
    result = transformer.run("build")
    if not result['success']:
        raise RuntimeError(f"dbt build failed: {result['stderr']}")

dag = DAG('dbt_pipeline', schedule_interval='@daily')

dbt_task = PythonOperator(
    task_id='run_dbt',
    python_callable=run_dbt_transform,
    dag=dag
)
```

## Configuration via YAML (Future)

You can also load dbt configuration from a YAML file and integrate it with PipelineConfig:

```yaml
# config.yaml
pipeline_name: my_etl
api_url: https://api.example.com/data
table_name: raw_data

dbt:
  project_dir: /path/to/dbt/project
  target: dev
  threads: 4
  vars:
    environment: production
```

## Default Behavior

- **No `project_dir` specified**: Uses current working directory (`os.getcwd()`)
- **No `threads` specified**: Defaults to 1 (single-threaded)
- **No `target` specified**: Uses dbt's default target from profiles.yml
- **JSON output**: Automatically enabled for structured result parsing

## dbt Model Selection Syntax

Common selection patterns:

```python
# Select by tag
"select": "tag:daily"                       # Models with tag 'daily'
"select": "+tag:daily"                      # Models and dependents
"select": "tag:daily,tag:weekly"            # Multiple tags (OR)

# Select by path
"select": "path/to/model"                   # Specific model path
"select": "models/staging/*"                # Wildcard selection

# Select by state
"select": "state:modified+"                 # Modified models and dependents

# Combine selections
"select": "+tag:daily state:modified+"      # AND combination
"exclude": "tag:deprecated"                 # Exclude specific models
```

## Troubleshooting

### Issue: "dbt command not found"
**Solution**: Ensure dbt is installed and in your PATH
```bash
pip install dbt-core dbt-postgres
```

### Issue: "dbt project directory not found"
**Solution**: Verify the `project_dir` path exists and contains dbt_project.yml
```bash
ls /path/to/project/dbt_project.yml
```

### Issue: "target not found in profiles.yml"
**Solution**: Check that the specified `target` exists in your dbt profiles.yml

### Issue: dbt command takes too long
**Solution**: Increase the `threads` value to enable parallel execution
```python
dbt_config = {"project_dir": "...", "threads": 8}
```

## Advanced Usage

### Custom dbt Commands

```python
# Run any valid dbt command
transformer = DbtTransformer({"project_dir": "/path/to/project"})
result = transformer.run("docs generate")
result = transformer.run("freshness")
result = transformer.run("seed")
```

### Debug Mode

```python
dbt_config = {
    "project_dir": "/path/to/project",
    "debug": True  # Enables --debug flag
}
transformer = DbtTransformer(dbt_config)
result = transformer.run("run")
```

### Accessing dbt Output

```python
result = transformer.run("run")

# View raw stdout
print(result['stdout'])

# View errors
print(result['stderr'])

# Access parsed dbt metadata
print(result['dbt_metadata'])

# Check if successful
if result['success']:
    print("✓ Complete!")
```

## See Also

- [dbt Documentation](https://docs.getdbt.com/)
- [dbt CLI Commands](https://docs.getdbt.com/reference/commands)
- [dbt Model Selection](https://docs.getdbt.com/reference/node-selection)
