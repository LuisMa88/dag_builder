# 🛠️ dag_builder

A Python package for building data pipelines that move data from **REST APIs** to **DuckDB** using **dlt**.

This package provides:

- Reads configuration from a **YAML file**
- Fetches incremental data from **REST APIs**
- Loads the results into **DuckDB** using **dlt**

---

## 🚀 Quick Start

### 1) Install the Package

```bash
pip install .
```

> ⚠️ This package depends on dlt. Install in a compatible environment (e.g., a dedicated virtualenv).

### 2) Create a config YAML

Create a `config.yaml` file with the following fields:

```yaml
# config.yaml

pipeline_name: "my_pipeline"
api_url: "https://jsonplaceholder.typicode.com/posts"
table_name: "posts"
incremental_cursor: "id"

# Optional: DuckDB configuration
duckdb_config:
  database_path: "./posts_data.duckdb"
```

### 3) Set your API token

The pipeline loads the API token from the environment variable `APP_API_TOKEN`:

```bash
export APP_API_TOKEN="<your_token>"
```

(On Windows PowerShell: `setx APP_API_TOKEN "<your_token>"` or `$env:APP_API_TOKEN = "<your_token>"`)

---

## 🧩 Configuration

### Config fields

Required fields (validated via Pydantic):

- `pipeline_name` – Pipeline name for dlt
- `api_url` – The REST API endpoint
- `table_name` – The target table name (used for the dlt resource)

Optional fields (with defaults):

- `task_id` – defaults to `ingest_api_data`
- `incremental_cursor` – defaults to `updated_at`
- `duckdb_config` – DuckDB database configuration

---

## ✅ Usage (Python Scripts)

Use the package in your Python scripts like this:

```python
import os
from dag_builder import DataPipeline, run_pipeline

# Set environment variable for API token
os.environ['APP_API_TOKEN'] = 'your_token_here'

# Option 1: Use the DataPipeline class
pipeline = DataPipeline('config.yaml')
result = pipeline.run_rest_api_to_duckdb()
print(f"Pipeline completed: {result}")

# Option 2: Use the convenience function
result = run_pipeline('config.yaml', 'rest_api_to_duckdb')
print(f"Pipeline completed: {result}")
```

### Example Usage

See the `examples/` directory for complete working examples:

- `simple_rest_api_example.py` – Basic usage
- `advanced_usage_example.py` – Advanced patterns and custom configurations

---

## 🧪 Running Tests

```bash
pytest
```

---

## 📦 Project Layout

- `dag_builder/` - core package code
- `examples/` - Python usage examples
- `example/` - configuration files

---

## 📄 License

This project does not include a license file. If you plan to share it, add a `LICENSE` file and update this section accordingly.
