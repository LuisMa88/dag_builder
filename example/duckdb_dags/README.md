# Example: DuckDB Data Pipelines

This folder contains examples showing how to use `dag_builder` with DuckDB as the target database.

## 📁 Files

- `example.py` – Sample Airflow DAGs using DuckDB operators
- `config.yaml` – Example configuration for GraphQL to DuckDB
- `rest_api_config.yaml` – Example configuration for REST API to DuckDB

## 🚀 DuckDB Configuration Options

### 1. In-Memory Database
```yaml
duckdb_config:
  memory: true
```

### 2. File-Based Database
```yaml
duckdb_config:
  database_path: "data.duckdb"
  read_only: false
```

### 3. Read-Only Database
```yaml
duckdb_config:
  database_path: "existing_data.duckdb"
  read_only: true
```

## 🐳 Usage Examples

### GraphQL to DuckDB
```python
from dag_builder.orchestrator import DltGraphqlToDuckDBOperator

DltGraphqlToDuckDBOperator(
    task_id="graphql_to_duckdb",
    config_path="config.yaml",
)
```

### REST API to DuckDB
```python
from dag_builder.orchestrator import DltRestApiToDuckDBOperator

DltRestApiToDuckDBOperator(
    task_id="rest_api_to_duckdb",
    config_path="rest_api_config.yaml",
)
```

## 🔧 DuckDB Target Features

- **Flexible Storage**: In-memory or file-based databases
- **Read-Only Support**: For querying existing databases
- **Path Handling**: Automatic absolute path resolution
- **Fallback Defaults**: Graceful fallback to in-memory database
- **Airflow Integration**: Optional connection-based configuration

## 📋 Setup Requirements

1. **Environment Variable**:
   ```bash
   export APP_API_TOKEN="your_api_token"
   ```

2. **Dependencies**: DuckDB support is included with dlt
3. **Airflow**: Deploy DAGs to your Airflow environment

## 🎯 Use Cases

- **Development**: In-memory databases for testing
- **Analytics**: File-based databases for persistent storage
- **ETL**: Incremental loading with DuckDB performance
- **Data Lake**: Local analytics with fast queries

## 🔄 Migration from Impala

Simply change the operator class and add `duckdb_config`:
```python
# From:
DltGraphqlToImpalaOperator(...)

# To:
DltGraphqlToDuckDBOperator(...)
```

And add the DuckDB configuration to your YAML file.
