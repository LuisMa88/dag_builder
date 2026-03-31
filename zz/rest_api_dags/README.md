# Example: REST API Data Pipeline

This folder contains examples showing how to use `dag_builder` with REST APIs.

## 📁 Files

- `example.py` – Sample Airflow DAG using `DltRestApiToImpalaOperator`
- `config.yaml` – Example configuration for REST API ingestion

## 🚀 Usage

### 1. Configuration

The REST API configuration supports these additional fields:

```yaml
# REST API specific settings
api_params:
  userId: 1
  status: "active"

api_headers:
  Content-Type: "application/json"
  User-Agent: "dag_builder/1.0"

pagination_type: "offset"  # Options: "offset", "page", "cursor"
```

### 2. Pagination Types

- **offset**: Uses `offset` and `limit` parameters (default)
- **page**: Uses `page` and `per_page` parameters
- **cursor**: Uses `cursor` and `limit` parameters

### 3. Response Formats

The fetcher automatically handles common response formats:

- Direct array: `[{"id": 1}, {"id": 2}]`
- Wrapped object: `{"data": [...], "pagination": {...}}`
- Alternative keys: `results`, `items`, `records`

### 4. Example API Endpoints

- JSONPlaceholder: `https://jsonplaceholder.typicode.com/posts`
- GitHub API: `https://api.github.com/repos/owner/repo/issues`
- Custom APIs with pagination

## 🐳 Setup

1. Set your API token:
   ```bash
   export APP_API_TOKEN="your_api_token"
   ```

2. Configure Airflow connection for Impala (`impala_conn`)

3. Deploy the DAG to your Airflow environment

## 📝 Notes

- The fetcher automatically handles incremental loading using the `incremental_cursor` field
- Authentication uses Bearer tokens via the `APP_API_TOKEN` environment variable
- Custom headers can be added via the `api_headers` configuration
- The fetcher includes safety checks to prevent infinite loops
