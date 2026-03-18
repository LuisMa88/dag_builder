# 🛠️ dag_builder

A modular framework to generate **Apache Airflow** DAGs that move data from **GraphQL APIs** to **Cloudera Impala** using **dlt**.

This project provides an Airflow `BaseOperator` that:

- Reads configuration from a **YAML file**
- Fetches incremental data from a **GraphQL API**
- Loads the results into **Cloudera Impala** using **dlt** and SQLAlchemy

---

## 🚀 Quick Start

### 1) Install the Package

```bash
pip install .
```

> ⚠️ This package depends on Airflow and dlt. Install in a compatible environment (e.g., a dedicated virtualenv).

### 2) Create a config YAML

Create a `config.yaml` file (or point to your own path via `PIPELINE_CONFIG_PATH`) with the following required fields:

```yaml
# config.yaml

dag_id: "my_pipeline"
airflow_conn_id: "impala_conn"
api_url: "https://my-graphql-api.example.com/graphql"
table_name: "my_table"
graphql_query: |
  query($cursor: String, $since: DateTime!) {
    analytics(cursor: $cursor, since: $since) {
      nodes {
        id
        updated_at
        ...
      }
      pageInfo {
        hasNextPage
        endCursor
      }
    }
  }
```

> The code expects the GraphQL response to contain a `nodes` list and a `pageInfo` object.

### 3) Set your API token

The pipeline loads the API token from the environment variable `APP_API_TOKEN`:

```bash
export APP_API_TOKEN="<your_token>"
```

(On Windows PowerShell: `setx APP_API_TOKEN "<your_token>"` or `$env:APP_API_TOKEN = "<your_token>"`)

---

## 🧩 Configuration

### Config file location

The operator looks for configuration in this order:

1. The path passed to the operator via `config_path`
2. The `PIPELINE_CONFIG_PATH` environment variable
3. A default `config.yaml` located next to the package code (`dag_builder/config.yaml`)

### Config fields

Required fields (validated via Pydantic):

- `dag_id` – Airflow DAG id and dlt pipeline name
- `airflow_conn_id` – Airflow connection id for Impala
- `api_url` – The GraphQL endpoint
- `table_name` – The target table name (used for the dlt resource)
- `graphql_query` – The GraphQL query string

Optional fields (with defaults):

- `task_id` – defaults to `ingest_api_data`
- `schedule` – defaults to `@daily`
- `start_date` – defaults to `2024-01-01`
- `catchup` – defaults to `false`
- `incremental_cursor` – defaults to `updated_at`

---

## ✅ Usage (Airflow DAG)

Use the operator in your Airflow DAG like this:

```python
from datetime import datetime

from airflow import DAG
from dag_builder.orchestrator import DltGraphqlToImpalaOperator

with DAG(
    dag_id="marketing_data_ingest",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
) as dag:

    DltGraphqlToImpalaOperator(
        task_id="run_ingestion",
        config_path="/path/to/config.yaml",
    )
```

> 🔧 Ensure your Airflow connection (e.g., `impala_conn`) is configured and points at your Impala host.

---

## 🧪 Running Tests

```bash
pytest
```

---

## 📦 Project Layout

- `dag_builder/` – core package code
- `example/` – sample Airflow DAG
- `tests/` – unit tests

---

## 📄 License

This project does not include a license file. If you plan to share it, add a `LICENSE` file and update this section accordingly.
