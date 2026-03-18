# Example Airflow DAG — dag_builder

This folder contains a small example showing how to use `dag_builder` in an Airflow DAG.

## 📁 Files

- `example.py` – A sample Airflow DAG that uses `DltGraphqlToImpalaOperator`.
- `config.yaml` – An example pipeline configuration file the operator reads.

## ▶️ How to run the example

1. Install the package in a virtual environment:

   ```bash
   pip install -e .
   ```

2. Ensure you have an Airflow environment configured, including an Airflow connection for Impala.
   - The example uses a connection id: `impala_conn`

3. Set your GraphQL API token:

   ```bash
   export APP_API_TOKEN="<your_token>"
   ```

   (Windows PowerShell: `$env:APP_API_TOKEN = "<your_token>"`)

4. Deploy the DAG to Airflow (copy `example.py` into your DAGs folder).

5. Optionally, update `config.yaml` with your real GraphQL endpoint and query.

---

## ⚙️ Notes

- The operator expects the GraphQL response to include a `nodes` list and `pageInfo` object.
- If you want to point to a different config file, update `config_path` in `example.py` or set `PIPELINE_CONFIG_PATH`.
