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

## 🐳 Docker Compose Quickstart (Impala + Airflow)

This example includes a Docker Compose stack that launches Impala QuickStart services and Airflow.

1. Ensure `quickstart-network` exists:

   ```powershell
   docker network inspect quickstart-network 2>$null || docker network create quickstart-network
   ```

2. Dot-source `env.ps1` in the same shell:

   ```powershell
   cd .\example\graphql2impala
   . .\env.ps1
   ```

3. Start the stack:

   ```powershell
   docker compose up --build
   ```

4. Initialize Airflow DB (run once):

   ```powershell
   docker compose exec airflow-webserver airflow db init
   docker compose restart airflow-webserver airflow-scheduler
   ```

5. If Impala images missing, build locally instead of pulling:

   ```powershell
   docker compose build --parallel
   docker compose up --build
   ```

6. If `catalogd`/`impalad-1` fail with JDO javax/jdo/JDOException, ensure Impala quickstart images are built and use the exact image names from this project.

   ```powershell
   # Check existing quickstart images:
   docker images --format "{{.Repository}}:{{.Tag}}" | Select-String "impala_quickstart|81d5377c2"

   # If missing, build from upstream Impala quickstart source (or your local build script):
   docker compose build --parallel
   docker compose up --build
   ```

---

## ⚙️ Notes

- The operator expects the GraphQL response to include a `nodes` list and `pageInfo` object.
- If you want to point to a different config file, update `config_path` in `example.py` or set `PIPELINE_CONFIG_PATH`.
