FROM apache/airflow:2.9.2-python3.11

# Set environment variables
ENV AIRFLOW_HOME=/opt/airflow

# Install system dependencies
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    libssl-dev \
    libffi-dev \
    python3-dev \
    curl && \
    rm -rf /var/lib/apt/lists/*

# Create a clean build directory for the package
USER airflow
WORKDIR /tmp/dag_builder_build

# Copy only the package files for installation
COPY pyproject.toml /tmp/dag_builder_build/
COPY dag_builder /tmp/dag_builder_build/dag_builder

# Install dag_builder in development mode to permanent location
RUN pip install -e /tmp/dag_builder_build/ && \
    pip install pytest pytest-cov pytest-mock

# Copy the source to a permanent location for the editable install
USER root
RUN mkdir -p /opt/dag_builder_package && \
    cp -r /tmp/dag_builder_build/dag_builder /opt/dag_builder_package/ && \
    cp /tmp/dag_builder_build/pyproject.toml /opt/dag_builder_package/ && \
    chown -R airflow:airflow /opt/dag_builder_package || chown -R airflow:root /opt/dag_builder_package || chown -R airflow /opt/dag_builder_package

# Update the editable install to point to the permanent location
USER airflow
RUN pip uninstall -y dag_builder && \
    pip install -e /opt/dag_builder_package/

# Copy example DAGs and configurations to Airflow directory
COPY example /opt/airflow/example

# Create necessary directories
RUN mkdir -p /opt/airflow/dags /opt/airflow/logs /opt/airflow/plugins

# Copy DAG examples to dags folder
RUN cp -r /opt/airflow/example/rest_api_dags/* /opt/airflow/dags/ || true
    # && \
    # cp -r /opt/airflow/example/graphql2impala/dags/* /opt/airflow/dags/ && \     
    # cp -r /opt/airflow/example/duckdb_dags/* /opt/airflow/dags/ || true

# Set ownership (check existing user/group)
USER root
RUN chown -R airflow:airflow /opt/airflow || chown -R airflow:root /opt/airflow || chown -R airflow /opt/airflow

# Clean up build directory (as root before switching)
RUN rm -rf /tmp/dag_builder_build

# Set working directory and switch back to airflow user for final state
WORKDIR /opt/airflow
USER airflow

# Default command
CMD ["airflow", "webserver"]
