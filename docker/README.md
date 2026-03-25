# Docker Compose for DAG Builder with Airflow

This directory contains Docker Compose configurations to run DAG Builder with Apache Airflow.

## 🐳 Quick Start

### 1. Production Setup (PostgreSQL + Redis + Celery)

```bash
# Setup environment
cp .env.example .env
# Edit .env with your API token and Fernet key

# Initialize and start
chmod +x setup.sh
./setup.sh
docker-compose up -d

# Access Airflow
# URL: http://localhost:8080
# Username: admin
# Password: admin
```

### 2. Development Setup (SQLite + LocalExecutor)

```bash
# Quick start with SQLite
docker-compose -f docker-compose.simple.yml up -d

# Access Airflow at http://localhost:8080
# Username: admin, Password: admin
```

## 📁 File Structure

```
├── docker-compose.yml          # Production setup (PostgreSQL + Redis)
├── docker-compose.simple.yml   # Development setup (SQLite only)
├── Dockerfile                   # Custom Airflow image with dag_builder
├── connections.json            # Default Airflow connections
├── .env.example                # Environment variables template
├── setup.sh                    # Setup script
└── README.md                   # This file
```

## 🔧 Configuration

### Environment Variables (.env)

```bash
# Required
APP_API_TOKEN=your_api_token_here
AIRFLOW__CORE__FERNET_KEY=your_fernet_key_here

# Optional
AIRFLOW__CORE__EXECUTOR=CeleryExecutor
AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
```

### Airflow Connections

The setup includes default connections:

- **impala_default**: Impala connection (localhost:21050)
- **duckdb_default**: File-based DuckDB connection
- **duckdb_memory**: In-memory DuckDB connection

## 🚀 Services

### Production Setup

| Service | Port | Description |
|---------|------|-------------|
| airflow-webserver | 8080 | Airflow Web UI |
| airflow-scheduler | - | DAG scheduler |
| airflow-worker | - | Celery workers |
| flower | 5555 | Celery monitoring |
| postgres | 5432 | PostgreSQL database |
| redis | 6379 | Redis broker |

### Development Setup

| Service | Port | Description |
|---------|------|-------------|
| airflow | 8080 | All-in-one Airflow (webserver + scheduler) |

## 📋 DAG Examples

The following DAG examples are automatically loaded:

- **GraphQL to Impala**: `example/graphql2impala/`
- **REST API to Impala**: `example/rest_api_dags/`
- **GraphQL/REST API to DuckDB**: `example/duckdb_dags/`

## 🛠️ Development

### Building Custom Image

```bash
# Build with dag_builder
docker build -t dag-builder-airflow .

# Run with custom image
docker run -p 8080:8080 dag-builder-airflow
```

### Local Development

```bash
# Mount local dag_builder for development
docker-compose up -d --build
docker-compose exec airflow-webserver bash
cd /opt/airflow/dag_builder
pip install -e .
```

## 🔍 Monitoring

### Airflow Web UI
- URL: http://localhost:8080
- Monitor DAG runs, task instances, and logs

### Flower (Celery Monitoring)
- URL: http://localhost:5555
- Monitor Celery workers and tasks

### Logs
- Airflow logs: `./logs/`
- DAG Builder logs: `./logs/dag_builder.log`

## 🧹 Cleanup

```bash
# Stop services
docker-compose down

# Remove volumes (data loss!)
docker-compose down -v

# Remove images
docker-compose down --rmi all
```

## 🎯 Use Cases

### 1. Development & Testing
```bash
# Quick setup with SQLite
docker-compose -f docker-compose.simple.yml up -d
```

### 2. Production Deployment
```bash
# Full production setup
docker-compose up -d
```

### 3. Scale Workers
```bash
# Scale workers
docker-compose up -d --scale airflow-worker=3
```

### 4. Add DuckDB Service
```bash
# Include DuckDB web interface
docker-compose --profile duckdb up -d
```

## 🔒 Security

- Change default admin password in production
- Use strong Fernet key
- Set proper API tokens
- Configure network access as needed
- Use HTTPS in production

## 🐛 Troubleshooting

### Common Issues

1. **Fernet Key Error**
   ```bash
   python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
   ```

2. **Permission Issues**
   ```bash
   sudo chown -R $USER:$USER dags logs plugins
   ```

3. **Port Conflicts**
   ```bash
   # Change ports in docker-compose.yml
   ports:
     - 8081:8080  # Use different port
   ```

4. **DAG Import Errors**
   - Check `./logs/dag_builder.log`
   - Verify `APP_API_TOKEN` is set
   - Check configuration files

### Logs

```bash
# View Airflow logs
docker-compose logs airflow-webserver

# View DAG Builder logs
docker-compose exec airflow-webserver tail -f /opt/airflow/logs/dag_builder.log

# View all logs
docker-compose logs -f
```

## 📚 Additional Resources

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [DAG Builder Documentation](./README.md)
- [Docker Compose Documentation](https://docs.docker.com/compose/)
