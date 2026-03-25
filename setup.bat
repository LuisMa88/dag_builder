@echo off
REM Docker Compose setup script for DAG Builder with Airflow (Windows)

echo 🚀 Setting up DAG Builder with Airflow...

REM Check if .env file exists
if not exist .env (
    echo 📝 Creating .env file from template...
    copy .env.example .env >nul
    echo ⚠️  Please update .env file with your API token and Fernet key
    echo    Generate Fernet key: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
)

REM Create necessary directories
echo 📁 Creating directories...
if not exist dags mkdir dags
if not exist logs mkdir logs
if not exist plugins mkdir plugins

REM Copy example DAGs to dags folder
echo 📋 Copying example DAGs...
xcopy /E /I /Y example\graphql2impala\dags\* dags\ >nul 2>&1
xcopy /E /I /Y example\rest_api_dags\* dags\ >nul 2>&1
xcopy /E /I /Y example\duckdb_dags\* dags\ >nul 2>&1

echo ✅ Setup complete!
echo.
echo 🐳 To start Airflow:
echo    docker-compose up -d
echo.
echo 🌐 Access Airflow at: http://localhost:8080
echo    Username: admin
echo    Password: admin
echo.
echo 🌸 Flower UI at: http://localhost:5555
echo.
echo 📚 To stop:
echo    docker-compose down
echo.
echo 🧹 To clean up:
echo    docker-compose down -v
pause
