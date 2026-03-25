#!/bin/bash

# Docker Compose setup script for DAG Builder with Airflow

set -e

echo "🚀 Setting up DAG Builder with Airflow..."

# Check if .env file exists
if [ ! -f .env ]; then
    echo "📝 Creating .env file from template..."
    cp .env.example .env
    echo "⚠️  Please update .env file with your API token and Fernet key"
    echo "   Generate Fernet key: python -c \"from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())\""
fi

# Create necessary directories
echo "📁 Creating directories..."
mkdir -p dags logs plugins

# Generate Fernet key if not set
if ! grep -q "your_fernet_key_here" .env; then
    echo "🔑 Fernet key already set in .env"
else
    echo "🔑 Generating Fernet key..."
    FERNET_KEY=$(python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())" 2>/dev/null || echo "")
    if [ -n "$FERNET_KEY" ]; then
        sed -i.bak "s/your_fernet_key_here/$FERNET_KEY/" .env
        echo "✅ Fernet key generated and added to .env"
    else
        echo "⚠️  Could not generate Fernet key automatically. Please set it manually in .env"
    fi
fi

# Copy example DAGs to dags folder
echo "📋 Copying example DAGs..."
cp -r example/graphql2impala/dags/* dags/ 2>/dev/null || true
cp -r example/rest_api_dags/* dags/ 2>/dev/null || true
cp -r example/duckdb_dags/* dags/ 2>/dev/null || true

echo "✅ Setup complete!"
echo ""
echo "🐳 To start Airflow:"
echo "   docker-compose up -d"
echo ""
echo "🌐 Access Airflow at: http://localhost:8080"
echo "   Username: admin"
echo "   Password: admin"
echo ""
echo "🌸 Flower UI at: http://localhost:5555"
echo ""
echo "📚 To stop:"
echo "   docker-compose down"
echo ""
echo "🧹 To clean up:"
echo "   docker-compose down -v"
