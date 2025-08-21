#!/bin/bash

# Script to easily run PyFlink tests with Docker Compose

set -e

echo "🚀 Starting Flink Cluster for PyFlink Tests"
echo "=========================================="

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Error: Docker is not running. Please start Docker first."
    exit 1
fi

# Start Flink cluster
echo "📦 Starting Flink JobManager and TaskManager..."
docker-compose up -d

echo "⏳ Waiting for Flink cluster to be ready..."
sleep 30

# Check if JobManager is healthy
echo "🔍 Checking Flink cluster health..."
if curl -s http://localhost:8081/config > /dev/null; then
    echo "✅ Flink cluster is ready!"
    echo "🌐 Flink Web UI available at: http://localhost:8081"
else
    echo "⚠️  Flink cluster might still be starting up..."
    echo "   You can check status with: docker-compose logs -f jobmanager"
fi

echo ""
echo "🧪 To run the deduplication test:"
echo "   docker-compose exec pyflink-runner python sources/test_raw_sales_events.py"
echo ""
echo "📊 To monitor the cluster:"
echo "   docker-compose logs -f"
echo ""
echo "🛑 To stop the cluster:"
echo "   docker-compose down"
echo ""