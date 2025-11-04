# Customer Analytics C360 API - Makefile
# Convenient commands for development and deployment using uv

.PHONY: help install dev-install clean test run dev docs docker-build docker-run lint format check-format type-check

# Default target
help:
	@echo "Customer Analytics C360 API - Available Commands"
	@echo "================================================"
	@echo ""
	@echo "Setup & Installation:"
	@echo "  install      Install production dependencies with uv"
	@echo "  dev-install  Install all dependencies including dev tools"
	@echo "  clean        Clean up build artifacts and cache"
	@echo ""
	@echo "Development:"
	@echo "  run          Start the API server"
	@echo "  dev          Start the API server with auto-reload"
	@echo "  test         Run the test suite"
	@echo "  demo         Run the complete demo workflow"
	@echo ""
	@echo "Code Quality:"
	@echo "  lint         Run linting checks (flake8)"
	@echo "  format       Format code with black and isort"
	@echo "  check-format Check if code is properly formatted"
	@echo "  type-check   Run type checking with mypy"
	@echo "  check        Run all quality checks"
	@echo ""
	@echo "Docker:"
	@echo "  docker-build Build the Docker image"
	@echo "  docker-run   Run the Docker container"
	@echo ""
	@echo "Documentation:"
	@echo "  docs         Build and serve documentation"

# Setup & Installation
install:
	@echo "📦 Installing production dependencies..."
	uv sync --no-dev

dev-install:
	@echo "📦 Installing all dependencies (including dev tools)..."
	uv sync

clean:
	@echo "🧹 Cleaning up build artifacts..."
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	rm -rf __pycache__/
	rm -rf .pytest_cache/
	rm -rf .mypy_cache/
	rm -rf htmlcov/
	rm -f .coverage
	rm -f *.log
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

# Development
run:
	@echo "🚀 Starting API server..."
	uv run python main.py

dev:
	@echo "🚀 Starting API server with auto-reload..."
	uv run uvicorn main:app --reload --host 0.0.0.0 --port 8000

test:
	@echo "🧪 Running test suite..."
	uv run python test_api.py

demo:
	@echo "🎯 Running complete demo workflow..."
	./demo.sh

# Code Quality
lint:
	@echo "🔍 Running linting checks..."
	uv run flake8 --max-line-length=100 --exclude=.venv .

format:
	@echo "✨ Formatting code..."
	uv run black --line-length=100 .
	uv run isort --profile black --line-length=100 .

check-format:
	@echo "🔍 Checking code formatting..."
	uv run black --check --line-length=100 .
	uv run isort --check-only --profile black --line-length=100 .

type-check:
	@echo "🔍 Running type checks..."
	uv run mypy . --ignore-missing-imports

check: check-format lint type-check
	@echo "✅ All quality checks completed"

# Docker
docker-build:
	@echo "🐳 Building Docker image..."
	docker build -t customer-analytics-c360-api:latest .

docker-run:
	@echo "🐳 Running Docker container..."
	docker run -p 8000:8000 --name c360-api customer-analytics-c360-api:latest

# Documentation
docs:
	@echo "📚 Building documentation..."
	@echo "Visit http://localhost:8000/docs once the API is running"

# Environment setup
setup-env:
	@echo "⚙️  Setting up environment..."
	python setup.py

# Health checks
health:
	@echo "🏥 Checking API health..."
	curl -s http://localhost:8000/health | python -m json.tool

# Development utilities
logs:
	@echo "📋 Showing API logs..."
	tail -f api.log

kill-api:
	@echo "🛑 Stopping API server..."
	@if [ -f .api_pid ]; then \
		kill $$(cat .api_pid) 2>/dev/null || true; \
		rm -f .api_pid; \
		echo "API server stopped"; \
	else \
		echo "No API process found"; \
	fi

# Database/Pipeline utilities
refresh-data:
	@echo "🔄 Refreshing C360 data..."
	curl -X POST "http://localhost:8000/admin/refresh-data?force_refresh=true"

pipeline-test:
	@echo "🔧 Testing C360 pipeline..."
	cd ../c360_spark_processing && ./test_pipeline.sh

# Deployment utilities
deploy-prep: clean check test
	@echo "🚀 Preparing for deployment..."
	@echo "✅ All checks passed - ready for deployment"

# Show project status
status:
	@echo "📊 Project Status"
	@echo "=================="
	@echo "Python: $$(python --version 2>/dev/null || echo 'Not found')"
	@echo "uv: $$(uv --version 2>/dev/null || echo 'Not found')"
	@echo "Spark: $$(spark-sql --version 2>/dev/null | head -1 || echo 'Not found')"
	@echo "Java: $$(java -version 2>&1 | head -1 || echo 'Not found')"
	@echo "API Status: $$(curl -s http://localhost:8000/health > /dev/null && echo 'Running' || echo 'Stopped')"

# Install uv if not present
install-uv:
	@echo "📦 Installing uv package manager..."
	curl -LsSf https://astral.sh/uv/install.sh | sh
	@echo "✅ uv installed - restart your shell or run: source ~/.bashrc"
