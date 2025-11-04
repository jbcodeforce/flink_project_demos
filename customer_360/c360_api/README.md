# Customer Analytics C360 REST API

A FastAPI-based REST API that exposes Customer 360 analytics for Marketing, Product, and Finance teams. Built on top of the Apache Spark-powered C360 data pipeline.

## **Business Domain Endpoints**

### 📈 **Marketing Team (4 endpoints)**
1. **Customer Health Overview** - Dashboard metrics showing customer status distribution
2. **Churn Risk Analysis** - High-value customers at risk for retention campaigns  
3. **Loyalty Program Metrics** - Performance analysis across loyalty tiers
4. **Customer Segmentation** - Targeted marketing campaign insights

### 🛍️ **Product Team (3 endpoints)**
1. **Digital Engagement Analysis** - App adoption patterns by generation
2. **Cross-sell Opportunities** - Channel expansion and tier upgrade identification
3. **Category Insights** - Product performance and customer preferences

### 💰 **Finance Team (4 endpoints)**  
1. **Customer Lifetime Value** - CLV analysis for budget allocation
2. **Revenue Analysis** - Revenue breakdown by customer segment
3. **RFM Segmentation** - Advanced customer segmentation matrix
4. **Profitability Metrics** - Cost optimization and margin analysis

### 👥 **Customer Success Team (2 endpoints)**
1. **Support Insights** - Customer satisfaction and ticket analysis
2. **Lifecycle Analysis** - Customer behavior across lifecycle stages


## 🚀 **Quick Start**

### Prerequisites
- **Python 3.11+**
- **uv** (fast Python package manager) - will be auto-installed if missing
- **Apache Spark 3.x** with `spark-sql` command available
- **Java 8+** (required by Spark)
- **C360 Data Pipeline** (must be in `../c360_spark_processing/`)

### Installation

1. **Clone and Setup** (Automated - Recommended)
   ```bash
   cd c360_api
   python setup.py  # Automated setup with uv installation and dependency management
   ```

2. **Manual Installation** (if setup script fails)
   ```bash
   # Install uv if not available
   curl -LsSf https://astral.sh/uv/install.sh | sh
   
   # Install dependencies
   uv sync
   
   # Or install in system Python
   uv pip install -e . --system
   
   # Create environment file
   cp .env.example .env  # Edit configuration as needed
   ```

3. **Start the API**
   ```bash
   uv run python main.py
   # OR with auto-reload for development:
   uv run uvicorn main:app --reload --host 0.0.0.0 --port 8000
   ```

4. **Verify Installation**
   ```bash
   curl http://localhost:8000/health
   # Visit http://localhost:8000/docs for interactive API documentation
   ```

### **Daily Development:**
```bash
# Start development server
make dev
# or
uv run uvicorn main:app --reload

# Run tests
make test
# or
uv run python test_api.py

# Add new dependency
uv add fastapi-users  # Production dependency
uv add --dev pytest-mock  # Development dependency
```

### **Using Make Commands:**
```bash
make help          # Show all available commands
make dev-install   # Install all dependencies
make run           # Start API server
make test          # Run tests
make format        # Format code
make check         # Run all quality checks
make docker-build  # Build Docker image
```

## 📊 **API Endpoints Overview**

### 🏥 **Health & Admin**
- `GET /health` - API health check and system status
- `POST /admin/refresh-data` - Trigger C360 data pipeline refresh
- `GET /admin/metrics` - API performance metrics

### 📈 **Marketing Endpoints**
- `GET /marketing/customer-health-overview` - Customer status distribution
- `GET /marketing/churn-risk-customers` - High-value at-risk customers
- `GET /marketing/loyalty-program-metrics` - Loyalty tier performance
- `GET /marketing/customer-segmentation` - Customer segment analysis

### 🛍️ **Product Endpoints**
- `GET /product/digital-engagement-analysis` - App usage by generation
- `GET /product/cross-sell-opportunities` - Growth opportunities
- `GET /product/category-insights` - Product category performance

### 💰 **Finance Endpoints**
- `GET /finance/customer-lifetime-value` - CLV analysis by segment
- `GET /finance/revenue-analysis` - Revenue by customer segment
- `GET /finance/rfm-segmentation` - RFM customer matrix
- `GET /finance/profitability-metrics` - Customer profitability

### 👥 **Customer Success Endpoints**
- `GET /customer-success/support-insights` - Support satisfaction analysis
- `GET /customer-success/lifecycle-analysis` - Customer lifecycle stages

### 🔬 **Advanced Analytics**
- `GET /analytics/customer-profiles` - Detailed C360 customer profiles

## 📖 **API Usage Examples**

### Marketing: Target Churn Risk Customers
```bash
# Get high-value customers at risk of churn
curl "http://localhost:8000/marketing/churn-risk-customers?min_lifetime_value=5000&limit=10"

# Response: List of customers with risk indicators
{
  "success": true,
  "data": [
    {
      "customer_id": "CUST009",
      "customer_name": "Lisa Anderson", 
      "customer_segment": "Premium",
      "loyalty_tier": "Platinum",
      "total_spent": 1019.98,
      "risk_status": "CHURN RISK",
      "customer_health_score": 2.67
    }
  ]
}
```

### Product: Digital Engagement Analysis
```bash
# Analyze app adoption by generation
curl "http://localhost:8000/product/digital-engagement-analysis"

# Use for: App feature optimization, digital marketing strategy
```

### Finance: Customer Lifetime Value
```bash
# Get CLV analysis for budget planning
curl "http://localhost:8000/finance/customer-lifetime-value"

# Response includes: avg_lifetime_value, total_revenue, customer_count by segment
```

### Advanced: Customer Profile Filtering
```bash
# Get active customers with high health scores
curl "http://localhost:8000/analytics/customer-profiles?customer_status=Active&min_health_score=4.0&limit=50"

# Filter by multiple statuses
curl "http://localhost:8000/analytics/customer-profiles?customer_status=Active&customer_status=At%20Risk"
```

## 🔧 **Configuration**

### Environment Variables
Create a `.env` file to customize settings:

```bash
# Environment
ENVIRONMENT=development
DEBUG=true

# API Settings  
API_HOST=0.0.0.0
API_PORT=8000

# Data Pipeline
C360_DATA_PATH=../c360_mock_data
PIPELINE_PATH=../c360_spark_processing
CACHE_TTL_MINUTES=30

# Spark Configuration
SPARK_APP_NAME=C360_API
SPARK_EXECUTOR_MEMORY=2g
SPARK_DRIVER_MEMORY=2g

# Security
SECRET_KEY=your-secret-key-change-in-production
RATE_LIMIT_PER_MINUTE=100

# CORS (adjust for production)
CORS_ORIGINS=*
```

### Production Configuration
```bash
# Production settings
ENVIRONMENT=production
DEBUG=false
CORS_ORIGINS=https://yourdomain.com,https://app.yourdomain.com
RATE_LIMIT_PER_MINUTE=100
LOG_LEVEL=WARNING
```

## 📚 **API Documentation**

### Interactive Documentation
- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

### Response Format
All endpoints return standardized responses:

```json
{
  "success": true,
  "message": "Request processed successfully",
  "timestamp": "2024-06-20T14:30:00Z",
  "data": [...],
  "count": 15
}
```

### Error Handling
```json
{
  "success": false,
  "error_type": "HTTPException",
  "error_message": "Customer not found",
  "timestamp": "2024-06-20T14:30:00Z",
  "error_details": {"status_code": 404}
}
```

## 🏗️ **Architecture**

### Components
- **FastAPI Application**: REST API framework with automatic OpenAPI documentation
- **Spark Integration**: Executes C360 pipeline and queries data via `spark-sql`
- **Caching Layer**: In-memory caching with configurable TTL for performance
- **Pydantic Models**: Type-safe request/response validation
- **Structured Logging**: JSON-formatted logs for monitoring and debugging

### Data Flow
1. **API Request** → FastAPI endpoint
2. **Cache Check** → Return cached results if available and fresh
3. **Pipeline Execution** → Run C360 Spark pipeline if data is stale
4. **Query Execution** → Execute business logic SQL queries
5. **Response Formatting** → Return typed, validated JSON responses

## 🚀 **Deployment**

### Development
```bash
uv run python main.py  # Built-in development server with uv
```

### Production
```bash
# Using Gunicorn with Uvicorn workers
uv add gunicorn
uv run gunicorn -w 4 -k uvicorn.workers.UvicornWorker main:app --bind 0.0.0.0:8000

# Using Docker
docker build -t c360-api .
docker run -p 8000:8000 c360-api
```

### Docker Deployment
```dockerfile
FROM python:3.11-slim

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

WORKDIR /app

# Copy dependency files
COPY pyproject.toml uv.lock* ./

# Install dependencies
RUN uv sync --frozen --no-dev

# Copy application code
COPY . .

EXPOSE 8000

# Run with uv
CMD ["uv", "run", "python", "main.py"]
```

## 🔍 **Monitoring & Troubleshooting**

### Health Checks
```bash
# Basic health check
curl http://localhost:8000/health

# Detailed system status
curl http://localhost:8000/admin/metrics
```

### Common Issues

**1. "spark-sql command not found"**
```bash
# Install Apache Spark
brew install apache-spark  # macOS
# OR download from https://spark.apache.org/downloads.html
```

**2. "Pipeline execution failed"**
```bash
# Check C360 pipeline manually
cd ../c360_spark_processing
./test_pipeline.sh

# Force refresh via API
curl -X POST "http://localhost:8000/admin/refresh-data?force_refresh=true"
```

**3. "High memory usage"**
```bash
# Adjust Spark memory settings in .env
SPARK_EXECUTOR_MEMORY=1g
SPARK_DRIVER_MEMORY=1g
```

### Logging
Structured JSON logs include:
- Request/response details
- Query execution times
- Error context and stack traces
- Performance metrics

## 🤝 **Integration Examples**

### Python Client
```python
import requests

# Marketing: Get churn risk customers
response = requests.get(
    "http://localhost:8000/marketing/churn-risk-customers",
    params={"min_lifetime_value": 5000, "limit": 20}
)
customers = response.json()["data"]

# Process for retention campaign
for customer in customers:
    if customer["customer_health_score"] < 3.0:
        # Trigger retention campaign
        print(f"High priority: {customer['customer_name']}")
```

### BI Tool Integration
```sql
-- Example: Connect Tableau/Power BI to API endpoints
-- Use REST connector with URLs like:
-- http://localhost:8000/marketing/customer-health-overview
-- http://localhost:8000/finance/customer-lifetime-value
```

## 📝 **Development**

### Adding New Endpoints
1. **Define Pydantic Model** in `models.py`
2. **Add Database Query** in `database.py`  
3. **Create API Endpoint** in `main.py`
4. **Update Documentation** in this README

### Testing
```bash
# Install test dependencies (included in pyproject.toml)
uv sync  # Installs dev dependencies

# Run tests
uv run pytest tests/

# Run specific test
uv run python test_api.py

# Run with coverage
uv run pytest --cov=. --cov-report=html
```

### 🐳 **Docker Building and Running:**
```bash
# Build image
make docker-build

# Run container
make docker-run

# Or manually
docker build -t c360-api .
docker run -p 8000:8000 c360-api
```
