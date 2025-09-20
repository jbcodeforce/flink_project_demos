#!/bin/bash

# Customer Analytics C360 API - Demo Script
# Demonstrates the complete workflow from setup to business insights

set -e

echo "🎯 Customer Analytics C360 API - Complete Demo"
echo "==============================================="
echo ""

# Color codes for better output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

print_step() {
    echo -e "${BLUE}📋 Step $1:${NC} $2"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Check if we're in the right directory
if [ ! -f "main.py" ]; then
    print_error "Please run this script from the c360_api directory"
    exit 1
fi

# Step 1: Setup and Prerequisites
print_step "1" "Checking Prerequisites"
echo ""

# Check Python
if command -v python3 &> /dev/null; then
    python_version=$(python3 --version 2>&1)
    print_success "Python: $python_version"
else
    print_error "Python 3 is not installed"
    exit 1
fi

# Check Spark
if command -v spark-sql &> /dev/null; then
    print_success "Apache Spark: spark-sql command available"
else
    print_warning "Apache Spark not found - install with: brew install apache-spark"
    echo "Demo will continue but API may not work properly"
fi

# Check Java
if command -v java &> /dev/null; then
    java_version=$(java -version 2>&1 | head -n 1)
    print_success "Java: $java_version"
else
    print_warning "Java not found - required by Apache Spark"
fi

echo ""

# Step 2: Setup Dependencies
print_step "2" "Installing Dependencies"
echo ""

if [ ! -f ".env" ]; then
    echo "Creating .env configuration file..."
    cat > .env << 'EOF'
# Customer Analytics C360 API Configuration
ENVIRONMENT=development
DEBUG=true
API_HOST=0.0.0.0
API_PORT=8000
C360_DATA_PATH=../c360_mock_data
PIPELINE_PATH=../c360_spark_processing
CACHE_TTL_MINUTES=10
SPARK_APP_NAME=C360_API_Demo
LOG_LEVEL=INFO
SECRET_KEY=demo-secret-key
RATE_LIMIT_PER_MINUTE=1000
CORS_ORIGINS=*
EOF
    print_success "Created .env configuration file"
else
    print_success ".env file already exists"
fi

# Check and install uv if needed
if ! command -v uv &> /dev/null; then
    print_warning "uv package manager not found, installing..."
    if curl -LsSf https://astral.sh/uv/install.sh | sh > /dev/null 2>&1; then
        export PATH="$HOME/.cargo/bin:$PATH"  # Add uv to PATH for this session
        print_success "uv package manager installed"
    else
        print_error "Failed to install uv package manager"
        print_error "Please install manually: curl -LsSf https://astral.sh/uv/install.sh | sh"
        exit 1
    fi
else
    print_success "uv package manager available"
fi

# Install Python dependencies with uv
echo "Installing Python dependencies with uv..."
if uv sync > /dev/null 2>&1; then
    print_success "Python dependencies installed with uv"
else
    print_warning "uv sync failed, trying alternative installation..."
    if uv pip install -e . --system > /dev/null 2>&1; then
        print_success "Dependencies installed with uv pip"
    else
        print_warning "Some dependencies might have failed to install"
    fi
fi

echo ""

# Step 3: Verify C360 Pipeline
print_step "3" "Verifying C360 Data Pipeline"
echo ""

if [ -d "../c360_spark_processing" ]; then
    print_success "C360 pipeline directory found"
    
    if [ -f "../c360_spark_processing/c360_consolidated_pipeline.sql" ]; then
        print_success "C360 consolidated pipeline found"
    else
        print_error "C360 pipeline SQL file not found"
        exit 1
    fi
    
    if [ -d "../c360_mock_data" ]; then
        print_success "C360 mock data directory found"
        data_files=$(find ../c360_mock_data -name "*.csv" | wc -l)
        print_success "Found $data_files CSV data files"
    else
        print_error "C360 mock data directory not found"
        exit 1
    fi
else
    print_error "C360 pipeline directory not found at ../c360_spark_processing"
    print_error "Please run this demo from the c360_api directory"
    exit 1
fi

echo ""

# Step 4: Test Pipeline Execution
print_step "4" "Testing C360 Pipeline Execution"
echo ""

echo "Running quick pipeline test..."
cd ../c360_spark_processing

if ./test_pipeline.sh > /dev/null 2>&1; then
    print_success "C360 pipeline test successful"
else
    print_warning "Pipeline test had issues - API may still work with cached data"
fi

cd ../c360_api
echo ""

# Step 5: Start API Server
print_step "5" "Starting Customer Analytics C360 API"
echo ""

echo "Starting API server in background..."
uv run python main.py > api.log 2>&1 &
API_PID=$!

# Wait for API to start
echo "Waiting for API to initialize..."
for i in {1..10}; do
    if curl -s http://localhost:8000/health > /dev/null 2>&1; then
        print_success "API server started successfully (PID: $API_PID)"
        break
    fi
    if [ $i -eq 10 ]; then
        print_error "API server failed to start"
        kill $API_PID 2>/dev/null || true
        echo "Check api.log for error details"
        exit 1
    fi
    sleep 2
    echo -n "."
done

echo ""

# Step 6: API Health Check
print_step "6" "API Health Check"
echo ""

health_response=$(curl -s http://localhost:8000/health)
if echo "$health_response" | grep -q '"status":"healthy"'; then
    print_success "API health check passed"
    total_customers=$(echo "$health_response" | grep -o '"total_customers":[0-9]*' | cut -d':' -f2)
    if [ ! -z "$total_customers" ] && [ "$total_customers" -gt 0 ]; then
        print_success "C360 data pipeline operational with $total_customers customers"
    fi
else
    print_warning "API health check returned unexpected response"
    echo "Response: $health_response"
fi

echo ""

# Step 7: Business Use Case Demonstrations
print_step "7" "Business Use Case Demonstrations"
echo ""

echo "🎯 Demonstrating Business Analytics Use Cases:"
echo ""

# Marketing: Churn Risk Analysis
echo "📈 Marketing Use Case: Churn Risk Analysis"
churn_response=$(curl -s "http://localhost:8000/marketing/churn-risk-customers?min_lifetime_value=1000&limit=3")
churn_count=$(echo "$churn_response" | grep -o '"data":\[.*\]' | grep -o '\[.*\]' | grep -o '{}' | wc -l)
if [ "$churn_count" -gt 0 ]; then
    print_success "Found high-value customers at churn risk"
    echo "   💡 Use case: Target these customers for retention campaigns"
else
    print_success "No high-value customers at churn risk (good news!)"
fi

# Finance: Revenue Analysis  
echo "💰 Finance Use Case: Revenue Analysis by Segment"
revenue_response=$(curl -s "http://localhost:8000/finance/revenue-analysis")
if echo "$revenue_response" | grep -q '"success":true'; then
    print_success "Revenue analysis by customer segment available"
    echo "   💡 Use case: Budget allocation and financial forecasting"
else
    print_warning "Revenue analysis had issues"
fi

# Product: Digital Engagement
echo "🛍️ Product Use Case: Digital Engagement Analysis"
engagement_response=$(curl -s "http://localhost:8000/product/digital-engagement-analysis")
if echo "$engagement_response" | grep -q '"success":true'; then
    print_success "Digital engagement analysis by generation available"
    echo "   💡 Use case: App optimization and digital marketing strategy"
else
    print_warning "Digital engagement analysis had issues"
fi

echo ""

# Step 8: Interactive API Documentation
print_step "8" "Interactive API Documentation"
echo ""

print_success "API Documentation available at:"
echo "   📚 Swagger UI: http://localhost:8000/docs"
echo "   📖 ReDoc: http://localhost:8000/redoc"
echo ""

# Step 9: Sample API Calls
print_step "9" "Sample API Calls for Each Business Domain"
echo ""

echo "🔧 Try these sample API calls:"
echo ""
echo "Marketing Team:"
echo "  curl 'http://localhost:8000/marketing/customer-health-overview'"
echo "  curl 'http://localhost:8000/marketing/loyalty-program-metrics'"
echo ""
echo "Product Team:"
echo "  curl 'http://localhost:8000/product/cross-sell-opportunities?limit=5'"
echo "  curl 'http://localhost:8000/product/digital-engagement-analysis'"
echo ""
echo "Finance Team:"
echo "  curl 'http://localhost:8000/finance/customer-lifetime-value'"
echo "  curl 'http://localhost:8000/finance/rfm-segmentation'"
echo ""
echo "Advanced Analytics:"
echo "  curl 'http://localhost:8000/analytics/customer-profiles?limit=5'"
echo "  curl 'http://localhost:8000/analytics/customer-profiles?min_health_score=4.0'"
echo ""

# Step 10: Complete Test Suite
print_step "10" "Running Complete Test Suite"
echo ""

echo "Running comprehensive API test suite..."
if uv run python test_api.py > test_results.log 2>&1; then
    print_success "All API tests passed!"
    echo "   📊 Detailed results available in test_results.log"
else
    print_warning "Some API tests had issues - check test_results.log"
fi

echo ""

# Final Summary
echo "🎉 Customer Analytics C360 API Demo Complete!"
echo "============================================="
echo ""
print_success "API Server: Running on http://localhost:8000 (PID: $API_PID)"
print_success "Documentation: http://localhost:8000/docs"
print_success "Health Check: http://localhost:8000/health"
echo ""
echo "📊 Available Business Intelligence Endpoints:"
echo "   📈 Marketing: /marketing/* (churn risk, loyalty, segmentation)"
echo "   🛍️  Product: /product/* (engagement, cross-sell, categories)"
echo "   💰 Finance: /finance/* (CLV, revenue, RFM, profitability)"  
echo "   👥 Customer Success: /customer-success/* (support, lifecycle)"
echo "   🔬 Advanced Analytics: /analytics/* (customer profiles)"
echo ""
echo "💡 Next Steps:"
echo "   1. Explore the API documentation at http://localhost:8000/docs"
echo "   2. Try the sample curl commands above"
echo "   3. Integrate with your BI tools (Tableau, Power BI, etc.)"
echo "   4. Build custom dashboards using the REST endpoints"
echo ""
echo "🛑 To stop the API server:"
echo "   kill $API_PID"
echo ""

# Save PID for easy cleanup
echo "$API_PID" > .api_pid

print_success "Demo completed successfully! 🚀"
