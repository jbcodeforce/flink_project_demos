"""
Customer Analytics C360 REST API
FastAPI application exposing C360 analytics for Marketing, Product, and Finance teams
"""

import os
import uvicorn
from typing import List, Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import structlog

# Import our models and database manager
from models import *
from database import c360_data_manager

# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger(__name__)

# Application metadata
API_VERSION = "1.0.0"
ENVIRONMENT = os.getenv("ENVIRONMENT", "development")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan events"""
    # Startup
    logger.info("Starting Customer Analytics C360 API", version=API_VERSION, environment=ENVIRONMENT)
    
    # Initialize data manager
    try:
        # Run initial pipeline check
        await run_pipeline_refresh(force_refresh=False)
        logger.info("Initial data refresh completed")
    except Exception as e:
        logger.error("Failed to initialize data pipeline", error=str(e))
    
    yield
    
    # Shutdown
    logger.info("Shutting down Customer Analytics C360 API")
    c360_data_manager.close_spark_session()


# Create FastAPI application
app = FastAPI(
    title="Customer Analytics C360 API",
    description="REST API for Customer 360 analytics supporting Marketing, Product, and Finance use cases",
    version=API_VERSION,
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify allowed origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

async def run_pipeline_refresh(force_refresh: bool = False):
    """Background task to refresh the C360 pipeline"""
    try:
        success = c360_data_manager.run_c360_pipeline(force_refresh=force_refresh)
        if not success:
            raise RuntimeError("Pipeline execution failed")
        logger.info("Pipeline refresh completed successfully")
    except Exception as e:
        logger.error("Pipeline refresh failed", error=str(e))
        raise


def handle_database_error(func_name: str, error: Exception):
    """Handle database errors consistently"""
    logger.error(f"{func_name} failed", error=str(error))
    raise HTTPException(
        status_code=500,
        detail=f"Internal server error in {func_name}: {str(error)}"
    )


# ============================================================================
# HEALTH CHECK & ADMIN ENDPOINTS
# ============================================================================

@app.get("/health", response_model=HealthCheckResponse, tags=["Admin"])
async def health_check():
    """
    Health check endpoint for monitoring and load balancers
    """
    try:
        # Check data product health
        health_data = c360_data_manager.get_data_product_health_check()
        
        return HealthCheckResponse(
            status="healthy",
            version=API_VERSION,
            environment=ENVIRONMENT,
            database_status="connected",
            spark_status="available",
            total_customers=health_data.get('total_customers_processed', 0)
        )
    except Exception as e:
        logger.error("Health check failed", error=str(e))
        raise HTTPException(status_code=503, detail="Service unavailable")


@app.post("/admin/refresh-data", tags=["Admin"])
async def refresh_data(background_tasks: BackgroundTasks, force_refresh: bool = Query(False)):
    """
    Trigger a refresh of the C360 data pipeline
    
    - **force_refresh**: Force refresh even if data was recently updated
    """
    try:
        background_tasks.add_task(run_pipeline_refresh, force_refresh)
        return APIResponse(
            message="Data refresh initiated" + (" (forced)" if force_refresh else ""),
            data={"refresh_initiated": True, "force_refresh": force_refresh}
        )
    except Exception as e:
        handle_database_error("refresh_data", e)


@app.get("/admin/metrics", response_model=MetricsResponse, tags=["Admin"])
async def get_metrics():
    """
    Get API performance metrics (placeholder - would integrate with monitoring system)
    """
    return MetricsResponse(
        total_requests=1000,  # Placeholder values
        avg_response_time_ms=150.5,
        error_rate=0.02,
        uptime_seconds=86400,
        cache_hit_rate=0.75
    )


# ============================================================================
# MARKETING USE CASES
# ============================================================================

@app.get("/marketing/customer-health-overview", 
         response_model=List[CustomerHealthOverview], 
         tags=["Marketing"])
async def get_customer_health_overview():
    """
    Get customer health distribution overview
    
    **Use Case**: Marketing dashboard showing customer status distribution,
    health scores, and spending patterns for campaign planning.
    """
    try:
        data = c360_data_manager.get_customer_health_overview()
        resp = [CustomerHealthOverview(**record) for record in data]
        resp.timestamp = resp.timestamp.isoformat()
        return resp
    except Exception as e:
        handle_database_error("get_customer_health_overview", e)


@app.get("/marketing/churn-risk-customers", 
         response_model=List[ChurnRiskCustomer], 
         tags=["Marketing"])
async def get_churn_risk_customers(
    min_lifetime_value: float = Query(1000, ge=0, description="Minimum customer lifetime value"),
    limit: int = Query(50, ge=1, le=1000, description="Maximum number of results")
):
    """
    Get high-value customers at risk of churn
    
    **Use Case**: Target retention campaigns at high-value customers showing 
    churn risk signals based on purchase recency and engagement.
    """
    try:
        data = c360_data_manager.get_churn_risk_customers(min_lifetime_value, limit)
        return [ChurnRiskCustomer(**record) for record in data]
    except Exception as e:
        handle_database_error("get_churn_risk_customers", e)


@app.get("/marketing/loyalty-program-metrics", 
         response_model=List[LoyaltyProgramMetrics], 
         tags=["Marketing"])
async def get_loyalty_program_metrics():
    """
    Get loyalty program effectiveness metrics
    
    **Use Case**: Evaluate loyalty program performance across tiers,
    optimize point structures, and identify tier upgrade opportunities.
    """
    try:
        data = c360_data_manager.get_loyalty_program_metrics()
        return [LoyaltyProgramMetrics(**record) for record in data]
    except Exception as e:
        handle_database_error("get_loyalty_program_metrics", e)


@app.get("/marketing/customer-segmentation", 
         response_model=List[CustomerSegmentation], 
         tags=["Marketing"])
async def get_customer_segmentation():
    """
    Get customer segmentation analysis
    
    **Use Case**: Create targeted marketing campaigns based on customer
    segments, lifetime value, and churn risk patterns.
    """
    try:
        # This would be a more complex query combining multiple dimensions
        data = c360_data_manager.get_customer_lifetime_value_analysis()
        
        # Transform the data to match our segmentation model
        segmentation_data = []
        for record in data:
            segmentation_data.append({
                "segment_name": record["value_segment"],
                "customer_count": record["customer_count"],
                "avg_lifetime_value": record["avg_lifetime_value"],
                "avg_health_score": 3.0,  # Placeholder - would calculate from actual data
                "churn_risk_percentage": 25.0  # Placeholder - would calculate from actual data
            })
        
        return [CustomerSegmentation(**record) for record in segmentation_data]
    except Exception as e:
        handle_database_error("get_customer_segmentation", e)


# ============================================================================
# PRODUCT USE CASES
# ============================================================================

@app.get("/product/digital-engagement-analysis", 
         response_model=List[DigitalEngagementAnalysis], 
         tags=["Product"])
async def get_digital_engagement_analysis():
    """
    Get digital engagement analysis by customer generation
    
    **Use Case**: Understand digital adoption patterns across generations
    to optimize app features and digital marketing strategies.
    """
    try:
        data = c360_data_manager.get_digital_engagement_analysis()
        return [DigitalEngagementAnalysis(**record) for record in data]
    except Exception as e:
        handle_database_error("get_digital_engagement_analysis", e)


@app.get("/product/cross-sell-opportunities", 
         response_model=List[CrossSellOpportunity], 
         tags=["Product"])
async def get_cross_sell_opportunities(
    limit: int = Query(50, ge=1, le=1000, description="Maximum number of results")
):
    """
    Get cross-sell and upsell opportunities
    
    **Use Case**: Identify customers for channel expansion, tier upgrades,
    and app adoption to increase customer engagement and revenue.
    """
    try:
        data = c360_data_manager.get_cross_sell_opportunities(limit)
        return [CrossSellOpportunity(**record) for record in data]
    except Exception as e:
        handle_database_error("get_cross_sell_opportunities", e)


@app.get("/product/category-insights", 
         response_model=List[ProductCategoryInsights], 
         tags=["Product"])
async def get_category_insights():
    """
    Get product category performance insights
    
    **Use Case**: Analyze product category performance, customer preferences,
    and cross-category purchasing patterns for inventory and merchandising decisions.
    """
    try:
        # This would require joining with product transaction data
        # For now, returning placeholder data structure
        placeholder_data = [
            {
                "category": "Electronics",
                "customer_count": 45,
                "avg_order_value": 299.99,
                "total_revenue": 13499.55,
                "repeat_purchase_rate": 0.35
            },
            {
                "category": "Fashion",
                "customer_count": 38,
                "avg_order_value": 149.99,
                "total_revenue": 5699.62,
                "repeat_purchase_rate": 0.42
            }
        ]
        
        return [ProductCategoryInsights(**record) for record in placeholder_data]
    except Exception as e:
        handle_database_error("get_category_insights", e)


# ============================================================================
# FINANCE USE CASES
# ============================================================================

@app.get("/finance/customer-lifetime-value", 
         response_model=List[CustomerLifetimeValue], 
         tags=["Finance"])
async def get_customer_lifetime_value():
    """
    Get customer lifetime value analysis by segment
    
    **Use Case**: Financial analysis of customer value segments for
    budget allocation, acquisition cost optimization, and revenue forecasting.
    """
    try:
        data = c360_data_manager.get_customer_lifetime_value_analysis()
        return [CustomerLifetimeValue(**record) for record in data]
    except Exception as e:
        handle_database_error("get_customer_lifetime_value", e)


@app.get("/finance/revenue-analysis", 
         response_model=List[RevenueAnalysis], 
         tags=["Finance"])
async def get_revenue_analysis():
    """
    Get revenue analysis by customer segment
    
    **Use Case**: Revenue performance analysis across customer segments
    for financial reporting and strategic planning.
    """
    try:
        # This would require a more complex query aggregating revenue by segment
        # Placeholder implementation using existing customer data
        query = """
        SELECT 
            customer_segment,
            COUNT(*) as customer_count,
            SUM(total_spent) as total_revenue,
            AVG(total_spent) as avg_revenue_per_customer,
            (SUM(total_spent) * 100.0 / 
                (SELECT SUM(total_spent) FROM customer_analytics_c360)) as percentage_of_total_revenue
        FROM customer_analytics_c360
        GROUP BY customer_segment
        ORDER BY total_revenue DESC
        """
        
        df = c360_data_manager.query_spark_sql(query, cache_key="revenue_analysis")
        data = df.to_dict('records')
        
        return [RevenueAnalysis(**record) for record in data]
    except Exception as e:
        handle_database_error("get_revenue_analysis", e)


@app.get("/finance/rfm-segmentation", 
         response_model=List[RFMSegmentation], 
         tags=["Finance"])
async def get_rfm_segmentation():
    """
    Get RFM (Recency, Frequency, Monetary) segmentation analysis
    
    **Use Case**: Advanced customer segmentation for targeted investment,
    resource allocation, and personalized pricing strategies.
    """
    try:
        data = c360_data_manager.get_rfm_segmentation()
        return [RFMSegmentation(**record) for record in data]
    except Exception as e:
        handle_database_error("get_rfm_segmentation", e)


@app.get("/finance/profitability-metrics", 
         response_model=List[ProfitabilityMetrics], 
         tags=["Finance"])
async def get_profitability_metrics():
    """
    Get customer profitability metrics
    
    **Use Case**: Profitability analysis across customer tiers for
    cost optimization and margin improvement initiatives.
    """
    try:
        # Placeholder implementation - would require cost data integration
        placeholder_data = [
            {
                "customer_tier": "Platinum",
                "customer_count": 8,
                "total_revenue": 4500.50,
                "avg_profit_margin": 0.35,
                "cost_to_serve": 125.00
            },
            {
                "customer_tier": "Gold",
                "customer_count": 5,
                "total_revenue": 2800.75,
                "avg_profit_margin": 0.28,
                "cost_to_serve": 85.00
            }
        ]
        
        return [ProfitabilityMetrics(**record) for record in placeholder_data]
    except Exception as e:
        handle_database_error("get_profitability_metrics", e)


# ============================================================================
# CUSTOMER SUCCESS USE CASES
# ============================================================================

@app.get("/customer-success/support-insights", 
         response_model=List[CustomerSupportInsights], 
         tags=["Customer Success"])
async def get_support_insights():
    """
    Get customer support satisfaction insights
    
    **Use Case**: Customer success team analysis of support effectiveness,
    satisfaction trends, and proactive intervention opportunities.
    """
    try:
        query = """
        SELECT 
            support_satisfaction_level,
            COUNT(*) as customer_count,
            ROUND(AVG(total_support_tickets), 1) as avg_tickets_per_customer,
            ROUND(AVG(avg_satisfaction), 2) as avg_satisfaction_score,
            SUM(urgent_support_tickets) as total_urgent_tickets,
            ROUND(AVG(total_spent), 2) as avg_customer_value
        FROM customer_analytics_c360
        WHERE total_support_tickets > 0
        GROUP BY support_satisfaction_level
        ORDER BY avg_customer_value DESC
        """
        
        df = c360_data_manager.query_spark_sql(query, cache_key="support_insights")
        data = df.to_dict('records')
        
        return [CustomerSupportInsights(**record) for record in data]
    except Exception as e:
        handle_database_error("get_support_insights", e)


@app.get("/customer-success/lifecycle-analysis", 
         response_model=List[CustomerLifecycleAnalysis], 
         tags=["Customer Success"])
async def get_lifecycle_analysis():
    """
    Get customer lifecycle stage analysis
    
    **Use Case**: Understand customer behavior patterns across lifecycle stages
    for onboarding optimization and retention strategy development.
    """
    try:
        query = """
        SELECT 
            customer_tenure_segment,
            COUNT(*) as customer_count,
            ROUND(AVG(total_transactions), 1) as avg_transactions,
            ROUND(AVG(total_spent), 2) as avg_total_spent,
            ROUND(AVG(customer_health_score), 2) as avg_health_score,
            SUM(CASE WHEN customer_status = 'Active' THEN 1 ELSE 0 END) as active_customers
        FROM customer_analytics_c360
        GROUP BY customer_tenure_segment
        ORDER BY 
            CASE customer_tenure_segment
                WHEN 'New (0-30 days)' THEN 1
                WHEN 'Recent (31-90 days)' THEN 2
                WHEN 'Established (3-12 months)' THEN 3
                WHEN 'Veteran (1+ years)' THEN 4
            END
        """
        
        df = c360_data_manager.query_spark_sql(query, cache_key="lifecycle_analysis")
        data = df.to_dict('records')
        
        return [CustomerLifecycleAnalysis(**record) for record in data]
    except Exception as e:
        handle_database_error("get_lifecycle_analysis", e)


# ============================================================================
# ADVANCED ANALYTICS ENDPOINTS
# ============================================================================

@app.get("/analytics/customer-profiles", 
         response_model=List[CustomerProfile], 
         tags=["Analytics"])
async def get_customer_profiles(
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of results"),
    customer_status: Optional[List[CustomerStatus]] = Query(None, description="Filter by customer status"),
    min_health_score: Optional[float] = Query(None, ge=1.0, le=5.0, description="Minimum health score"),
    max_health_score: Optional[float] = Query(None, ge=1.0, le=5.0, description="Maximum health score")
):
    """
    Get detailed customer profiles with comprehensive C360 data
    
    **Use Case**: Detailed customer analysis for account management,
    personalized marketing, and customer success initiatives.
    """
    try:
        # Build dynamic query based on filters
        where_conditions = []
        
        if customer_status:
            status_list = "', '".join([status.value for status in customer_status])
            where_conditions.append(f"customer_status IN ('{status_list}')")
        
        if min_health_score is not None:
            where_conditions.append(f"customer_health_score >= {min_health_score}")
            
        if max_health_score is not None:
            where_conditions.append(f"customer_health_score <= {max_health_score}")
        
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        query = f"""
        SELECT *
        FROM customer_analytics_c360
        {where_clause}
        ORDER BY customer_health_score DESC, total_spent DESC
        LIMIT {limit}
        """
        
        df = c360_data_manager.query_spark_sql(query, cache_key=f"customer_profiles_{hash(str(where_conditions))}")
        data = df.to_dict('records')
        
        return [CustomerProfile(**record) for record in data]
    except Exception as e:
        handle_database_error("get_customer_profiles", e)


# ============================================================================
# ERROR HANDLERS
# ============================================================================

@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    """Handle HTTP exceptions"""
    return JSONResponse(
        status_code=exc.status_code,
        content=ErrorResponse(
            error_type="HTTPException",
            error_message=exc.detail,
            error_details={"status_code": exc.status_code}
        ).model_dump_json()
    )


@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    """Handle general exceptions"""
    logger.error("Unhandled exception", error=str(exc), path=request.url.path)
    return JSONResponse(
        status_code=500,
        content=ErrorResponse(
            error_type=type(exc).__name__,
            error_message="Internal server error",
            error_details={"original_error": str(exc)}
        ).model_dump_json()
    )


# ============================================================================
# MAIN APPLICATION ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    # Run the application
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True if ENVIRONMENT == "development" else False,
        log_level="info"
    )
