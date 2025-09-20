"""
Customer Analytics C360 API - Pydantic Models
Data models for API requests and responses
"""

from datetime import datetime, date
from decimal import Decimal
from enum import Enum
from typing import List, Optional, Union
from pydantic import BaseModel, Field


# ============================================================================
# ENUMS AND CONSTANTS
# ============================================================================

class CustomerStatus(str, Enum):
    """Customer status based on purchase activity"""
    ACTIVE = "Active"
    AT_RISK = "At Risk"
    CHURNED = "Churned"
    NEVER_PURCHASED = "Never Purchased"


class CustomerSegment(str, Enum):
    """Customer value segments"""
    PREMIUM = "Premium"
    STANDARD = "Standard"
    BASIC = "Basic"


class LoyaltyTier(str, Enum):
    """Loyalty program tiers"""
    PLATINUM = "Platinum"
    GOLD = "Gold"
    SILVER = "Silver"
    BRONZE = "Bronze"


class GenerationSegment(str, Enum):
    """Customer generation segments"""
    GEN_Z = "Gen Z"
    MILLENNIAL = "Millennial"
    GEN_X = "Gen X"
    BOOMER_PLUS = "Boomer+"


class Channel(str, Enum):
    """Sales and communication channels"""
    ONLINE = "online"
    MOBILE = "mobile"
    STORE = "store"


# ============================================================================
# BASE CUSTOMER MODELS
# ============================================================================

class CustomerBase(BaseModel):
    """Base customer information"""
    customer_id: str = Field(..., description="Unique customer identifier")
    first_name: str = Field(..., description="Customer first name")
    last_name: str = Field(..., description="Customer last name")
    email: str = Field(..., description="Customer email address")


class CustomerProfile(CustomerBase):
    """Complete customer profile for C360 analytics"""
    customer_segment: CustomerSegment
    preferred_channel: Channel
    generation_segment: GenerationSegment
    age_years: float
    city: str
    state: str
    country: str
    
    # Loyalty program
    loyalty_tier: LoyaltyTier
    points_balance: int
    lifetime_value: float  # Changed from Decimal to float for JSON serialization
    
    # Transaction metrics
    total_transactions: int
    total_spent: float  # Changed from Decimal to float for JSON serialization
    avg_order_value: float  # Changed from Decimal to float for JSON serialization
    first_purchase_date: Optional[date]
    last_purchase_date: Optional[date]
    
    # Customer health
    customer_status: CustomerStatus
    customer_health_score: float = Field(..., ge=1.0, le=5.0, description="Customer health score (1-5)")
    churn_risk_flag: bool
    
    # Digital engagement
    total_app_sessions: int
    app_engagement_score: float
    is_digital_native: bool
    
    # Support metrics
    total_support_tickets: int
    avg_satisfaction: Optional[float]
    
    # Metadata
    profile_created_at: datetime


# ============================================================================
# MARKETING USE CASE MODELS
# ============================================================================

class CustomerHealthOverview(BaseModel):
    """Customer health distribution overview"""
    customer_status: CustomerStatus
    customer_count: int
    avg_health_score: float
    avg_total_spent: float  # Changed from Decimal to float for JSON serialization
    avg_transactions: float


class ChurnRiskCustomer(CustomerBase):
    """High-value customer at risk of churn"""
    customer_segment: CustomerSegment
    loyalty_tier: LoyaltyTier
    total_spent: float  # Changed from Decimal to float for JSON serialization
    last_purchase_date: Optional[date]
    customer_health_score: float
    risk_status: str
    lifetime_value: float  # Changed from Decimal to float for JSON serialization


class CustomerSegmentation(BaseModel):
    """Customer segmentation analysis"""
    segment_name: str
    customer_count: int
    avg_lifetime_value: float  # Changed from Decimal to float for JSON serialization
    avg_health_score: float
    churn_risk_percentage: float


class LoyaltyProgramMetrics(BaseModel):
    """Loyalty program effectiveness metrics"""
    loyalty_tier: LoyaltyTier
    customer_count: int
    avg_lifetime_value: float  # Changed from Decimal to float for JSON serialization
    avg_total_spent: float  # Changed from Decimal to float for JSON serialization
    avg_points_balance: int
    avg_redemption_rate_pct: float


# ============================================================================
# PRODUCT USE CASE MODELS
# ============================================================================

class DigitalEngagementAnalysis(BaseModel):
    """Digital engagement by generation"""
    generation_segment: GenerationSegment
    total_customers: int
    app_users: int
    digital_natives: int
    avg_engagement_score: float
    app_adoption_rate_pct: float


class CrossSellOpportunity(CustomerBase):
    """Cross-sell and upsell opportunity"""
    customer_segment: CustomerSegment
    loyalty_tier: LoyaltyTier
    channels_used: int
    total_spent: float  # Changed from Decimal to float for JSON serialization
    opportunities: str = Field(..., description="Comma-separated list of opportunities")


class ProductCategoryInsights(BaseModel):
    """Product category performance insights"""
    category: str
    customer_count: int
    avg_order_value: float  # Changed from Decimal to float for JSON serialization
    total_revenue: float  # Changed from Decimal to float for JSON serialization
    repeat_purchase_rate: float


# ============================================================================
# FINANCE USE CASE MODELS
# ============================================================================

class CustomerLifetimeValue(BaseModel):
    """Customer lifetime value analysis"""
    value_segment: str
    customer_count: int
    avg_lifetime_value: float  # Changed from Decimal to float for JSON serialization
    total_revenue: float  # Changed from Decimal to float for JSON serialization
    avg_transactions_per_customer: float


class RevenueAnalysis(BaseModel):
    """Revenue analysis by customer segment"""
    customer_segment: CustomerSegment
    customer_count: int
    total_revenue: float  # Changed from Decimal to float for JSON serialization
    avg_revenue_per_customer: float  # Changed from Decimal to float for JSON serialization
    percentage_of_total_revenue: float


class RFMSegmentation(BaseModel):
    """RFM (Recency, Frequency, Monetary) segmentation"""
    recency_score: int = Field(..., ge=1, le=5)
    frequency_score: int = Field(..., ge=1, le=5)
    monetary_score: int = Field(..., ge=1, le=5)
    customer_count: int
    avg_total_spent: float  # Changed from Decimal to float for JSON serialization
    business_segment: str


class ProfitabilityMetrics(BaseModel):
    """Customer profitability metrics"""
    customer_tier: str
    customer_count: int
    total_revenue: float  # Changed from Decimal to float for JSON serialization
    avg_profit_margin: float
    cost_to_serve: float  # Changed from Decimal to float for JSON serialization


# ============================================================================
# CUSTOMER SUCCESS USE CASE MODELS
# ============================================================================

class CustomerSupportInsights(BaseModel):
    """Customer support satisfaction insights"""
    support_satisfaction_level: str
    customer_count: int
    avg_tickets_per_customer: float
    avg_satisfaction_score: float
    total_urgent_tickets: int
    avg_customer_value: float  # Changed from Decimal to float for JSON serialization


class CustomerLifecycleAnalysis(BaseModel):
    """Customer lifecycle stage analysis"""
    customer_tenure_segment: str
    customer_count: int
    avg_transactions: float
    avg_total_spent: float  # Changed from Decimal to float for JSON serialization
    avg_health_score: float
    active_customers: int


# ============================================================================
# API RESPONSE WRAPPERS
# ============================================================================

class APIResponse(BaseModel):
    """Generic API response wrapper"""
    success: bool = True
    message: str = "Request processed successfully"
    timestamp: datetime = Field(default_factory=datetime.now)
    data: Union[List[dict], dict, None] = None
    count: Optional[int] = None


class PaginatedResponse(APIResponse):
    """Paginated API response"""
    page: int = Field(..., ge=1, description="Current page number")
    page_size: int = Field(..., ge=1, le=1000, description="Items per page")
    total_pages: int = Field(..., ge=0, description="Total number of pages")
    has_next: bool = Field(..., description="Whether there are more pages")
    has_previous: bool = Field(..., description="Whether there are previous pages")


class ErrorResponse(BaseModel):
    """Error response model"""
    success: bool = False
    error_type: str
    error_message: str
    error_details: Optional[dict] = None
    timestamp: datetime = Field(default_factory=datetime.now)


# ============================================================================
# REQUEST MODELS
# ============================================================================

class CustomerQuery(BaseModel):
    """Customer query parameters"""
    customer_status: Optional[List[CustomerStatus]] = None
    customer_segment: Optional[List[CustomerSegment]] = None
    loyalty_tier: Optional[List[LoyaltyTier]] = None
    min_lifetime_value: Optional[float] = Field(None, ge=0)  # Changed from Decimal to float for JSON serialization
    max_lifetime_value: Optional[float] = Field(None, ge=0)  # Changed from Decimal to float for JSON serialization
    min_health_score: Optional[float] = Field(None, ge=1.0, le=5.0)
    max_health_score: Optional[float] = Field(None, ge=1.0, le=5.0)
    churn_risk_only: Optional[bool] = False
    has_app_usage: Optional[bool] = None
    limit: int = Field(100, ge=1, le=1000, description="Maximum number of results")


class AnalyticsTimeframe(BaseModel):
    """Analytics timeframe parameters"""
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    include_inactive: bool = Field(True, description="Include inactive customers in analysis")


class PaginationParams(BaseModel):
    """Pagination parameters"""
    page: int = Field(1, ge=1, description="Page number (1-based)")
    page_size: int = Field(50, ge=1, le=1000, description="Items per page")


# ============================================================================
# CONFIGURATION MODELS
# ============================================================================

class HealthCheckResponse(BaseModel):
    """Health check response"""
    status: str = "healthy"
    version: str
    environment: str
    timestamp: datetime = Field(default_factory=datetime.now)
    database_status: str
    spark_status: str
    total_customers: Optional[int] = None


class MetricsResponse(BaseModel):
    """API metrics response"""
    total_requests: int
    avg_response_time_ms: float
    error_rate: float
    uptime_seconds: int
    cache_hit_rate: float
    timestamp: datetime = Field(default_factory=datetime.now)
