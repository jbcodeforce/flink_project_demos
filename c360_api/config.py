"""
Customer Analytics C360 API - Configuration
Application configuration and environment settings
"""

import os
from typing import Optional
from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    """Application settings loaded from environment variables"""
    
    # Application settings
    app_name: str = Field(default="Customer Analytics C360 API", description="Application name")
    app_version: str = Field(default="1.0.0", description="Application version")
    environment: str = Field(default="development", description="Environment (development, staging, production)")
    debug: bool = Field(default=True, description="Enable debug mode")
    
    # API settings
    api_host: str = Field(default="0.0.0.0", description="API host address")
    api_port: int = Field(default=8000, description="API port number")
    api_workers: int = Field(default=1, description="Number of API workers")
    
    # CORS settings
    cors_origins: str = Field(default="*", description="Comma-separated list of CORS origins")
    cors_methods: str = Field(default="*", description="Comma-separated list of CORS methods")
    cors_headers: str = Field(default="*", description="Comma-separated list of CORS headers")
    
    # Data pipeline settings
    c360_data_path: str = Field(default="../c360_mock_data", description="Path to C360 mock data")
    pipeline_path: str = Field(default="../c360_spark_processing", description="Path to Spark processing pipeline")
    cache_ttl_minutes: int = Field(default=30, description="Cache TTL in minutes")
    
    # Spark settings
    spark_app_name: str = Field(default="C360_API", description="Spark application name")
    spark_master: str = Field(default="local[*]", description="Spark master URL")
    spark_executor_memory: str = Field(default="2g", description="Spark executor memory")
    spark_driver_memory: str = Field(default="2g", description="Spark driver memory")
    
    # Database settings (for future use)
    database_url: Optional[str] = Field(default=None, description="Database URL for persistent storage")
    redis_url: Optional[str] = Field(default="redis://localhost:6379", description="Redis URL for caching")
    
    # Logging settings
    log_level: str = Field(default="INFO", description="Logging level")
    log_format: str = Field(default="json", description="Log format (json, text)")
    
    # Security settings
    api_key: Optional[str] = Field(default=None, description="API key for authentication")
    secret_key: str = Field(default="your-secret-key-change-in-production", description="Secret key for JWT")
    
    # Rate limiting
    rate_limit_per_minute: int = Field(default=100, description="API calls per minute per client")
    
    # Monitoring settings
    enable_metrics: bool = Field(default=True, description="Enable metrics collection")
    metrics_port: int = Field(default=9090, description="Metrics server port")
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
        
    @property
    def cors_origins_list(self) -> list:
        """Get CORS origins as a list"""
        if self.cors_origins == "*":
            return ["*"]
        return [origin.strip() for origin in self.cors_origins.split(",")]
    
    @property
    def cors_methods_list(self) -> list:
        """Get CORS methods as a list"""
        if self.cors_methods == "*":
            return ["*"]
        return [method.strip() for method in self.cors_methods.split(",")]
    
    @property
    def cors_headers_list(self) -> list:
        """Get CORS headers as a list"""
        if self.cors_headers == "*":
            return ["*"]
        return [header.strip() for header in self.cors_headers.split(",")]


# Global settings instance
settings = Settings()


# Environment-specific configurations
def get_environment_config(environment: str) -> dict:
    """Get environment-specific configuration overrides"""
    configs = {
        "development": {
            "debug": True,
            "log_level": "DEBUG",
            "cache_ttl_minutes": 5,  # Shorter cache for development
            "rate_limit_per_minute": 1000,  # Higher limit for development
        },
        "staging": {
            "debug": False,
            "log_level": "INFO",
            "cache_ttl_minutes": 15,
            "rate_limit_per_minute": 200,
        },
        "production": {
            "debug": False,
            "log_level": "WARNING",
            "cache_ttl_minutes": 30,
            "rate_limit_per_minute": 100,
            "cors_origins": "https://yourdomain.com,https://app.yourdomain.com",
        }
    }
    
    return configs.get(environment, {})
