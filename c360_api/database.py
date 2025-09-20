"""
Customer Analytics C360 API - Database Integration
Handles Spark connection and data querying for the C360 pipeline
"""

import os
import subprocess
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
from typing import List, Dict, Any, Optional
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import structlog

logger = structlog.get_logger(__name__)


class C360DataManager:
    """
    Manages data access for the Customer 360 analytics pipeline
    Provides both Spark integration and cached query results
    """
    
    def __init__(self, 
                 spark_app_name: str = "C360_API",
                 c360_data_path: str = "../c360_mock_data",
                 pipeline_path: str = "../c360_spark_processing",
                 cache_ttl_minutes: int = 30):
        """
        Initialize the C360 Data Manager
        
        Args:
            spark_app_name: Name for the Spark application
            c360_data_path: Path to the mock CSV data
            pipeline_path: Path to the Spark processing pipeline
            cache_ttl_minutes: Cache time-to-live in minutes
        """
        self.spark_app_name = spark_app_name
        self.c360_data_path = Path(c360_data_path)
        self.pipeline_path = Path(pipeline_path)
        self.cache_ttl = timedelta(minutes=cache_ttl_minutes)
        self.spark: Optional[SparkSession] = None
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._last_pipeline_run: Optional[datetime] = None
        
    def get_spark_session(self) -> SparkSession:
        """Get or create Spark session"""
        if self.spark is None:
            self.spark = SparkSession.builder \
                .appName(self.spark_app_name) \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .getOrCreate()
            
            # Set log level to reduce noise
            self.spark.sparkContext.setLogLevel("WARN")
            
            logger.info("Spark session created", app_name=self.spark_app_name)
            
        return self.spark
    
    def close_spark_session(self):
        """Close Spark session and cleanup"""
        if self.spark is not None:
            self.spark.stop()
            self.spark = None
            logger.info("Spark session closed")
    
    def run_c360_pipeline(self, force_refresh: bool = False) -> bool:
        """
        Run the C360 pipeline to refresh data
        
        Args:
            force_refresh: Force pipeline refresh even if recently run
            
        Returns:
            True if pipeline ran successfully, False otherwise
        """
        try:
            # Check if pipeline was recently run
            if not force_refresh and self._last_pipeline_run:
                time_since_run = datetime.now() - self._last_pipeline_run
                if time_since_run < self.cache_ttl:
                    logger.info("Pipeline recently run, skipping", 
                              time_since_run=str(time_since_run))
                    return True
            
            logger.info("Running C360 pipeline", pipeline_path=str(self.pipeline_path))
            
            # Run the consolidated pipeline using spark-sql
            pipeline_file = self.pipeline_path / "c360_consolidated_pipeline.sql"
            if not pipeline_file.exists():
                raise FileNotFoundError(f"Pipeline file not found: {pipeline_file}")
            
            # Change to the pipeline directory
            original_cwd = os.getcwd()
            os.chdir(self.pipeline_path)
            
            try:
                # Run spark-sql command
                result = subprocess.run([
                    "spark-sql", 
                    "-f", "c360_consolidated_pipeline.sql",
                    "--silent"
                ], capture_output=True, text=True, timeout=300)
                
                if result.returncode == 0:
                    self._last_pipeline_run = datetime.now()
                    self._cache.clear()  # Clear cache after pipeline refresh
                    logger.info("C360 pipeline completed successfully")
                    return True
                else:
                    logger.error("Pipeline failed", 
                               stdout=result.stdout, 
                               stderr=result.stderr)
                    return False
                    
            finally:
                os.chdir(original_cwd)
                
        except subprocess.TimeoutExpired:
            logger.error("Pipeline execution timed out")
            return False
        except Exception as e:
            logger.error("Pipeline execution failed", error=str(e))
            return False
    
    def query_spark_sql(self, query: str, cache_key: Optional[str] = None) -> pd.DataFrame:
        """
        Execute a Spark SQL query and return results as pandas DataFrame
        
        Args:
            query: SQL query to execute
            cache_key: Optional cache key for storing results
            
        Returns:
            pandas DataFrame with query results
        """
        # Check cache first
        if cache_key and cache_key in self._cache:
            cached_data = self._cache[cache_key]
            if datetime.now() - cached_data['timestamp'] < self.cache_ttl:
                logger.debug("Returning cached results", cache_key=cache_key)
                return pd.DataFrame(cached_data['data'])
        
        try:
            # Ensure pipeline has been run recently
            if not self._last_pipeline_run or \
               datetime.now() - self._last_pipeline_run > self.cache_ttl:
                logger.info("Running pipeline before query execution")
                if not self.run_c360_pipeline():
                    raise RuntimeError("Failed to run C360 pipeline")
            
            # Execute query using spark-sql
            with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
                # Add pipeline setup and query
                full_query = f"""
                -- Load C360 pipeline
                source {self.pipeline_path}/c360_consolidated_pipeline.sql;
                
                -- Execute user query
                {query};
                """
                f.write(full_query)
                temp_sql_file = f.name
            
            try:
                # Change to pipeline directory for relative paths to work
                original_cwd = os.getcwd()
                os.chdir(self.pipeline_path)
                
                # Execute query
                result = subprocess.run([
                    "spark-sql", 
                    "-f", temp_sql_file,
                    "--silent"
                ], capture_output=True, text=True, timeout=120)
                
                if result.returncode != 0:
                    raise RuntimeError(f"Query execution failed: {result.stderr}")
                
                # Parse results (this is a simplified approach)
                # In production, you'd want more robust result parsing
                lines = result.stdout.strip().split('\n')
                if not lines or not lines[0]:
                    return pd.DataFrame()
                
                # Convert to DataFrame (simplified parsing)
                # This assumes tab-separated output from spark-sql
                data = []
                for line in lines:
                    if line.strip():
                        data.append(line.split('\t'))
                
                if data:
                    df = pd.DataFrame(data[1:], columns=data[0] if len(data) > 1 else None)
                else:
                    df = pd.DataFrame()
                
                # Cache results
                if cache_key:
                    self._cache[cache_key] = {
                        'data': df.to_dict('records'),
                        'timestamp': datetime.now()
                    }
                
                logger.info("Query executed successfully", 
                          rows=len(df), 
                          cache_key=cache_key)
                return df
                
            finally:
                os.chdir(original_cwd)
                os.unlink(temp_sql_file)
                
        except subprocess.TimeoutExpired:
            logger.error("Query execution timed out")
            raise RuntimeError("Query execution timed out")
        except Exception as e:
            logger.error("Query execution failed", error=str(e))
            raise
    
    def get_customer_health_overview(self) -> List[Dict[str, Any]]:
        """Get customer health distribution overview"""
        query = """
        SELECT 
            customer_status,
            COUNT(*) as customer_count,
            ROUND(AVG(customer_health_score), 2) as avg_health_score,
            ROUND(AVG(total_spent), 2) as avg_total_spent,
            ROUND(AVG(total_transactions), 1) as avg_transactions
        FROM customer_analytics_c360
        GROUP BY customer_status
        ORDER BY customer_count DESC
        """
        df = self.query_spark_sql(query, cache_key="customer_health_overview")
        
        # Convert DataFrame to records and ensure proper data types for JSON serialization
        records = df.to_dict('records')
        for record in records:
            # Convert numeric strings to proper types
            if 'customer_count' in record:
                record['customer_count'] = int(float(record['customer_count']))
            if 'avg_health_score' in record:
                record['avg_health_score'] = float(record['avg_health_score'])
            if 'avg_total_spent' in record:
                record['avg_total_spent'] = float(record['avg_total_spent'])
            if 'avg_transactions' in record:
                record['avg_transactions'] = float(record['avg_transactions'])
        
        return records
    
    def get_churn_risk_customers(self, min_lifetime_value: float = 1000, limit: int = 50) -> List[Dict[str, Any]]:
        """Get high-value customers at risk of churn"""
        query = f"""
        SELECT 
            customer_id,
            first_name || ' ' || last_name as customer_name,
            customer_segment,
            loyalty_tier,
            total_spent,
            last_purchase_date,
            customer_health_score,
            lifetime_value,
            CASE 
                WHEN churn_risk_flag = 1 THEN 'CHURN RISK' 
                ELSE 'Stable'
            END as risk_status
        FROM customer_analytics_c360
        WHERE lifetime_value > {min_lifetime_value}
          AND churn_risk_flag = 1
        ORDER BY total_spent DESC
        LIMIT {limit}
        """
        df = self.query_spark_sql(query, cache_key=f"churn_risk_{min_lifetime_value}_{limit}")
        return df.to_dict('records')
    
    def get_loyalty_program_metrics(self) -> List[Dict[str, Any]]:
        """Get loyalty program effectiveness metrics"""
        query = """
        SELECT 
            loyalty_tier,
            COUNT(*) as customer_count,
            ROUND(AVG(lifetime_value), 2) as avg_lifetime_value,
            ROUND(AVG(total_spent), 2) as avg_total_spent,
            ROUND(AVG(points_balance), 0) as avg_points_balance,
            ROUND(AVG(redemption_rate) * 100, 1) as avg_redemption_rate_pct
        FROM customer_analytics_c360
        GROUP BY loyalty_tier
        ORDER BY avg_lifetime_value DESC
        """
        df = self.query_spark_sql(query, cache_key="loyalty_program_metrics")
        return df.to_dict('records')
    
    def get_digital_engagement_analysis(self) -> List[Dict[str, Any]]:
        """Get digital engagement analysis by generation"""
        query = """
        SELECT 
            generation_segment,
            COUNT(*) as total_customers,
            SUM(is_app_user) as app_users,
            SUM(is_digital_native) as digital_natives,
            ROUND(AVG(app_engagement_score), 1) as avg_engagement_score,
            ROUND(SUM(is_app_user) * 100.0 / COUNT(*), 1) as app_adoption_rate_pct
        FROM customer_analytics_c360
        GROUP BY generation_segment
        ORDER BY app_adoption_rate_pct DESC
        """
        df = self.query_spark_sql(query, cache_key="digital_engagement_analysis")
        return df.to_dict('records')
    
    def get_cross_sell_opportunities(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get cross-sell and upsell opportunities"""
        query = f"""
        SELECT 
            customer_id,
            first_name || ' ' || last_name as customer_name,
            customer_segment,
            loyalty_tier,
            channels_used,
            total_spent,
            CASE WHEN channel_expansion_opportunity = 1 THEN 'Channel Expansion' ELSE '' END ||
            CASE WHEN tier_upgrade_opportunity = 1 THEN ' Tier Upgrade' ELSE '' END ||
            CASE WHEN app_adoption_opportunity = 1 THEN ' App Adoption' ELSE '' END as opportunities
        FROM customer_analytics_c360
        WHERE channel_expansion_opportunity = 1 
           OR tier_upgrade_opportunity = 1 
           OR app_adoption_opportunity = 1
        ORDER BY total_spent DESC
        LIMIT {limit}
        """
        df = self.query_spark_sql(query, cache_key=f"cross_sell_opportunities_{limit}")
        return df.to_dict('records')
    
    def get_customer_lifetime_value_analysis(self) -> List[Dict[str, Any]]:
        """Get customer lifetime value analysis by segment"""
        query = """
        SELECT 
            value_segment,
            COUNT(*) as customer_count,
            ROUND(AVG(lifetime_value), 2) as avg_lifetime_value,
            ROUND(SUM(total_spent), 2) as total_revenue,
            ROUND(AVG(total_transactions), 1) as avg_transactions_per_customer
        FROM customer_analytics_c360
        GROUP BY value_segment
        ORDER BY avg_lifetime_value DESC
        """
        df = self.query_spark_sql(query, cache_key="customer_lifetime_value_analysis")
        return df.to_dict('records')
    
    def get_rfm_segmentation(self) -> List[Dict[str, Any]]:
        """Get RFM segmentation matrix"""
        query = """
        SELECT 
            recency_score,
            frequency_score,
            monetary_score,
            COUNT(*) as customer_count,
            ROUND(AVG(total_spent), 2) as avg_total_spent,
            CASE 
                WHEN recency_score >= 4 AND frequency_score >= 4 AND monetary_score >= 4 THEN 'Champions'
                WHEN recency_score >= 3 AND frequency_score >= 3 AND monetary_score >= 4 THEN 'Loyal Customers'
                WHEN recency_score >= 4 AND frequency_score <= 2 AND monetary_score >= 3 THEN 'Big Spenders'
                WHEN recency_score >= 4 AND frequency_score >= 3 AND monetary_score <= 2 THEN 'Potential Loyalists'
                WHEN recency_score <= 2 AND frequency_score >= 3 AND monetary_score >= 3 THEN 'At Risk'
                WHEN recency_score <= 2 AND frequency_score <= 2 THEN 'Hibernating'
                ELSE 'Others'
            END as business_segment
        FROM customer_analytics_c360
        GROUP BY recency_score, frequency_score, monetary_score
        ORDER BY recency_score DESC, frequency_score DESC, monetary_score DESC
        """
        df = self.query_spark_sql(query, cache_key="rfm_segmentation")
        return df.to_dict('records')
    
    def get_data_product_health_check(self) -> Dict[str, Any]:
        """Get data product health check information"""
        query = """
        SELECT 
            COUNT(*) as total_customers_processed,
            COUNT(CASE WHEN email IS NOT NULL THEN 1 END) as customers_with_email,
            COUNT(CASE WHEN total_transactions > 0 THEN 1 END) as customers_with_purchases,
            COUNT(CASE WHEN total_app_sessions > 0 THEN 1 END) as app_engaged_customers,
            COUNT(CASE WHEN total_support_tickets > 0 THEN 1 END) as customers_with_support_history,
            ROUND(AVG(customer_health_score), 2) as overall_health_score,
            MAX(profile_created_at) as last_profile_update
        FROM customer_analytics_c360
        """
        df = self.query_spark_sql(query, cache_key="data_product_health_check")
        return df.to_dict('records')[0] if len(df) > 0 else {}


# Global instance
c360_data_manager = C360DataManager()
