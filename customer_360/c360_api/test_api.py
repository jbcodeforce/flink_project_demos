#!/usr/bin/env python3
"""
Customer Analytics C360 API - Test Script
Simple test script to verify API functionality
"""

import requests
import json
import time
import sys
from typing import Dict, Any


class C360APITester:
    """Test the C360 API endpoints"""
    
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()
        
    def test_health_check(self) -> bool:
        """Test API health endpoint"""
        try:
            response = self.session.get(f"{self.base_url}/health", timeout=10)
            if response.status_code == 200:
                data = response.json()
                print(f"✅ Health Check: {data['status']} (v{data['version']})")
                print(f"   📊 Total customers: {data.get('total_customers', 'N/A')}")
                return True
            else:
                print(f"❌ Health Check failed: HTTP {response.status_code}")
                return False
        except requests.exceptions.RequestException as e:
            print(f"❌ Health Check failed: {e}")
            return False
    
    def test_marketing_endpoints(self):
        """Test marketing use case endpoints"""
        print("\n📈 Testing Marketing Endpoints:")
        
        endpoints = [
            ("/marketing/customer-health-overview", "Customer Health Overview"),
            ("/marketing/churn-risk-customers?min_lifetime_value=1000&limit=5", "Churn Risk Customers"),
            ("/marketing/loyalty-program-metrics", "Loyalty Program Metrics"),
            ("/marketing/customer-segmentation", "Customer Segmentation")
        ]
        
        for endpoint, name in endpoints:
            self._test_endpoint(endpoint, name)
    
    def test_product_endpoints(self):
        """Test product use case endpoints"""
        print("\n🛍️ Testing Product Endpoints:")
        
        endpoints = [
            ("/product/digital-engagement-analysis", "Digital Engagement Analysis"),
            ("/product/cross-sell-opportunities?limit=5", "Cross-sell Opportunities"),
            ("/product/category-insights", "Category Insights")
        ]
        
        for endpoint, name in endpoints:
            self._test_endpoint(endpoint, name)
    
    def test_finance_endpoints(self):
        """Test finance use case endpoints"""
        print("\n💰 Testing Finance Endpoints:")
        
        endpoints = [
            ("/finance/customer-lifetime-value", "Customer Lifetime Value"),
            ("/finance/revenue-analysis", "Revenue Analysis"),
            ("/finance/rfm-segmentation", "RFM Segmentation"),
            ("/finance/profitability-metrics", "Profitability Metrics")
        ]
        
        for endpoint, name in endpoints:
            self._test_endpoint(endpoint, name)
    
    def test_customer_success_endpoints(self):
        """Test customer success use case endpoints"""
        print("\n👥 Testing Customer Success Endpoints:")
        
        endpoints = [
            ("/customer-success/support-insights", "Support Insights"),
            ("/customer-success/lifecycle-analysis", "Lifecycle Analysis")
        ]
        
        for endpoint, name in endpoints:
            self._test_endpoint(endpoint, name)
    
    def test_analytics_endpoints(self):
        """Test advanced analytics endpoints"""
        print("\n🔬 Testing Advanced Analytics Endpoints:")
        
        endpoints = [
            ("/analytics/customer-profiles?limit=3", "Customer Profiles (Limited)"),
            ("/analytics/customer-profiles?min_health_score=3.0&limit=2", "High Health Score Customers")
        ]
        
        for endpoint, name in endpoints:
            self._test_endpoint(endpoint, name)
    
    def _test_endpoint(self, endpoint: str, name: str):
        """Test a single endpoint"""
        try:
            start_time = time.time()
            response = self.session.get(f"{self.base_url}{endpoint}", timeout=30)
            duration = time.time() - start_time
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    if isinstance(data, dict) and data.get('success'):
                        count = len(data.get('data', []))
                        print(f"   ✅ {name}: {count} records ({duration:.1f}s)")
                        
                        # Show sample data for first few endpoints
                        if count > 0 and len(name) < 20:  # Simple check for main endpoints
                            sample = data['data'][0] if isinstance(data['data'], list) else data['data']
                            sample_keys = list(sample.keys())[:3] if isinstance(sample, dict) else []
                            if sample_keys:
                                print(f"      📄 Sample fields: {', '.join(sample_keys)}")
                    elif isinstance(data, list):
                        print(f"   ✅ {name}: {len(data)} records ({duration:.1f}s)")
                    else:
                        print(f"   ⚠️  {name}: Unexpected response format ({duration:.1f}s)")
                except json.JSONDecodeError:
                    print(f"   ⚠️  {name}: Invalid JSON response ({duration:.1f}s)")
            else:
                print(f"   ❌ {name}: HTTP {response.status_code} ({duration:.1f}s)")
                if response.status_code >= 500:
                    print(f"      Error: {response.text[:100]}")
                
        except requests.exceptions.Timeout:
            print(f"   ⏰ {name}: Timeout (>30s)")
        except requests.exceptions.RequestException as e:
            print(f"   ❌ {name}: Request failed - {e}")
    
    def test_admin_endpoints(self):
        """Test admin endpoints"""
        print("\n🔧 Testing Admin Endpoints:")
        
        # Test metrics endpoint
        self._test_endpoint("/admin/metrics", "API Metrics")
        
        # Test data refresh (non-blocking)
        try:
            response = self.session.post(f"{self.base_url}/admin/refresh-data", timeout=10)
            if response.status_code == 200:
                print("   ✅ Data Refresh: Initiated successfully")
            else:
                print(f"   ❌ Data Refresh: HTTP {response.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"   ❌ Data Refresh: {e}")
    
    def demonstrate_business_use_cases(self):
        """Demonstrate business use cases with sample queries"""
        print("\n🎯 Business Use Case Demonstrations:")
        
        # Marketing: Churn Risk Analysis
        print("\n📈 Marketing Use Case: Churn Risk Analysis")
        try:
            response = self.session.get(
                f"{self.base_url}/marketing/churn-risk-customers?min_lifetime_value=2000&limit=3"
            )
            if response.status_code == 200:
                data = response.json()
                customers = data.get('data', [])
                if customers:
                    print(f"   🎯 Found {len(customers)} high-value at-risk customers:")
                    for customer in customers[:2]:  # Show top 2
                        print(f"      • {customer.get('customer_name', 'N/A')} "
                              f"({customer.get('customer_segment', 'N/A')}) - "
                              f"${customer.get('total_spent', 0):.2f} spent")
                else:
                    print("   📊 No at-risk customers found (good news!)")
        except Exception as e:
            print(f"   ❌ Marketing demo failed: {e}")
        
        # Finance: Revenue Analysis
        print("\n💰 Finance Use Case: Revenue by Segment")
        try:
            response = self.session.get(f"{self.base_url}/finance/revenue-analysis")
            if response.status_code == 200:
                data = response.json()
                segments = data.get('data', [])
                if segments:
                    print("   💰 Revenue by customer segment:")
                    for segment in segments:
                        print(f"      • {segment.get('customer_segment', 'N/A')}: "
                              f"${segment.get('total_revenue', 0):.2f} "
                              f"({segment.get('customer_count', 0)} customers)")
        except Exception as e:
            print(f"   ❌ Finance demo failed: {e}")
        
        # Product: Digital Engagement
        print("\n🛍️ Product Use Case: Digital Engagement")
        try:
            response = self.session.get(f"{self.base_url}/product/digital-engagement-analysis")
            if response.status_code == 200:
                data = response.json()
                generations = data.get('data', [])
                if generations:
                    print("   📱 App adoption by generation:")
                    for gen in generations:
                        adoption_rate = gen.get('app_adoption_rate_pct', 0)
                        print(f"      • {gen.get('generation_segment', 'N/A')}: "
                              f"{adoption_rate}% app adoption rate")
        except Exception as e:
            print(f"   ❌ Product demo failed: {e}")
    
    def run_comprehensive_test(self):
        """Run all tests"""
        print("🧪 Customer Analytics C360 API - Comprehensive Test Suite")
        print("=" * 60)
        
        # Check if API is running
        if not self.test_health_check():
            print("\n❌ API is not running or not responding")
            print("💡 Start the API with: python main.py")
            return False
        
        # Test all endpoint categories
        self.test_marketing_endpoints()
        self.test_product_endpoints()
        self.test_finance_endpoints()
        self.test_customer_success_endpoints()
        self.test_analytics_endpoints()
        self.test_admin_endpoints()
        
        # Business use case demonstrations
        self.demonstrate_business_use_cases()
        
        print("\n🎉 Test Suite Completed!")
        print("💡 Visit http://localhost:8000/docs for interactive API documentation")
        
        return True


def main():
    """Main test function"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Test Customer Analytics C360 API")
    parser.add_argument("--url", default="http://localhost:8000", 
                       help="API base URL (default: http://localhost:8000)")
    parser.add_argument("--quick", action="store_true", 
                       help="Run only quick health check")
    
    args = parser.parse_args()
    
    tester = C360APITester(args.url)
    
    if args.quick:
        success = tester.test_health_check()
        sys.exit(0 if success else 1)
    else:
        success = tester.run_comprehensive_test()
        sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
