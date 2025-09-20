# Customer Analytics C360 - Quick Start Guide

## 🚀 Getting Started

### Prerequisites
- Apache Spark 3.x installed with `spark-sql` command available
- Mock CSV data in `../c360_mock_data/` directory



## 🎯 Business Intelligence Queries

Run demo business analysis queries:

#### In the same session as pipeline:
```sql
-- After running c360_consolidated_pipeline.sql, run:
SELECT * FROM customer_analytics_c360 LIMIT 10;
```

#### In a new session:
```bash
# First load the pipeline
spark-sql -f c360_consolidated_pipeline.sql

# Then run analysis queries  
spark-sql -f demo_queries.sql
```

## 📈 Key Demo Queries Available

1. **Customer Health Overview** - Status distribution and health metrics
2. **High-Value At-Risk Customers** - Premium customers needing retention
3. **Loyalty Program Effectiveness** - Tier performance analysis
4. **Digital Engagement Analysis** - App usage by generation
5. **Cross-sell & Upsell Opportunities** - Growth opportunity identification
6. **Customer Support Insights** - Satisfaction and ticket analysis
7. **Customer Lifecycle Analysis** - Tenure-based behavior patterns
8. **RFM Segmentation Matrix** - Advanced customer segmentation

## 🔧 Troubleshooting

### Common Issues:

**Error: "spark-sql: command not found"**
- Install Apache Spark: `brew install apache-spark` (macOS)
- Or download from: https://spark.apache.org/downloads.html

**Error: "path not found"**
- Ensure you're in the `c360_spark_processing` directory
- Verify `../c360_mock_data/` exists with CSV files

**Error: "read_csv function not found"**  
- ✅ **Fixed!** Use the consolidated pipeline which uses proper Spark SQL syntax

## 🎯 Integration Examples

### Connect to BI Tools
```sql
-- Query the data product directly
SELECT customer_id, customer_segment, loyalty_tier, 
       customer_health_score, total_spent
FROM customer_analytics_c360
WHERE customer_status = 'Active';
```

### ML Feature Extraction
```sql  
-- Extract features for churn prediction model
SELECT customer_id, recency_score, frequency_score, monetary_score,
       total_app_sessions, avg_satisfaction, churn_risk_flag
FROM customer_analytics_c360;
```

### Marketing Campaigns
```sql
-- Target high-value at-risk customers
SELECT customer_id, email, first_name, last_name
FROM customer_analytics_c360  
WHERE churn_risk_flag = 1 AND lifetime_value > 1000;
```

## 📚 File Structure

```
c360_spark_processing/
├── 📄 c360_consolidated_pipeline.sql    # Main pipeline (use this!)
├── 📄 demo_queries.sql                  # Business analysis examples  
├── 🔧 run_c360_pipeline.sh             # Complete execution script
├── 🔧 test_pipeline.sh                 # Basic functionality test
├── 📖 README.md                        # Detailed documentation
├── 📖 QUICKSTART.md                    # This file
└── [sources/, intermediates/, facts/, views/]  # Individual components (reference only)
```

## 🤝 Support

- **Issues**: Check the detailed README.md for troubleshooting
- **Business Questions**: Review demo_queries.sql for analysis examples
- **Technical Details**: See comprehensive documentation in README.md

---

**Ready to start?** Run `./run_c360_pipeline.sh` and explore your Customer 360 data! 🎉
