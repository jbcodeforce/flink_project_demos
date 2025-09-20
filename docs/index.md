# Flink Project Demonstrations

This project includes one Spark project to illustrate batch processing to build a customer 360 analytic data product. With a move to real-time the equivalent Flink project is also provided. The project also demonstrate an automatic migration from Spark to Flink using the [shift_left tool](https://jbcodeforce.github.io/shift_left_utils/coding/llm_based_translation/) and agentic AI.

## A data as a product design

The specific use case is a multi-channel retailer (bricks-and-mortar stores, e-commerce, mobile app) migrating their analytics platform to an Analytics Lakehouse with real-time processing.

The project identifies the key business domains and assigning the following ownership:

| Domain	|Data Owner/Team	|Key Data Sources|
| --- | --- | --- |
| **Customer**	| Customer Experience/CRM Team	| CRM system, loyalty program, support tickets, app usage logs |
| **Sales**	| Finance/Sales Operations Team	 |Point of Sale (POS) system, e-commerce transactions, regional sales ledgers |
| **Product** |	Merchandising Team	| Product catalog, inventory system, supplier data |
| **Logistics**	| Supply Chain Team	| Warehouse management, shipping manifests, tracking data |

The Customer Domain Team is responsible for building a core data product named, the **Customer 360 Data Product**.

| Metadata | Description | 
| --- | --- | 
| Product Name: | customer.analytics.C360 |
| Purpose: | To provide a comprehensive, high-quality, and up-to-date view of a customer for analytics, BI, and ML initiatives across all other domains. |
| Data Storage: | Uses the Lakehouse's object storage in an open format (e.g. Iceberg tables) for ACID-compliant, performant data.|
| Ingestion: | Real-time are managed by the Customer Domain Team to ingest and transform raw data from source systems into the Lakehouse. |

#### Data Product Output:

The final curated data is exposed via well-defined, easily consumable interfaces: 

* SQL Endpoint: A materialized view or table on the Lakehouse that other domains can query directly using SQL.
* API Service: A REST API for low-latency, record-by-record lookups (e.g., for a personalized recommendation service).
* File Export: Secure, versioned file exports for large-scale ML model training.

#### Data Product Characteristics:

* Discoverable: Registered in a central Data Catalog (a self-service component of the Mesh).
* Addressable: Has a unique identifier and clear documentation (customer.analytics.C360).
* Trustworthy: Includes built-in data quality checks (DQ) and validation rules enforced by the Customer Team.
* Self-Describing: Contains rich, up-to-date metadata (schema, lineage, ownership, DQ metrics).
* Secure & Governed: Access is controlled using federated governance rules (e.g., fine-grained, tag-based access control on the Lakehouse).

#### Cross-Domain Consumption for Analytics

Other domains then consume this product to achieve their analytical goals:

| Consuming Domain | Analytical Goal |Data Product Consumed
| --- | --- | --- |
| Marketing	| Predict churn for a targeted email campaign. |	customer.analytics.Customer360Profile (to get demographics, loyalty score, purchase history).|
| Product |	Analyze which customer segments are buying a new line of shoes. | customer.analytics.Customer360Profile joined with the Sales Domain's sales.transaction.AggregatedDailySales product.|
| Finance |	Calculate the Customer Lifetime Value (CLV) |	Queries the customer.analytics.Customer360Profile via the SQL endpoint. |

---

To be continued

## Folder structure

* flink_projects includes a set of flink processing in the form of pipelines to demonstrate some Flink concepts, and for demonstrating pipeline management with [shift_left tool](https://jbcodeforce.github.io/shift_left_utils/pipeline_mgr/)

    * the saleops is a simple example of Kimball structure for a star schema about revenu computation in the context of sales of products within different channels (See the [readme](./flink_data_products/pipelines/facts/saleops/fct_revenu/readme.md) for details). 
* Spark project: a folder that includes two Spark SQL batch processing flows.
* Ksql project: a set of files to be used for testing automatic migration.


### Spark project

The spark project is organized by batch scripts, in a kimball structure of sources, intermediates and facts.

Different 'data product' to illustrate different Spark patterns.

* `cj` for customer journey: fact: `cj/fct_customer_metrics.sql` 

### Flink data products demonstration

The folder `flink_data_products` was created with the shift_left CLI.


### KSQL project