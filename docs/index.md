# Flink Project Demonstrations

This git repository includes multiple projects to illustrate shifting from batch processing to data stream processing using Flink, and how to manage data as a product.

1. The first project is a classical customer 360 assessment. The different components are in the customer_360 folder. It starts from one Spark project to illustrate how to implement a customer 360 analytic data product using Apache Spark batch processing, then supports the same semantic with a data streaming processing using Apache Flink. This examples illustrates how to use Agentic solution to automate most of the migration using the [shit_left utilities](https://jbcodeforce.github.io/shift_left_utils)
1. The project `online_store` illustrates starting from a white page to process online store activities for customer behavior and real-time inventory. This is a fictuous demonstration to build using a set of incremental labs.

## Customer 360 introduction

The high level architecture for  batch processing is presented in the figure below, and use a simplified version of the star schema of the Kimball methodology, to build dimensional models:

![](./c360/images/spark_process.drawio.png)

To shift left by moving the batch-based Spark processing to a real-time processing with Kafka and Flink project, the architecture looks like:

![](./c360/images/kafka_flink_process.drawio.png)

The project also demonstrates an automatic migration from Spark to Flink using the [shift_left tool](https://jbcodeforce.github.io/shift_left_utils/coding/llm_based_translation/) and agentic AI.


### A data as a product design
Any data as a product design starts by assessing the domain, the data ownership, the data sources, and consumer of the analytic data products,... We recommend to read [this chapter](https://jbcodeforce.github.io/flink-studies/methodology/data_as_a_product/) to review a methodology to build data as a product.

The specific customer 360 use case is a multi-channel retailer (bricks-and-mortar stores, e-commerce, mobile app) migrating their analytics platform to an Analytics Lakehouse with real-time processing.

The project identifies the following key business domains and assigning the following ownership:

| Domain	|Data Owner/Team	|Key Data Sources|
| --- | --- | --- |
| **Customer**	| Customer Experience/CRM Team	| CRM system, loyalty program, support tickets, app usage logs |
| **Sales**	| Finance/Sales Operations Team	 |Point of Sale (POS) system, e-commerce transactions, regional sales ledgers |
| **Product** |	Merchandising Team	| Product catalog, inventory system, supplier data |
| **Logistics**	| Supply Chain Team	| Warehouse management, shipping manifests, tracking data |

The Customer Domain Team is responsible for building a core data product named, the **Customer 360 Data Product**.

| Metadata | Description | 
| --- | --- | 
| **Product Name:** | customer.analytics.C360 |
| **Purpose:** | To provide a comprehensive, high-quality, and up-to-date view of a customer for analytics, BI, and ML initiatives across all other domains. |
| **Data Storage:** | Uses the Lakehouse's object storage in an open format (e.g. Iceberg tables) for ACID-compliant, performant data.|
| **Ingestion:** | Real-time are managed by the Customer Domain Team to ingest and transform raw data from source systems into the Lakehouse. |

#### Data Product Output:

The final curated data is exposed via well-defined, easily consumable interfaces: 

* **SQL Endpoint:** A materialized view or table on the Lakehouse that other domains can query directly using SQL.
* **API Service:** A REST API for low-latency, record-by-record lookups (e.g., for a personalized recommendation service).
* **File Export:** Secure, versioned file exports for large-scale ML model training.

#### Data Product Characteristics:

* **Discoverable:** Registered in a central Data Catalog (a self-service component of the Mesh).
* **Addressable:** Has a unique identifier and clear documentation (customer.analytics.C360).
* **Trustworthy:** Includes built-in data quality checks (DQ) and validation rules enforced by the Customer Team.
* **Self-Describing:** Contains rich, up-to-date metadata (schema, lineage, ownership, DQ metrics).
* **Secure & Governed:** Access is controlled using federated governance rules (e.g., fine-grained, tag-based access control on the Lakehouse).

#### Cross-Domain Consumption for Analytics

Other domains may consume the data as a product to achieve their analytical goals:

| Consuming Domain | Analytical Goal |Data Product Consumed
| --- | --- | --- |
| **Marketing**	| Predict churn for a targeted email campaign. |	customer.analytics.Customer360Profile (to get demographics, loyalty score, purchase history).|
| **Product** |	Analyze which customer segments are buying a new line of shoes. | customer.analytics.Customer360Profile joined with the Sales Domain's sales.transaction.AggregatedDailySales product.|
| **Finance** |	Calculate the Customer Lifetime Value (CLV) |	Queries the customer.analytics.Customer360Profile via the SQL endpoint. |

[To read more about moving from Domain Driven Design to Data as a product methodology see this chapter](https://jbcodeforce.github.io/flink-studies/methodology/data_as_a_product/).

---

## Other projects
TBC

* **flink_data_products** includes a set of small data as a product examples implemented using Flink SQL. They are used to demonstrate pipeline management with [shift_left tool](https://jbcodeforce.github.io/shift_left_utils/pipeline_mgr/) and simplest use cases.

    * the saleops is a simple example of Kimball structure for a star schema about revenu computation in the context of sales of products within different channels (See the [readme](https://github.com/jbcodeforce/flink_project_demos/tree/main/flink_data_products/pipelines/facts/saleops/fct_revenu/readme.md) for details). 

* Ksql project: a set of files to be used for testing automatic migration from ksql.

To Be continued....
