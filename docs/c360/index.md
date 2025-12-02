# Customer 360 Analytics

## How to consume this content

The project is organized in two folds: 

* a batch processing using [Apache Sparks](./spark_project.md) to build the customer_analytics_c360 data product
* real-time processing to build the same customer_analytics_c360 data product using [Confluent Cloud Flink](flink_project.md)

## A methodology to design Data As A Product

[The chapter 'Moving to a Data as a Product Architecture'](https://jbcodeforce.github.io/flink-studies/methodology/data_as_a_product/) is practical overview of current data lake and lakehouse challenges, discusses the implementation of 'data as a product' principles, and demonstrates how real-time streaming can be effectively integrated into modern data architectures.

## Folder Structure

* **customer_360*** folder includes a set of sub-projects to demonstrate how to define a data as a product in Spark and its equivalent for real time processing in Flink SQL.
* **c360_spark_processing** a batch implementation using the `star schema` and Kimball method to organize facts, and dimensions. The project [description is here.](./spark_project.md). This project was created using `shift_left project init c360_spark_processing` command.
* **c360_mock_data**: a set of CSV files to create synthetic data.
* **c360_api**: A Python FastAPI-based REST API that exposes Customer 360 analytics for Marketing, Product, and Finance teams built on top of Spark data pipeline.
* **c360_flink_processing**: Building the same analytic data as a product with Flink processing.
* **Kafka_consumer**: a cli based tool to consume the different topics from Kafka to validate the different results within the pipeline.

## To Do

* [ ] Kafka Producer to simulate injecting the data into raw topics
* [ ] Add tableflow in IaC for the customer_analytics_c360 topic to Iceberg table in AWS S3 or Confluent Storage.
* [ ] Python FastAPI App with a HTML dashboard to present the different data products from parquet files, using `duckdb` as query engine
* [ ] Be able to run the same SQLs on Confluent Platform for Flink.
