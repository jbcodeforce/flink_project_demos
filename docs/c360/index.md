# Customer 360 Analytics

## How to consume this content

## Flink Project Demos - Folder Structure

* **c360_*** are a set of projects to demonstrate how to define a data as a product in Spark and its equivalent for real time processing in Flink SQL.
* **c360_spark_processing** a batch implementation using the `star schema` and Kimball method to organize facts, and dimensions. The project [description is here.](./c360/spark_project.md). This project was created using `shift_left project init c360_spark_processing` command.
* **c360_mock_data**: a set of CSV files to create synthetic data.
* **c360_api**: A Python FastAPI-based REST API that exposes Customer 360 analytics for Marketing, Product, and Finance teams built on top of Spark data pipeline.
* **c360_flink_processing**: Building the same analytic data as a product with Flink processing.
