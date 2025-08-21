# Flink Project Demonstrations

This folder includes different Flink projects for demonstrating Flink processing, but also to support automatic migration from ksqlDN projects or Spark projects to flink using the [shift_left tool](https://jbcodeforce.github.io/shift_left_utils/coding/llm_based_translation/).

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