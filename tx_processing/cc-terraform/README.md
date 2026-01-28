# Terraform to deploy infrastructure of the tx_processing demo

[See Confluent Terraform documentation](https://docs.confluent.io/cloud/current/clusters/terraform-provider.html) and [this confluent terraform github samples](https://github.com/confluentinc/terraform-provider-confluent/tree/master/examples/configurations) and the Flink-studies [book chapter](https://jbcodeforce.github.io/flink-studies/coding/terraform/).

## Confluent Cloud resources needed

We assume you already have a Confluent Cloud Environment with at least Kafka cluster and schema registry. 

[See this section to use resource importer for your own resources](https://jbcodeforce.github.io/flink-studies/coding/terraform/#resource-importer)

To add Flink specific resources, we propose to follow the process outlined in [this section - "add Flink resources to an existing environment"](https://jbcodeforce.github.io/flink-studies/coding/terraform/#adding-flink-to-an-existing-environment).

### Flink Resource Summary

| Resource | Purpose |
|----------|---------|
| `flink-app` service account | Runtime principal for Flink statements |
| `flink-developer-sa` service account | Deploys Flink statements |
| `CloudClusterAdmin` role binding | Allows flink-app to access Kafka cluster |
| `DeveloperRead/Write` role bindings | Allows flink-app to access Schema Registry |
| `FlinkDeveloper` role binding | Allows flink-developer-sa to create statements |
| `Assigner` role binding | Allows flink-developer-sa to assign flink-app as principal |
| Flink API key | Authentication for deploying statements |
| Compute pool | Resources for running Flink statements |


Once done we can focus on adding Flink statements for the application.

## Adding Flink Statements

* Flink statements for clicks, customers, products data generation





