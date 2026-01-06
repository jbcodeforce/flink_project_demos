# Terraform to deploy infrastructure of the tx_processing demo

## Flink resources needed

* Confluent cloud environment
* A Service Account as environment admin
* Kafka Cluster with API key
* A schema registry with API key
* Kafka Cluster
* Schema Registry
* Flink service accounts: `flink-app` (with ClusterAdmin role, DeveloperRead and DeveloperWrite on schema registry), `flink-developer-sa` (FlinkDeveloper), Flink API key owned by Flink Developer SA
* Flink compute pools: a default and one for data generations
* Flink statements for clicks, customers, products data generation

1. Set environment variables for API Keys and Secrets in a .env file and source it:
    ```sh
    source .env
    ```

## Build the Terraform manifests incrementally

The approach is to reuse an existing environment and add Flink resources.


1. Create a main.tf with the confluent cloud provider. (see a main.tf [example here](https://github.com/confluentinc/terraform-provider-confluent/blob/master/examples/configurations/basic-kafka-acls/main.tf))

1. Init:
    ```sh
    terraform init
    ```

1. Add variables.tf: we need at least: `confluent_cloud_api_key, confluent_cloud_api_secret, cloud_provider, cloud_region and prefix`. Each variable definition has the same structure:

    ```terraform
    variable "prefix" {
        description = "Prefix for resource names to avoid conflicts"
        type        = string
        default     = "j9r"
    }
    ```
    
1. Define specific variable values using: `terraform.tfvars` or use default values.
1. Define an `env.tf` for Confluent Cloud environment definition.
1. When using an existing environment, add an `imports.tf` with declaration like:
    ```tf
    import {
        to = confluent_environment.env
        id = "env-nknqp3"
    }
    ```
1. Define an `output.tf` to get expected information. See [Confluent example](https://github.com/confluentinc/terraform-provider-confluent/blob/master/examples/configurations/basic-kafka-acls/outputs.tf).
1. At each iteration of adding resources, do:
    ```sh
    terraform validate
    terraform plan
    terraform apply
    ```

