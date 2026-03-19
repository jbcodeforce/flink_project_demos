terraform {
  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "2.58.0"
    }
  }
}

provider "confluent" {}

resource "confluent_tf_importer" "cloud_resources" {
  output_path = "${path.module}/imported_infrastructure"

  # Supported resource types (cannot mix Cloud and Kafka resources)
  resources = [
    "confluent_environment",
    "confluent_service_account",
    "confluent_kafka_cluster"
  ]
}