## Kafka Cluster

resource "confluent_kafka_cluster" "standard" {
  display_name = "${var.prefix}-kafka"
  availability = "SINGLE_ZONE"
  cloud        = var.cloud_provider
  region       = var.cloud_region
  standard {}

  environment {
    id = confluent_environment.env.id
  }

  depends_on = [
    confluent_environment.env
  ]
}
