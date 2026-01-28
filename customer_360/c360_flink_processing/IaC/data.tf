# -----------------------------------------------------------------------------
# Data Sources
# Additional data sources for Flink statement configuration
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# Environment Data Source
# Get environment display name for Flink properties (sql.current-catalog)
# -----------------------------------------------------------------------------
data "confluent_environment" "env" {
  id = local.environment_id
}

# -----------------------------------------------------------------------------
# Kafka Cluster Data Source
# Get cluster display name for Flink properties (sql.current-database)
# -----------------------------------------------------------------------------
data "confluent_kafka_cluster" "cluster" {
  id = local.kafka_cluster_id
  
  environment {
    id = local.environment_id
  }
}
