output "org_id" {
  value = data.confluent_organization.my_org.id
}

output "env_id" {
  value = confluent_environment.env.id
}

output "env_name" {
  value = confluent_environment.env.display_name
}


# Flink Service Account

output "flink_app_sa_id" {
  value = "confluent_service_account.${var.prefix}-flink-app.id"
}

# Flink Compute Pool

output "flink_compute_pool_id" {
  value = confluent_flink_compute_pool.data-generation.id
}

output "flink_rest_endpoint" {
  value = data.confluent_flink_region.flink_region.rest_endpoint
}