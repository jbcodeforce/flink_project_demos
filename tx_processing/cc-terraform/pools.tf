
resource "confluent_flink_compute_pool" "data-generation" {
  display_name     = "data-generation"
  cloud            =  upper(data.confluent_flink_region.flink_region.cloud)
  region           =  data.confluent_flink_region.flink_region.region
  max_cfu          = 50
  environment {
    id = confluent_environment.env.id
  }
}