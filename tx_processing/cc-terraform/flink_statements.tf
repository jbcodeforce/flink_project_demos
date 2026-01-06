resource "confluent_flink_statement" "transaction_faker" {

  organization {
    id = data.confluent_organization.my_org.id
  }

   environment {
    id = confluent_environment.env.id
  }

  compute_pool  {
    id = confluent_flink_compute_pool.data-generation.id
  }
 
  principal {
    id = "confluent_service_account.${var.prefix}-flink-app.id"
  }
 
  rest_endpoint = data.confluent_flink_region.flink_region.rest_endpoint


  properties = {
    "sql.current-catalog"  = confluent_environment.env.display_name
    "sql.current-database" = confluent_kafka_cluster.standard.display_name
  }

  credentials {
    key    = var.FLINK_API_KEY
    secret = var.FLINK_API_SECRET
  }

  statement  = file("${path.module}/../pipelines/sources/tx/src_transactions/tests/dml_faker.sql")
  statement_name = "transactions-datagen"

}