
# -----------------------------------------------------------------------------
# Data Source: Organization
# -----------------------------------------------------------------------------
data "confluent_organization" "org" {
}

# -----------------------------------------------------------------------------
# Local: Compute pool ID (created or provided)
# -----------------------------------------------------------------------------
# Use the compute pool ID from confluent.tf which handles existing vs created
locals {
  flink_compute_pool_id_final = local.flink_compute_pool_id
  
  # Flink API key and secret from confluent.tf
  flink_api_key    = confluent_api_key.cdc_sa_flink_key.id
  flink_api_secret = confluent_api_key.cdc_sa_flink_key.secret
  
  base_properties = {
    "sql.current-catalog"  = data.confluent_environment.env.display_name
    "sql.current-database" = data.confluent_kafka_cluster.cluster.display_name
  }
  

 tables = {
    "src_tx_items" : {
      ddl_path = "../pipelines/sources/c360/src_tx_items/tests/ddl.tx_items_raw.sql"
      dml_path = "../pipelines/sources/c360/src_tx_items/tests/insert_tx_items_raw.sql"
    }
    "src_transactions" : {
      ddl_path = "../pipelines/sources/c360/src_transactions/tests/ddl.tx_raw.sql"
      dml_path = "../pipelines/sources/c360/src_transactions/tests/insert_tx_raw.sql"
    }
    "src_support_ticket" : {
      ddl_path = "../pipelines/sources/c360/src_support_ticket/tests/ddl.support_ticket_raw.sql"
      dml_path = "../pipelines/sources/c360/src_support_ticket/tests/insert_support_ticket_raw.sql"
    }
    "src_products": {
      ddl_path = "../pipelines/sources/c360/src_products/tests/ddl.product_raw.sql"
      dml_path = "../pipelines/sources/c360/src_products/tests/insert_product_raw.sql"
    }
    "src_loyalty_program": {
      ddl_path = "../pipelines/sources/c360/src_loyalty_program/tests/ddl.loyalty_program_raw.sql"
      dml_path = "../pipelines/sources/c360/src_loyalty_program/tests/insert_loyalty_program_raw.sql"
    }
    "src_customers": {
      ddl_path = "../pipelines/sources/c360/src_customers/tests/ddl.customers_raw.sql"
      dml_path = "../pipelines/sources/c360/src_customers/tests/insert_customers_raw.sql"
    }
    "src_app_usage": {
      ddl_path = "../pipelines/sources/c360/src_app_usage/tests/ddl.app_usage_raw.sql"
      dml_path = "../pipelines/sources/c360/src_app_usage/tests/insert_app_usage_raw.sql"
    }
  }
}
# -----------------------------------------------------------------------------
# DDL Statement: Create Tables
# -----------------------------------------------------------------------------
resource "confluent_flink_statement" "ddls" {
  for_each = local.tables
  organization {
    id = data.confluent_organization.org.id
  }
  
  environment {
    id = local.environment_id
  }
  
  compute_pool {
    id = local.flink_compute_pool_id_final
  }
  
  principal {
    id = local.service_account_id
  }
  
  dynamic "credentials" {
    for_each = local.flink_api_key != "" && local.flink_api_secret != "" ? [1] : []
    content {
      key    = local.flink_api_key
      secret = local.flink_api_secret
    }
  }
  
  rest_endpoint = data.confluent_flink_region.cdc_flink_region.rest_endpoint
  
  statement      = file(each.value.ddl_path)
  statement_name = "${var.prefix}-ddl-${replace(each.key, "_", "-")}"
  
  properties = local.base_properties
  
  depends_on = [
    confluent_api_key.cdc_sa_flink_key
  ]
  
  lifecycle {
    prevent_destroy = false
  }
}

# -----------------------------------------------------------------------------
# DML Statement: Insert Into
# -----------------------------------------------------------------------------
resource "confluent_flink_statement" "dml" {
  for_each = local.tables
  organization {
    id = data.confluent_organization.org.id
  }
  
  environment {
    id = local.environment_id
  }
  
  compute_pool {
    id = local.flink_compute_pool_id_final
  }
  
  principal {
    id = local.service_account_id
  }
  
  rest_endpoint = data.confluent_flink_region.cdc_flink_region.rest_endpoint
  
  dynamic "credentials" {
    for_each = local.flink_api_key != "" && local.flink_api_secret != "" ? [1] : []
    content {
      key    = local.flink_api_key
      secret = local.flink_api_secret
    }
  }
  properties = local.base_properties
  
  statement      = file(each.value.dml_path)
  statement_name = "${var.prefix}-ins-${replace(each.key, "_", "-")}"
  
  # DML statement depends on DDL being created first and API key existing
  depends_on = [
    confluent_flink_statement.ddls,
    confluent_api_key.cdc_sa_flink_key
  ]
  
  lifecycle {
    prevent_destroy = false
  }
}