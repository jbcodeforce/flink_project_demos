# -----------------------------------------------------------------------------
# SDP: Shipments and tracking_events_raw Flink deployments
# -----------------------------------------------------------------------------

locals {
  sdp_tables = {
    "src_shipments" = {
      ddl_path = "../pipelines/sources/sdp/src_shipments/tests/ddl.shipments_raw.sql"
      dml_path = "../pipelines/sources/sdp/src_shipments/tests/insert_shipments_raw.sql"
    }
    "src_tracking_events" = {
      ddl_path = "../pipelines/sources/sdp/src_tracking_events/tests/ddl.tracking_events_raw.sql"
      dml_path = "../pipelines/sources/sdp/src_tracking_events/tests/insert_tracking_events_raw.sql"
    }
  }
}

# -----------------------------------------------------------------------------
# DDL Statement: Create Tables (SDP)
# -----------------------------------------------------------------------------
resource "confluent_flink_statement" "sdp_ddls" {
  for_each = local.sdp_tables
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
# DML Statement: Insert Into (SDP)
# -----------------------------------------------------------------------------
resource "confluent_flink_statement" "sdp_dml" {
  for_each = local.sdp_tables
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

  depends_on = [
    confluent_flink_statement.sdp_ddls,
    confluent_api_key.cdc_sa_flink_key
  ]

  lifecycle {
    prevent_destroy = false
  }
}
