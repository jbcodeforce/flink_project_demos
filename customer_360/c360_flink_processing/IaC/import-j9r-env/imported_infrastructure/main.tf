terraform {
  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "2.58.0"
    }
  }
}
provider "confluent" {
  cloud_api_key    = var.confluent_cloud_api_key
  cloud_api_secret = var.confluent_cloud_api_secret
}

resource "confluent_environment" "sapintegrationplayground" {
  display_name = "SAPIntegrationPlayground"
  stream_governance {
    package = "ESSENTIALS"
  }
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_environment" "satakshi_workspace_2" {
  stream_governance {
    package = "ESSENTIALS"
  }
  display_name = "Satakshi-Workspace"
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_environment" "justin_public_3" {
  stream_governance {
    package = "ADVANCED"
  }
  display_name = "justin-public"
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_environment" "satakshi_workspace_new_4" {
  stream_governance {
    package = "ESSENTIALS"
  }
  display_name = "Satakshi-Workspace-New"
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_environment" "astuart_5" {
  stream_governance {
    package = "ADVANCED"
  }
  display_name = "astuart"
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_environment" "env_eu_6" {
  display_name = "env_eu"
  stream_governance {
    package = "ESSENTIALS"
  }
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_environment" "qi_7" {
  stream_governance {
    package = "ESSENTIALS"
  }
  display_name = "Qi"
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_environment" "staging_8" {
  stream_governance {
    package = "ESSENTIALS"
  }
  display_name = "Staging"
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_environment" "jeremy_playground_9" {
  display_name = "jeremy-playground"
  stream_governance {
    package = "ESSENTIALS"
  }
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_environment" "lorenzo_playground_10" {
  display_name = "lorenzo-playground"
  stream_governance {
    package = "ESSENTIALS"
  }
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_environment" "j9r_env_11" {
  display_name = "j9r-env"
  stream_governance {
    package = "ESSENTIALS"
  }
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_environment" "dominique_12" {
  stream_governance {
    package = "ESSENTIALS"
  }
  display_name = "Dominique"
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_environment" "taka_blue_13" {
  display_name = "Taka-Blue"
  stream_governance {
    package = "ADVANCED"
  }
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_environment" "schemaregistry_14" {
  display_name = "SchemaRegistry"
  stream_governance {
    package = "ESSENTIALS"
  }
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_environment" "sap_datasphere_streaming_15" {
  stream_governance {
    package = "ESSENTIALS"
  }
  display_name = "SAP_Datasphere_Streaming"
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_environment" "accelerator_16" {
  stream_governance {
    package = "ESSENTIALS"
  }
  display_name = "accelerator"
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_environment" "jcustenborder_17" {
  display_name = "jcustenborder"
  stream_governance {
    package = "ADVANCED"
  }
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_environment" "mic_18" {
  display_name = "mic"
  stream_governance {
    package = "ESSENTIALS"
  }
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_environment" "jsvoboda_19" {
  stream_governance {
    package = "ADVANCED"
  }
  display_name = "jsvoboda"
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_environment" "taka_green_20" {
  display_name = "Taka-Green"
  stream_governance {
    package = "ADVANCED"
  }
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_environment" "justin_private_21" {
  display_name = "justin-private"
  stream_governance {
    package = "ADVANCED"
  }
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_environment" "osowski_sandbox_22" {
  stream_governance {
    package = "ESSENTIALS"
  }
  display_name = "osowski-sandbox"
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_environment" "dsp_edge_test_49ac54e0_23" {
  display_name = "dsp-edge-test-49ac54e0"
  stream_governance {
    package = "ESSENTIALS"
  }
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_service_account" "sraj_test" {
  display_name = "sraj-test"
  description  = "srajtest"
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_service_account" "lorenzo_playground_crm_2" {
  description  = "SA for Cloud Resource Management on Lorenzo's playground"
  display_name = "Lorenzo-playground-CRM"
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_service_account" "taka_flink_workshop_3" {
  display_name = "taka-flink-workshop"
  description  = "APIKey used by Taka to run the Flink Workshop."
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_service_account" "streaming_agents_setup_sa_147235_4" {
  description  = "Service account for streaming-agents streaming agents setup"
  display_name = "streaming-agents-setup-sa-147235"
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_service_account" "lorenzo_playground_terraform_5" {
  description  = "SA for Terraform on Lorenzo's playground"
  display_name = "Lorenzo-playground-Terraform"
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_service_account" "lorenzo_tf_app_manager_6" {
  display_name = "lorenzo-tf-app-manager"
  description  = "Lorenzo playground - TF app-manager"
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_service_account" "sa_datagensource_eb0c9921_7" {
  description  = "Service Account for DatagenSource"
  display_name = "SA_DatagenSource_eb0c9921"
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_service_account" "justin_flink_sa_8" {
  description  = "orgadmin sa for flink"
  display_name = "justin-flink-sa"
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_service_account" "xperryment_sap_datasphere_sa_9" {
  display_name = "xperryment_sap_datasphere_sa"
  description  = "Service account to be used by SAP Datasphere data connections, with minimal RBAC required by SAP Datasphere"
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_service_account" "taka_general_tests_with_terraform_10" {
  description  = ""
  display_name = "taka-general-tests-with-terraform"
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_service_account" "sgws_cpi_11" {
  description  = "Test account for SGWS SAP Cloud Integration with limited access rights"
  display_name = "SGWS_CPI"
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_service_account" "dsp_north_admin_12" {
  description  = "Service account for managing dsp-north cluster"
  display_name = "dsp-north-admin"
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_service_account" "taka_training_sa_13" {
  description  = "Service account used by Taka during training."
  display_name = "taka-training-sa"
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_service_account" "j9r_flink_app_14" {
  description  = "Service account as which Flink statements run in the environment"
  display_name = "j9r-flink-app"
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_service_account" "justin_datagen_15" {
  description  = ""
  display_name = "justin-datagen"
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_service_account" "taka_connectors_sa_16" {
  display_name = "taka-connectors-sa"
  description  = ""
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_service_account" "test_cpi_17" {
  description  = "CPI test account`"
  display_name = "test_cpi"
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_service_account" "sa_datagensource_e0837f07_18" {
  description  = "Service Account for DatagenSource"
  display_name = "SA_DatagenSource_e0837f07"
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_service_account" "app_consumer_19" {
  display_name = "app-consumer"
  description  = "Service account to consume from 'orders' topic of 'inventory' Kafka cluster"
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_service_account" "app_producer_20" {
  description  = "Service account to produce to 'orders' topic of 'inventory' Kafka cluster"
  display_name = "app-producer"
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_service_account" "sa_terraform_bot_21" {
  description  = "Service Account to be used by Terraform provider that invokes the Control Plane REST APIs."
  display_name = "sa-terraform-bot"
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_service_account" "sa_terraform_22" {
  description  = "Service Account for Terraform"
  display_name = "sa-terraform"
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_service_account" "justin_usm_23" {
  description  = "justin-usm-testing"
  display_name = "justin-usm"
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_service_account" "astuart_demo_deployer_24" {
  description  = "deploy demos for astuart"
  display_name = "astuart_demo_deployer"
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_service_account" "astuart_tf_25" {
  description  = "tf for astuar"
  display_name = "astuart_TF"
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_service_account" "sa_datagensource_45d510d4_26" {
  description  = "Service Account for DatagenSource"
  display_name = "SA_DatagenSource_45d510d4"
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_service_account" "sa_datagensource_552b403f_27" {
  description  = "Service Account for DatagenSource"
  display_name = "SA_DatagenSource_552b403f"
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_service_account" "lorenzo_tf_platform_manager_28" {
  description  = "Lorenzo playground - TF platform-manager"
  display_name = "lorenzo-tf-platform-manager"
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_service_account" "app_manager_29" {
  description  = "Service account to manage Kafka cluster"
  display_name = "app-manager"
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_service_account" "sa_datagensource_a9588c1f_30" {
  description  = "Service Account for DatagenSource"
  display_name = "SA_DatagenSource_a9588c1f"
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_service_account" "justin_tableflow_31" {
  description  = ""
  display_name = "justin-tableflow"
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_service_account" "txp_app_manager_69025db6_32" {
  description  = "Service account for managing CDC resources"
  display_name = "txp-app-manager-69025db6"
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_service_account" "lorenzo_tf_statements_runner_33" {
  description  = "Lorenzo playground - TF statements-runner"
  display_name = "lorenzo-tf-statements-runner"
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_service_account" "kafka_sa_34" {
  description  = "A service account for app to access kafka cluster"
  display_name = "kafka-sa"
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_service_account" "bluemelon_sap_datasphere_sa_35" {
  display_name = "bluemelon-sap-datasphere-sa"
  description  = "Service Account for SAP Datasphere in BlueMelon Product Returns demo."
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_service_account" "app_manager_dev_36" {
  display_name = "app-manager-dev"
  description  = "Service account for managing Kafka resources"
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_service_account" "sa_datagensource_082495df_37" {
  description  = "Service Account for DatagenSource"
  display_name = "SA_DatagenSource_082495df"
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_service_account" "sa_datagensource_adbe2778_38" {
  display_name = "SA_DatagenSource_adbe2778"
  description  = "Service Account for DatagenSource"
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_service_account" "justin_connect_39" {
  description  = ""
  display_name = "justin-connect"
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_service_account" "j9r_kafka_mgr_40" {
  display_name = "j9r-kafka-mgr"
  description  = "Service account to manage 'standard' Kafka cluster"
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_service_account" "j9r_fd_sa_41" {
  display_name = "j9r-fd-sa"
  description  = "Service account to deploy Flink statements in the environment"
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_service_account" "app_connector_42" {
  description  = "Service account to manage Kafka connectors"
  display_name = "app-connector"
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_service_account" "healthplus_1729002926838_43" {
  display_name = "HealthPlus.1729002926838"
  description  = "SA for Health+"
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_service_account" "j9r_env_manager_44" {
  description  = "Service account to manage j9r environment"
  display_name = "j9r-env-manager"
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_service_account" "ntwk_experiments_aws_app_manager_45" {
  description  = "Service account to manage Kafka cluster"
  display_name = "ntwk_experiments_aws_app-manager"
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_kafka_cluster" "sap_s4hana_onibex_oneconnect" {
  environment {
    id = "env-mzpz7x"
  }
  region       = "eu-central-1"
  availability = "LOW"
  cloud        = "AWS"
  display_name = "sap_s4hana_onibex_oneconnect"
  standard {
    max_ecku = 10
  }
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_kafka_cluster" "sap_datasphere_xperryment_2" {
  environment {
    id = "env-xx8qkq"
  }
  availability = "LOW"
  cloud        = "AWS"
  region       = "eu-central-1"
  display_name = "SAP_Datasphere_Xperryment"
  standard {
    max_ecku = 10
  }
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_kafka_cluster" "teladoc_3" {
  availability = "LOW"
  cloud        = "AWS"
  region       = "us-east-2"
  display_name = "teladoc"
  environment {
    id = "env-30d332"
  }
  basic {
    max_ecku = 50
  }
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_kafka_cluster" "cluster_0_4" {
  cloud        = "AWS"
  display_name = "cluster_0"
  region       = "us-east-2"
  availability = "LOW"
  environment {
    id = "env-20q1my"
  }
  basic {
    max_ecku = 50
  }
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_kafka_cluster" "cluster_1_5" {
  environment {
    id = "env-20q1my"
  }
  basic {
    max_ecku = 50
  }
  cloud        = "AWS"
  display_name = "cluster_1"
  availability = "LOW"
  region       = "ap-southeast-1"
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_kafka_cluster" "azure_6" {
  standard {
    max_ecku = 10
  }
  availability = "LOW"
  cloud        = "AZURE"
  region       = "westeurope"
  display_name = "azure"
  environment {
    id = "env-9325m7"
  }
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_kafka_cluster" "cluster_0_7" {
  region       = "us-east-2"
  availability = "LOW"
  basic {
    max_ecku = 50
  }
  cloud = "AWS"
  environment {
    id = "env-nx1696"
  }
  display_name = "cluster_0"
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_kafka_cluster" "enablement_cluster_8" {
  cloud        = "AWS"
  display_name = "enablement_cluster"
  availability = "LOW"
  basic {
    max_ecku = 50
  }
  region = "us-east-2"
  environment {
    id = "env-kk5d72"
  }
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_kafka_cluster" "bharatpe_9" {
  availability = "LOW"
  cloud        = "AWS"
  display_name = "bharatpe"
  basic {
    max_ecku = 50
  }
  region = "ap-southeast-1"
  environment {
    id = "env-30d332"
  }
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_kafka_cluster" "inventory_10" {
  cloud = "AWS"
  dedicated {
    cku            = 1
    encryption_key = ""
    zones          = ["use2-az1"]
  }
  environment {
    id = "env-6z6xwq"
  }
  display_name = "inventory"
  network {
    id = "n-g0d4em"
  }
  availability = "SINGLE_ZONE"
  region       = "us-east-2"
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_kafka_cluster" "cluster_1_11" {
  availability = "LOW"
  display_name = "cluster_1"
  basic {
    max_ecku = 50
  }
  region = "uksouth"
  cloud  = "AZURE"
  environment {
    id = "env-9325m7"
  }
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_kafka_cluster" "azure_cluster_12" {
  environment {
    id = "env-q6r1wp"
  }
  standard {
    max_ecku = 10
  }
  region       = "eastus2"
  cloud        = "AZURE"
  display_name = "azure-cluster"
  availability = "LOW"
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_kafka_cluster" "cluster_0_13" {
  region       = "us-east-2"
  display_name = "cluster_0"
  environment {
    id = "env-2y8vz1"
  }
  basic {
    max_ecku = 50
  }
  availability = "LOW"
  cloud        = "AWS"
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_kafka_cluster" "cluster_0_14" {
  basic {
    max_ecku = 50
  }
  environment {
    id = "env-7dzogo"
  }
  region       = "us-central1"
  cloud        = "GCP"
  display_name = "cluster_0"
  availability = "LOW"
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_kafka_cluster" "sap_s4hana_faa_integration_cluster_15" {
  availability = "LOW"
  cloud        = "AZURE"
  basic {
    max_ecku = 50
  }
  display_name = "SAP_S4HANA_FAA_Integration_Cluster"
  environment {
    id = "env-mzpz7x"
  }
  region = "westus3"
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_kafka_cluster" "atg_justin_public_16" {
  cloud        = "AWS"
  region       = "ap-southeast-1"
  availability = "LOW"
  environment {
    id = "env-20k0v1"
  }
  standard {
    max_ecku = 10
  }
  display_name = "atg-justin-public"
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_kafka_cluster" "j9r_kafka_17" {
  display_name = "j9r-kafka"
  region       = "us-west-2"
  standard {
    max_ecku = 10
  }
  cloud        = "AWS"
  availability = "SINGLE_ZONE"
  environment {
    id = "env-yk3jm6"
  }
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_kafka_cluster" "dsp_north_18" {
  availability = "MULTI_ZONE"
  cloud        = "AWS"
  environment {
    id = "env-kww5o6"
  }
  display_name = "dsp-north"
  enterprise {
    max_ecku = 32
  }
  region = "eu-north-1"
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_kafka_cluster" "bluemelon_returns_demo_19" {
  availability = "LOW"
  cloud        = "GCP"
  region       = "europe-west3"
  basic {
    max_ecku = 50
  }
  environment {
    id = "env-xx8qkq"
  }
  display_name = "BlueMelon_Returns_Demo"
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_kafka_cluster" "atg_justin_private_standard_20" {
  standard {
    max_ecku = 10
  }
  cloud        = "AWS"
  display_name = "atg-justin-private-standard"
  environment {
    id = "env-wjzw95"
  }
  availability = "LOW"
  region       = "ap-southeast-1"
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_kafka_cluster" "atg_justin_private_enterprise_21" {
  environment {
    id = "env-wjzw95"
  }
  cloud        = "AWS"
  availability = "LOW"
  display_name = "atg-justin-private-enterprise"
  enterprise {
    max_ecku = 1
  }
  region = "ap-southeast-1"
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_kafka_cluster" "azure_22" {
  display_name = "azure"
  region       = "southeastasia"
  enterprise {
    max_ecku = 32
  }
  environment {
    id = "env-wjzw95"
  }
  cloud        = "AZURE"
  availability = "LOW"
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_kafka_cluster" "swiggy_23" {
  display_name = "swiggy"
  environment {
    id = "env-30d332"
  }
  region       = "us-east-2"
  availability = "LOW"
  basic {
    max_ecku = 50
  }
  cloud = "AWS"
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_kafka_cluster" "cluster_0_24" {
  display_name = "cluster_0"
  environment {
    id = "env-wk5yzj"
  }
  region = "eu-west-1"
  standard {
    max_ecku = 10
  }
  availability = "LOW"
  cloud        = "AWS"
  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_kafka_cluster" "jans_aws_frankfurt_25" {
  basic {
    max_ecku = 50
  }
  availability = "LOW"
  cloud        = "AWS"
  display_name = "jans_aws_frankfurt"
  environment {
    id = "env-6wzjxq"
  }
  region = "eu-central-1"
  lifecycle {
    prevent_destroy = true
  }
}

