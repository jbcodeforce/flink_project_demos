
## Schema registry 
data "confluent_schema_registry_cluster" "essentials" {
  id = "lsrc-vz1zrz"
  environment {
    id = confluent_environment.env.id
  }
}

