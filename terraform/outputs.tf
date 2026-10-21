output "project_id" {
  description = "MongoDB Atlas Project ID"
  value       = mongodbatlas_project.project.id
}

output "cluster_id" {
  description = "MongoDB Atlas Cluster ID"
  value       = mongodbatlas_advanced_cluster.cluster.cluster_id
}

output "cluster_state" {
  description = "State of the provisioned Atlas cluster"
  value       = mongodbatlas_advanced_cluster.cluster.state_name
}

output "connection_string_srv" {
  description = "Raw SRV connection string of the MongoDB Atlas cluster"
  value       = mongodbatlas_advanced_cluster.cluster.connection_strings[0].standard_srv
}

output "connection_string_standard" {
  description = "Standard connection string of the MongoDB Atlas cluster"
  value       = mongodbatlas_advanced_cluster.cluster.connection_strings[0].standard
}

output "app_connection_string" {
  description = "Fully-qualified SRV connection string for the Hyperflix application user"
  value       = "mongodb+srv://${var.app_username}:${local.final_app_password}@${local.srv_host}/?retryWrites=true&w=majority"
  sensitive   = true
}

output "reader_connection_string" {
  description = "Fully-qualified SRV connection string for the Data Lake reader user"
  value       = "mongodb+srv://${var.reader_username}:${local.final_reader_password}@${local.srv_host}/?retryWrites=true&w=majority"
  sensitive   = true
}

output "env_file_path" {
  description = "Absolute or relative path to the generated environment file"
  value       = local_file.env_file.filename
}
