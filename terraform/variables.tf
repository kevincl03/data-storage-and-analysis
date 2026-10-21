variable "atlas_org_id" {
  description = "MongoDB Atlas Organization ID where the project will be created"
  type        = string
}

variable "atlas_public_key" {
  description = "Public Programmatic API key for MongoDB Atlas"
  type        = string
  sensitive   = true
}

variable "atlas_private_key" {
  description = "Private Programmatic API key for MongoDB Atlas"
  type        = string
  sensitive   = true
}

variable "project_name" {
  description = "Name of the MongoDB Atlas project"
  type        = string
  default     = "hyperflix-platform"
}

variable "cluster_name" {
  description = "Name of the MongoDB Atlas cluster"
  type        = string
  default     = "hyperflix-cluster"
}

variable "cluster_tier" {
  description = "Instance tier for MongoDB cluster. Default M0 is free tier"
  type        = string
  default     = "M0"

  validation {
    condition     = can(regex("^(M0|M2|M5|M10|M20|M30)$", var.cluster_tier))
    error_message = "cluster_tier must be one of M0, M2, M5, M10, M20, M30."
  }
}

variable "cloud_provider" {
  description = "Cloud provider hosting the Atlas cluster (AWS, GCP, or AZURE)"
  type        = string
  default     = "AWS"

  validation {
    condition     = can(regex("^(AWS|GCP|AZURE)$", var.cloud_provider))
    error_message = "cloud_provider must be AWS, GCP, or AZURE."
  }
}

variable "region_name" {
  description = "Cloud provider region name (e.g. US_EAST_1 for AWS)"
  type        = string
  default     = "US_EAST_1"
}

variable "database_name" {
  description = "Target database name for Hyperflix"
  type        = string
  default     = "hyperflix_db"
}

variable "app_username" {
  description = "Username for application OLTP/OLAP user"
  type        = string
  default     = "hyperflix_app"
}

variable "app_password" {
  description = "Optional password for app user. If empty, a secure random password will be generated"
  type        = string
  default     = ""
  sensitive   = true
}

variable "reader_username" {
  description = "Username for read-only analytics and data lake user"
  type        = string
  default     = "data_lake_reader"
}

variable "reader_password" {
  description = "Optional password for reader user. If empty, a secure random password will be generated"
  type        = string
  default     = ""
  sensitive   = true
}

variable "allowed_cidr_blocks" {
  description = "List of CIDR blocks allowed to access the cluster. Defaults to 0.0.0.0/0 for academic/dev use"
  type        = list(string)
  default     = ["0.0.0.0/0"]
}
