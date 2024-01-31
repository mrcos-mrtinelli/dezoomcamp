variable "credentials" {
  description = "GCS Credentials location"
  default     = "./keys/terraform-de-camp-cred.json"
}

variable "gcs_project_name" {
  description = "My GCS Project name"
  default     = "terraform-de-camp"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default     = "terraform_de_camp_dataset"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket name"
  default     = "terraform-de-camp-bucket"
}

variable "gcs_storage_class" {
  description = "My Storage Bucket Class"
  default     = "STANDARD"
}

variable "location" {
  description = "Project location"
  default     = "US"
}

variable "region" {
  description = "Project region setting"
  default     = "us-west2-a"
}
