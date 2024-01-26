terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "4.51.0"
    }
  }
}

provider "google" {
# Credentials only needs to be set if you do not have the GOOGLE_APPLICATION_CREDENTIALS set
#  credentials = 
  project = "terraform-de-camp"
  region  = "us-west2-a"
}



resource "google_storage_bucket" "terraform-basics-bucket" {
  name          = "terraform-de-camp-butket"
  location      = "US"

  # Optional, but recommended settings:
  storage_class = "STANDARD"
  uniform_bucket_level_access = true

  versioning {
    enabled     = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30  // days
    }
  }

  force_destroy = true
}


resource "google_bigquery_dataset" "terraform-basics-dataset" {
  dataset_id = "terraform_de_camp_dataset"
  project    = "terraform-de-camp"
  location   = "US"
}