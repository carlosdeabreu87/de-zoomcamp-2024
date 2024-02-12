variable "project_id" {
    description = "Project ID"
    default = "de-zoomcamp-2024-411900"
}

variable "location" {
    description = "Project location"
    default = "US"
}

variable "region" {
    description = "Project Region"
    default = "us-east1"
}

variable "gcs_bucket_id" {
    description = "My GCS Bucket name"
    default = "gcs-for-mage-411900-terra-bucket"
}

variable "gcs_bucket_name" {
    description = "My GCS Bucket name"
    default = "gcs-for-mage-411900-terra-bucket"
}

variable "bq_dataset_id" {
    description = "My BigQuery Dataset name"
    default = "bq_for_mage_dataset"
}