provider "google" {
  project = "new-gcp-cloud-sql-project"
  region  = "us-central1"
}

resource "google_storage_bucket" "bucket" {
  name                        = "test-bucket-9793886877"
  location                    = "EU"
  force_destroy               = true
  uniform_bucket_level_access = true
}

# bigquery dataset

resource "google_bigquery_dataset" "dataset" {
  dataset_id                  = "example_dataset"
  description                 = "This is a test description"
  location                    = "EU"
  default_table_expiration_ms = 3600000
}

# bigquery table

resource "google_bigquery_table" "default" {
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  table_id   = "bar"
  # deletion_protection = false
  
  schema = <<EOF
[
  {
    "name": "permalink",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "The Permalink"
  },
  {
    "name": "state",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "State where the head office is located"
  }
]
EOF

}
 