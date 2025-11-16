terraform {
  required_version = ">= 1.0"
  
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.29"
    }
  }
  
  backend "gcs" {
    bucket = "customer-lakehouse-terraform-state"
    prefix = "terraform/state"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

provider "databricks" {
  host  = var.databricks_host
  token = var.databricks_token
}

# Local variables
locals {
  common_tags = {
    Project     = "customer-lakehouse"
    Environment = var.environment
    ManagedBy   = "terraform"
    CostCenter  = "data-engineering"
  }
}

### ============================================================================
# GCS BUCKETS
### ============================================================================

# Landing zone for raw data
resource "google_storage_bucket" "landing_zone" {
  name          = "${var.project_id}-landing-zone"
  location      = var.region
  storage_class = "STANDARD"
  
  uniform_bucket_level_access = true
  
  versioning {
    enabled = true
  }
  
  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 90  # Delete after 90 days
    }
  }
  
  encryption {
    default_kms_key_name = google_kms_crypto_key.bucket_key.id
  }
  
  labels = local.common_tags
}

# Bronze layer storage
resource "google_storage_bucket" "bronze_layer" {
  name          = "${var.project_id}-bronze"
  location      = var.region
  storage_class = "STANDARD"
  
  uniform_bucket_level_access = true
  
  versioning {
    enabled = true
  }
  
  encryption {
    default_kms_key_name = google_kms_crypto_key.bucket_key.id
  }
  
  labels = local.common_tags
}

# Silver layer storage
resource "google_storage_bucket" "silver_layer" {
  name          = "${var.project_id}-silver"
  location      = var.region
  storage_class = "STANDARD"
  
  uniform_bucket_level_access = true
  
  versioning {
    enabled = true
  }
  
  encryption {
    default_kms_key_name = google_kms_crypto_key.bucket_key.id
  }
  
  labels = local.common_tags
}

# Gold layer storage
resource "google_storage_bucket" "gold_layer" {
  name          = "${var.project_id}-gold"
  location      = var.region
  storage_class = "STANDARD"
  
  uniform_bucket_level_access = true
  
  versioning {
    enabled = true
  }
  
  encryption {
    default_kms_key_name = google_kms_crypto_key.bucket_key.id
  }
  
  labels = local.common_tags
}

# Airflow DAGs bucket
resource "google_storage_bucket" "airflow_dags" {
  name          = "${var.project_id}-airflow-dags"
  location      = var.region
  storage_class = "STANDARD"
  
  uniform_bucket_level_access = true
  
  labels = local.common_tags
}

# ============================================================================
# KMS FOR ENCRYPTION (CMEK)
# ============================================================================

resource "google_kms_key_ring" "lakehouse_keyring" {
  name     = "customer-lakehouse-keyring"
  location = var.region
}

resource "google_kms_crypto_key" "bucket_key" {
  name            = "bucket-encryption-key"
  key_ring        = google_kms_key_ring.lakehouse_keyring.id
  rotation_period = "7776000s"  # 90 days
  
  lifecycle {
    prevent_destroy = true
  }
}

# ============================================================================
# SERVICE ACCOUNTS (Least Privilege)
# ============================================================================

# Airflow service account
resource "google_service_account" "airflow_sa" {
  account_id   = "airflow-orchestrator"
  display_name = "Airflow Orchestration Service Account"
  description  = "Service account for Airflow to orchestrate data pipelines"
}

# Databricks service account
resource "google_service_account" "databricks_sa" {
  account_id   = "databricks-compute"
  display_name = "Databricks Compute Service Account"
  description  = "Service account for Databricks clusters"
}

# Airbyte service account
resource "google_service_account" "airbyte_sa" {
  account_id   = "airbyte-ingestion"
  display_name = "Airbyte Ingestion Service Account"
  description  = "Service account for Airbyte data ingestion"
}

# ============================================================================
# IAM BINDINGS (Least Privilege Access)
# ============================================================================

# Airflow can read/write to all buckets
resource "google_storage_bucket_iam_member" "airflow_landing" {
  bucket = google_storage_bucket.landing_zone.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.airflow_sa.email}"
}

resource "google_storage_bucket_iam_member" "airflow_bronze" {
  bucket = google_storage_bucket.bronze_layer.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.airflow_sa.email}"
}

# Databricks can read from landing/bronze, write to silver/gold
resource "google_storage_bucket_iam_member" "databricks_landing_read" {
  bucket = google_storage_bucket.landing_zone.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.databricks_sa.email}"
}

resource "google_storage_bucket_iam_member" "databricks_silver" {
  bucket = google_storage_bucket.silver_layer.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.databricks_sa.email}"
}

resource "google_storage_bucket_iam_member" "databricks_gold" {
  bucket = google_storage_bucket.gold_layer.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.databricks_sa.email}"
}

# Airbyte can write to landing zone only
resource "google_storage_bucket_iam_member" "airbyte_landing" {
  bucket = google_storage_bucket.landing_zone.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.airbyte_sa.email}"
}

# ============================================================================
# CLOUD SQL (PostgreSQL for metadata and pgvector)
# ============================================================================

resource "google_sql_database_instance" "postgres" {
  name             = "customer-lakehouse-postgres"
  database_version = "POSTGRES_15"
  region           = var.region
  
  settings {
    tier              = "db-custom-2-7680"  # 2 vCPU, 7.6GB RAM
    availability_type = var.environment == "prod" ? "REGIONAL" : "ZONAL"
    disk_size         = 100
    disk_type         = "PD_SSD"
    disk_autoresize   = true
    
    backup_configuration {
      enabled                        = true
      point_in_time_recovery_enabled = true
      start_time                     = "02:00"
      transaction_log_retention_days = 7
    }
    
    ip_configuration {
      ipv4_enabled    = false  # Private IP only
      private_network = google_compute_network.vpc.id
      require_ssl     = true
    }
    
    database_flags {
      name  = "max_connections"
      value = "100"
    }
    
    database_flags {
      name  = "shared_preload_libraries"
      value = "pg_stat_statements,pgvector"
    }
  }
  
  deletion_protection = var.environment == "prod" ? true : false
}

resource "google_sql_database" "airflow_metadata" {
  name     = "airflow"
  instance = google_sql_database_instance.postgres.name
}

resource "google_sql_database" "vector_db" {
  name     = "vectors"
  instance = google_sql_database_instance.postgres.name
}

resource "google_sql_user" "airflow_user" {
  name     = "airflow"
  instance = google_sql_database_instance.postgres.name
  password = var.airflow_db_password  # Store in Secret Manager
}

# ============================================================================
# VPC AND NETWORKING
# ============================================================================

resource "google_compute_network" "vpc" {
  name                    = "customer-lakehouse-vpc"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "subnet" {
  name          = "customer-lakehouse-subnet"
  ip_cidr_range = "10.0.0.0/24"
  region        = var.region
  network       = google_compute_network.vpc.id
  
  private_ip_google_access = true
}

# Cloud NAT for private instances
resource "google_compute_router" "router" {
  name    = "customer-lakehouse-router"
  region  = var.region
  network = google_compute_network.vpc.id
}

resource "google_compute_router_nat" "nat" {
  name                               = "customer-lakehouse-nat"
  router                             = google_compute_router.router.name
  region                             = var.region
  nat_ip_allocate_option             = "AUTO_ONLY"
  source_subnetwork_ip_ranges_to_nat = "ALL_SUBNETWORKS_ALL_IP_RANGES"
}

# ============================================================================
# SECRET MANAGER
# ============================================================================

resource "google_secret_manager_secret" "databricks_token" {
  secret_id = "databricks-token"
  
  replication {
    automatic = true
  }
  
  labels = local.common_tags
}

resource "google_secret_manager_secret" "airflow_db_password" {
  secret_id = "airflow-db-password"
  
  replication {
    automatic = true
  }
  
  labels = local.common_tags
}

# Grant Airflow SA access to secrets
resource "google_secret_manager_secret_iam_member" "airflow_databricks_token" {
  secret_id = google_secret_manager_secret.databricks_token.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.airflow_sa.email}"
}

# ============================================================================
# CLOUD LOGGING AND MONITORING
# ============================================================================

resource "google_logging_project_sink" "audit_logs" {
  name        = "customer-lakehouse-audit-logs"
  destination = "storage.googleapis.com/${google_storage_bucket.landing_zone.name}"
  
  filter = "resource.type=\"gcs_bucket\" OR resource.type=\"cloudsql_database\""
  
  unique_writer_identity = true
}

# ============================================================================
# CLOUD COMPOSER (Managed Airflow) - Optional
# ============================================================================

resource "google_composer_environment" "airflow" {
  count  = var.use_cloud_composer ? 1 : 0
  name   = "customer-lakehouse-composer"
  region = var.region
  
  config {
    software_config {
      image_version = "composer-2-airflow-2.7.3"
      
      pypi_packages = {
        apache-airflow-providers-databricks = ">=4.0.0"
        apache-airflow-providers-google     = ">=10.0.0"
        great-expectations                  = ">=0.18.0"
      }
      
      env_variables = {
        DATABRICKS_HOST = var.databricks_host
        GCS_BUCKET      = google_storage_bucket.landing_zone.name
      }
    }
    
    node_config {
      network         = google_compute_network.vpc.id
      subnetwork      = google_compute_subnetwork.subnet.id
      service_account = google_service_account.airflow_sa.email
    }
    
    workloads_config {
      scheduler {
        cpu        = 2
        memory_gb  = 7.5
        storage_gb = 5
        count      = 2
      }
      web_server {
        cpu        = 2
        memory_gb  = 7.5
        storage_gb = 5
      }
      worker {
        cpu        = 2
        memory_gb  = 7.5
        storage_gb = 5
        min_count  = 2
        max_count  = 6
      }
    }
  }
  
  labels = local.common_tags
}
