# Customer Lakehouse: GCP + Databricks + Airflow

[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

A production-grade data lakehouse implementation demonstrating modern data engineering practices with healthcare data. This project showcases secure ingestion, transformation, and orchestration patterns suitable for regulated environments (HIPAA-aware design).

## 🏗️ Architecture

```
┌─────────────┐     ┌──────────────┐     ┌─────────────────┐
│   Airbyte   │────▶│  GCS Landing │────▶│   Databricks    │
│  (Ingestion)│     │     Zone     │     │  (Processing)   │
└─────────────┘     └──────────────┘     └─────────────────┘
                           │                       │
                           ▼                       ▼
                    ┌──────────────┐     ┌─────────────────┐
                    │   Airflow    │     │  Delta Lake     │
                    │ (Orchestrate)│     │  (Bronze/Silver/│
                    └──────────────┘     │     Gold)       │
                                         └─────────────────┘
                                                  │
                                                  ▼
                                         ┌─────────────────┐
                                         │   Analytics &   │
                                         │   Vector Store  │
                                         └─────────────────┘
```

**Key Components:**
- **Ingestion**: Airbyte for ELT from multiple sources
- **Storage**: Google Cloud Storage with CMEK encryption
- **Processing**: Databricks (PySpark, Delta Lake)
- **Orchestration**: Apache Airflow (Composer or self-hosted)
- **Analytics**: Superset dashboards, vector embeddings for AI

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose
- Python 3.9+
- Google Cloud account (for production)
- Databricks account (for production)

### Local Development Setup

```bash
# Clone the repository
git clone https://github.com/IamMrMac/customer-lakehouse-gcp-databricks-airflow.git
cd customer-lakehouse-gcp-databricks-airflow

# Run setup script
chmod +x scripts/local_dev_setup.sh
./scripts/local_dev_setup.sh

# Start services
docker-compose up -d

# Access services
# Airflow: http://localhost:8080 (admin/admin)
# Airbyte: http://localhost:8000 (airbyte/password)
```

### Running the Pipeline

```bash
# Generate synthetic patient data
python scripts/generate_sample_data.py

# Trigger the ingestion DAG
docker-compose exec airflow-webserver airflow dags trigger ingestion_dag

# Monitor progress
docker-compose logs -f airflow-scheduler
```

## 📁 Project Structure

```
customer-lakehouse-gcp-databricks-airflow/
├── README.md
├── docker-compose.yml                    # Local dev environment
├── .env.example                          # Environment variables template
│
├── docs/
│   ├── architecture.md                   # Detailed architecture guide
│   ├── security_hipaa.md                 # HIPAA compliance details
│   ├── runbook.md                        # Operations guide
│   └── cost_estimates.md                 # GCP cost breakdown
│
├── airflow/
│   ├── dags/
│   │   ├── ingestion_dag.py             # Main ingestion pipeline
│   │   ├── delta_transform_dag.py       # Bronze -> Silver -> Gold
│   │   └── embeddings_dag.py            # Vector embeddings generation
│   ├── plugins/
│   │   └── custom_operators.py          # Custom Airflow operators
│   ├── config/
│   │   └── airflow.cfg
│   └── requirements.txt
│
├── databricks/
│   ├── notebooks/
│   │   ├── 00_ingest_streaming.py       # Structured streaming setup
│   │   ├── 01_bronze_to_silver.py       # Data quality & cleansing
│   │   ├── 02_silver_to_gold.py         # Business logic transforms
│   │   └── 03_embeddings_pipeline.py    # Generate vectors for AI
│   └── jobs/
│       └── job_configs.json              # Databricks job definitions
│
├── airbyte/
│   ├── connectors/
│   │   └── custom_api_connector/        # Custom source connector
│   └── configs/
│       └── connections.yaml              # Source/destination configs
│
├── infra/
│   ├── terraform/
│   │   ├── main.tf                       # GCP infrastructure
│   │   ├── variables.tf
│   │   ├── outputs.tf
│   │   └── modules/
│   │       ├── gcs/
│   │       ├── composer/
│   │       └── databricks/
│   └── k8s/
│       └── airflow-helm-values.yaml      # K8s deployment config
│
├── scripts/
│   ├── local_dev_setup.sh                # Local environment setup
│   ├── generate_sample_data.py           # Create synthetic data
│   ├── databricks_deploy.py              # Deploy notebooks/jobs
│   └── deidentify.py                     # PHI removal utilities
│
├── examples/
│   ├── sample_patient_data.csv           # Synthetic healthcare data
│   └── sample_queries.sql                # Example analytics queries
│
├── tests/
│   ├── unit/
│   │   ├── test_transforms.py
│   │   └── test_dags.py
│   └── integration/
│       └── test_end_to_end.py
│
└── ci/
    └── .github/
        └── workflows/
            ├── ci.yml                     # Lint, test, validate
            └── deploy.yml                 # Deploy to production
```

## 🔐 Security & Compliance

This project implements HIPAA-aware design patterns:

- **Encryption**: Data encrypted at rest (CMEK) and in transit (TLS)
- **Access Control**: Least-privilege IAM, RBAC on Databricks
- **Secrets Management**: GCP Secret Manager integration
- **Audit Logging**: Comprehensive logging to Cloud Logging
- **Data Governance**: Row-level security, column masking
- **Backup & Recovery**: Point-in-time recovery with Delta Lake time travel

See [docs/security_hipaa.md](docs/security_hipaa.md) for complete details.

## 🧪 Data Pipeline

### Bronze Layer (Raw Data)
- Ingests data from Airbyte sources
- Minimal transformations (schema inference)
- Full audit trail with metadata columns

### Silver Layer (Cleaned Data)
- Data quality checks with Great Expectations
- Deduplication and cleansing
- Schema enforcement and evolution
- CDC handling with Delta merge

### Gold Layer (Business Logic)
- Aggregations and denormalization
- Business KPIs and metrics
- Optimized for analytics queries
- Vector embeddings for AI/ML

## 📊 Features Demonstrated

### Data Engineering
- ✅ ELT pipeline orchestration with Airflow
- ✅ Delta Lake medallion architecture
- ✅ Schema evolution and time travel
- ✅ Incremental processing with watermarks
- ✅ Data quality validation
- ✅ Idempotent transforms with merge/upsert

### DevOps & MLOps
- ✅ Infrastructure as Code (Terraform)
- ✅ CI/CD with GitHub Actions
- ✅ Containerized local development
- ✅ Unit and integration tests
- ✅ Automated deployment pipelines

### AI/ML Integration
- ✅ Text embeddings generation (sentence-transformers)
- ✅ Vector store integration (pgvector/Pinecone)
- ✅ De-identification utilities for PHI
- ✅ RAG-ready data preparation

## 🔧 Configuration

### Environment Variables

```bash
# GCP
export GCP_PROJECT_ID=your-project-id
export GCS_BUCKET=your-data-bucket
export GCP_REGION=us-central1

# Databricks
export DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
export DATABRICKS_TOKEN=dapi...

# Airflow
export AIRFLOW__CORE__EXECUTOR=LocalExecutor
export AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://...
```


## 🧑‍💻 Development

### Running Tests

```bash
# Unit tests
pytest tests/unit/ -v

# Integration tests (requires Docker)
pytest tests/integration/ -v

# Coverage report
pytest --cov=airflow --cov=databricks tests/
```

### Code Quality

```bash
# Format code
black airflow/ databricks/ scripts/

# Lint
flake8 airflow/ databricks/
pylint airflow/ databricks/

# Type checking
mypy airflow/dags/
```

## 📚 Documentation

- [Architecture Guide](docs/architecture.md) - Detailed system design
- [Security & HIPAA](docs/security_hipaa.md) - Compliance details
- [Operations Runbook](docs/runbook.md) - Day-to-day operations
- [Cost Optimization](docs/cost_estimates.md) - Managing cloud costs

## 🤝 Contributing

This is a portfolio project, but suggestions are welcome! Please open an issue or submit a PR.

## 📝 License

MIT License - see [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Uses synthetic patient data generated with Faker
- Architecture inspired by Databricks' medallion pattern
- Security patterns from Google Cloud healthcare reference architectures

## 📧 Contact

**Your Name** - [@IamMrMac](https://github.com/IamMrMac)

Project Link: [https://github.com/IamMrMac/customer-lakehouse-gcp-databricks-airflow](https://github.com/IamMrMac/customer-lakehouse-gcp-databricks-airflow)

---

**Note**: This project uses de-identified synthetic data for demonstration purposes. Always consult with legal and compliance teams before handling real PHI/PII data.
