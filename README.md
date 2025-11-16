# Customer Lakehouse Pipeline on GCP — Portfolio Repo

This repository is a polished portfolio example that demonstrates strong, production-oriented experience with **Airflow**, **Databricks (Spark, Delta Lake, SQL, Python)**, **Airbyte**, and the **GCP** ecosystem. It showcases secure ingestion, transformation, orchestration, and serving layers suitable for regulated healthcare environments (HIPAA-aware) and modern AI/LLM embedding pipelines.

---

## Repository overview

```
customer-lakehouse-portfolio/
├─ README.md
├─ docs/
│  ├─ architecture.md
│  ├─ security_hipaa.md
│  ├─ runbook.md
│  └─ cost_estimates.md
├─ infra/
│  ├─ terraform/
│  │  ├─ main.tf
│  │  ├─ variables.tf
│  │  └─ outputs.tf
│  └─ k8s/
├─ airflow/
│  ├─ dags/
│  │  ├─ ingestion_dag.py
│  │  ├─ delta_transform_dag.py
│  │  └─ embeddings_dag.py
│  ├─ plugins/
│  └─ requirements.txt
├─ databricks/
│  ├─ notebooks/
│  │  ├─ 00_ingest_streaming.py
│  │  ├─ 01_delta_schema_evolution.ipynb
│  │  ├─ 02_transform_and_test.py
│  │  └─ 03_embeddings_and_vector_store.py
│  └─ jobs/
│     └─ job_definitions.json
├─ airbyte/
│  ├─ connectors/
│  │  └─ custom_api_connector/  # example custom connector skeleton
│  └─ configs/
│     └─ sources_and_connections.yaml
├─ examples/
│  ├─ sample_patient_data.csv
│  └─ sample_queries.sql
├─ analytics/
│  └─ superset/
│     └─ superset_dashboard_export.json
├─ ci/
│  └─ github-actions/
│     └─ ci-cd.yml
├─ scripts/
│  ├─ local_dev_setup.sh
│  └─ databricks_deploy.py
└─ LICENSE
```

---

## README.md (what the repo demonstrates)

This project README contains:
- A short one-paragraph project description emphasizing HIPAA-aware design and GCP + Databricks + Airflow orchestration.
- Quickstart: how to run locally with Docker Compose for Airflow, Airbyte, and a small PySpark/local Delta emulation.
- Architecture diagram snapshot & explanation.
- Examples: run an ingestion via Airbyte -> landing zone in GCS -> Databricks job converts to Delta -> Airflow orchestrates daily runs -> indexing to embeddings -> vector store.
- Security & compliance section listing Key Vault/Secret Manager usage, RBAC, encryption-at-rest, VPC-SC, CMEK guidance.

---

## Key files to review (high level)

### `airflow/dags/ingestion_dag.py`
- An Airflow DAG demonstrating: AirbyteOperator (or simple REST trigger), sensor for job completion, move files to GCS, trigger Databricks job via DatabricksSubmitRunOperator.
- Uses TaskFlow style for clarity and built-in XComs for metadata passing.

### `airflow/dags/delta_transform_dag.py`
- Triggers Databricks notebook to perform Delta writes, demonstrates schema evolution (merge, CDC handling), data quality checks using `great_expectations` or simple SQL assertions.
- Shows how to implement incremental processing using watermarking columns.

### `airflow/dags/embeddings_dag.py`
- Runs a Databricks job that:
  1. extracts text fields,
  2. uses a small embedding model (example: sentence-transformers local or calls to a secure LLM endpoint),
  3. writes vectors to a vector DB (example: managed Pinecone or pgvector on Cloud SQL) and to Delta for auditing.

### `databricks/notebooks/02_transform_and_test.py`
- PySpark code snippet that reads from GCS/Delta, performs transformations with tests, writes to Delta Lake using `merge` for idempotency, and commits schema changes using Delta `replaceWhere` or `overwriteSchema` patterns where appropriate.

### `databricks/notebooks/03_embeddings_and_vector_store.py`
- Example showing how to produce embeddings for patient notes (de-identified or synthetic data for portfolio demo), storing vectors in Delta and in a vector DB.

### `airbyte/connectors/custom_api_connector/`
- Skeleton for a custom Airbyte connector (source) that implements incremental sync with opaque cursor handling, backoff, and retry logic; sample dockerfile and connector spec.

### `infra/terraform/`
- Minimal Terraform to provision:
  - GCP buckets (with uniform bucket-level access & CMEK),
  - Service accounts with least-privilege IAM roles,
  - Cloud SQL (Postgres) for metadata & optionally pgvector,
  - Secret Manager entries for credentials,
  - Databricks workspace resources (using provider),
  - VPC and firewall rules.

### `ci/github-actions/ci-cd.yml`
- Linting (Python, SQL), unit tests, `dbt` or SQL model tests (if included), Databricks notebook validation (e.g., `databricks-cli` job submit dry-run), and a deploy job that applies Terraform and triggers Airflow DAG refresh.

---

## Security & HIPAA considerations (`docs/security_hipaa.md`)
- Use only de-identified/synthetic data in public portfolio repo.
- Ensure encryption at rest (GCS + Cloud SQL) and in transit (TLS), use Customer-Managed Encryption Keys (CMEK) where possible.
- Use Secret Manager for all secrets and short-lived service account keys or Workload Identity where applicable.
- Describe audit logging: Cloud Audit Logs, Databricks audit logs, Airflow logs in secure storage.
- Data access control: least-privilege IAM roles, row-level security in analytics, and dataset-level ACLs on Delta tables.
- Operational runbook for breach response, retention policy, and data deletion.

---

## Example code snippets (in repo content)

- **Airflow DAG (Task skeleton)** — located at `airflow/dags/ingestion_dag.py` (complete example in file)
- **Databricks PySpark snippet** — a robust `merge` into Delta template with schema evolution handling.
- **Embedding pipeline** — notebook that tokenizes/anonymizes, calls an embedding model, and persists vectors.

> All these examples are included as runnable snippets and documented with comments explaining why choices were made (idempotency, backpressure, cost control, monitoring).

---

## Demo data & privacy
- `examples/sample_patient_data.csv` — small synthetic dataset with realistic fields: patient_id, encounter_id, note_text, dt_event.
- A script `scripts/deidentify.py` shows how you'd remove PHI and store safe demo data.

---

## How this repo shows your strengths
- **Airflow & orchestration:** multiple DAGs, sensors, operator patterns, error handling and SLA management.
- **Databricks & Delta Lake:** notebook examples for streaming and batch, schema evolution, time travel, and versioned datasets.
- **Ingestion tools:** Airbyte configurations and custom source connector template to show breadth (Airbyte, plus how to build a connector).
- **GCP infra & security:** Terraform scaffolding demonstrating familiarity with GCP Data Lake (GCS), Secret Manager, Cloud SQL, VPC, and CMEK.
- **Regulated environments:** explicit HIPAA sections, encryption, audit logging, and secure testing patterns.
- **AI/LLM readiness:** embedding notebook, vector store integration notes, capability to add Pinecone/pgvector.
- **Dev practices:** CI/CD, GitHub Actions, code organization, tests, and doc-driven approach.

---

## Next steps / how I can help
I created this scaffold to be a strong starting point for a portfolio repository that *speaks directly to your skills*. I can:

- Generate full file contents for any or all of the files (e.g., complete Airflow DAGs, Databricks notebooks, Terraform modules).
- Produce a polished `README.md` ready for GitHub with diagrams and badges.
- Create CI pipeline YAML and a sample GitHub repo tree you can `git clone`/push.

Tell me which files you want generated first (I will add them to the repo content): e.g., `airflow/dags/ingestion_dag.py`, `databricks/notebooks/02_transform_and_test.py`, or `infra/terraform/main.tf`.

---

*End of scaffold.*

# customer-lakehouse-gcp-databricks-airflow
