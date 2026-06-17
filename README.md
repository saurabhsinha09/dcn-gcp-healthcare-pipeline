# dcn-gcp-healthcare-pipeline

An end-to-end GCP Data Engineering pipeline designed for the Healthcare domain. This project implements a Medallion architecture (Bronze, Silver, Gold) to ingest, transform, and analyze healthcare data from disparate sources including MySQL databases, CSV files, and external APIs.

##  Business Context

In the modern healthcare landscape, data is often siloed across different hospital branches, legacy databases, and third-party APIs. This project addresses the need for a unified data platform to solve:
*   **Data Fragmentation**: Consolidating patient, provider, and department information from multiple hospital systems (Hospital A & B).
*   **Historical Tracking**: Ensuring clinical and demographic changes are preserved for auditing and longitudinal studies.
*   **Revenue Cycle Management (RCM)**: Integrating claims data with clinical encounters to analyze financial health and provider performance.

## 🏗️ Project Architecture
![Architecture Diagram](architecture.jpg)

```mermaid
graph TD
    subgraph Sources [Data Sources]
        A[Cloud SQL - Hospital A/B]
        B[GCS - Claims CSV]
        C[External API - NPI Registry]
    end

    subgraph Orchestration [Orchestration]
        D[Cloud Composer / Airflow]
    end

    subgraph Processing [Processing & Storage]
        E[Cloud Dataproc / PySpark]
        F[(GCS Landing / Archive)]
        G[(BigQuery - Bronze/Silver/Gold)]
    end

    D -->|Triggers| E
    E -->|Extract| A
    A -->|Landing| F
    F -->|External Tables| G
    B -->|Direct Load| G
    C -->|Ingest| G
    D -->|SQL Transformations| G
    G -->|Analytics| H[Looker / Tableau / BI]
```

The pipeline follows a modern data engineering workflow on Google Cloud Platform:

1.  **Data Sources**:
    *   **Cloud SQL (MySQL)**: Transactional data from multiple hospital systems (Hospital A, Hospital B).
    *   **GCS Landing**: Raw CSV files for claims and metadata.
    *   **External API**: NPI (National Provider Identifier) registry data fetched via Python.
2.  **Ingestion & Processing**:
    *   **Cloud Composer (Airflow)**: Orchestrates the entire workflow.
    *   **Cloud Dataproc**: Executes PySpark jobs to extract data from MySQL via JDBC, perform initial cleansing, and handle watermarking.
3.  **Storage & Warehouse (BigQuery)**:
    *   **Landing (GCS)**: Raw JSON/Parquet files stored in Cloud Storage.
    *   **Bronze**: Raw data mirrored in BigQuery tables.
    *   **Silver**: Cleaned and standardized data with deduplication and unified schemas.
    *   **Gold**: Aggregated business-ready tables for analytics (e.g., Provider Performance, Financial Metrics).

## 🚀 Tech Stack

*   **Orchestration**: Google Cloud Composer (Apache Airflow)
*   **Compute**: Google Cloud Dataproc (Spark/PySpark)
*   **Storage**: Google Cloud Storage (GCS)
*   **Warehouse**: Google BigQuery
*   **Database**: Google Cloud SQL (MySQL)
*   **Languages**: Python, PySpark, SQL

## 📂 Project Structure

```text
├── architecture.jpg            # Architecture diagram
├── data/
│   ├── BQ/                     # SQL scripts for Medallion transformations
│   │   ├── bronze.sql          # Raw to Bronze loading
│   │   ├── silver.sql          # Silver layer (SCD Type 2/Cleansing)
│   │   └── gold.sql            # Gold layer (Analytics/Aggregations)
│   └── INGESTION/              # PySpark scripts for data extraction
│       ├── hospitalA_mysqlToLanding.py
│       ├── hospitalB_mysqlToLanding.py
│       ├── claims.py           # GCS CSV ingestion
│       ├── cpt_codes.py        # CPT codes metadata ingestion
│       ├── npi_codes.py        # External API (NPI Registry) ingestion
│       └── icd_codes.py        # External API (WHO ICD-10) ingestion
├── utils/
│   ├── add_dags_to_composer.py # Helper script to sync local files to Composer bucket
│   └── requirements.txt        # Python dependencies
└── workflows/
    ├── pyspark_dag.py          # Airflow DAG for Dataproc cluster and ingestion jobs
    └── bq_dag.py               # Airflow DAG for BigQuery SQL transformations
```

## ⚙️ Pipeline Details

### 🔑 Key Techniques
*   **Incremental Ingestion (Watermarking)**: PySpark scripts utilize a high-watermarking strategy, querying BigQuery audit logs to fetch only new records from MySQL, reducing I/O and cost.
*   **SCD Type 2 (Slowly Changing Dimensions)**: Implemented in the Silver layer using BigQuery `MERGE` statements. This ensures a full history of patient and transaction changes is maintained with `is_current` and `effective_date` flags.
*   **Data Quality & Quarantine**: Records failing critical validation (e.g., missing Patient IDs or malformed dates) are flagged as `is_quarantined` rather than dropped, allowing for downstream data stewardship.
*   **Automated Observability**: Every ingestion run logs status, record counts, and timestamps to a centralized BigQuery audit table and GCS JSON logs.
*   **Medallion Storage**: Progressive data refinement from Raw (Bronze) to Standardized (Silver) to Analytic (Gold).

### 1. Ingestion Workflow (`pyspark_dag`)
*   **Cluster Management**: Dynamically starts a Dataproc cluster and stops it upon completion to optimize costs.
*   **Hospital Ingestion**: Incremental and full loads from MySQL to GCS Landing using watermarking tracked in BigQuery.
*   **Claims & NPI**: Ingests claims from GCS and fetches California-based provider data from the NPI Registry API.

### 2. Transformation Workflow (`bigquery_dag`)
*   **Bronze Layer**: Loads raw data from GCS.
*   **Silver Layer**: Standardizes Patient IDs, handles data types, and implements "Quarantine" flags for data quality.
*   **Gold Layer**: Generates high-value analytics tables including:
    *   `patient_history`: Unified view of patient visits and financial interactions.
    *   `provider_performance`: Analysis of claim approval rates and billed amounts.
    *   `department_performance`: Efficiency and revenue metrics by department.

## 🛠️ Setup & Deployment

### Prerequisites
*   GCP Project with billing enabled.
*   APIs enabled: Dataproc, BigQuery, Cloud SQL, Cloud Composer.
*   `gcloud` CLI configured.

### 1. Cloud Infrastructure Setup
Before deploying the pipeline, you must set up the networking and compute infrastructure.

**Networking (NAT Gateway):**
Because the Dataproc nodes do not have public IP addresses (`--no-address`), you must configure a Cloud NAT to allow the ingestion scripts to access external APIs.
```bash
# Create a Cloud Router
gcloud compute routers create healthcare-router --network default --region asia-south1

# Create a Cloud NAT Gateway
gcloud compute routers nats create healthcare-nat --router healthcare-router --region asia-south1 --auto-allocate-nat-external-ips --nat-all-subnet-ip-ranges

# Create Dataproc Cluster
gcloud dataproc clusters create my-demo-cluster1 --enable-component-gateway --region asia-south1 \
--subnet default --no-address --master-machine-type n4-standard-2 --master-boot-disk-type hyperdisk-balanced \
--master-boot-disk-size 100 --num-workers 2 --worker-machine-type n4-standard-2 \
--worker-boot-disk-type hyperdisk-balanced --worker-boot-disk-size 100 --image-version 2.3-debian12 \
--optional-components ICEBERG,DELTA,JUPYTER --scopes 'https://www.googleapis.com/auth/cloud-platform' \
--project {project_id} --zone asia-south1-a
```

### 2. Deployment
1.  **Configure Environment Variables**:
    The pipeline relies on environment variables for security and flexibility. Set the following variables in your Cloud Composer environment (Environment Configuration > Environment variables):

    | Variable Name | Description | Example Value |
    | :--- | :--- | :--- |
    | `PROJECT_ID` | Your GCP Project ID | `dcn-development` |
    | `REGION` | GCP Region for Dataproc/Composer | `asia-south1` |
    | `DATAPROC_CLUSTER_NAME` | Name of the Dataproc cluster | `my-demo-cluster1` |
    | `COMPOSER_BUCKET` | The GCS bucket created by Composer | `asia-south1-dcn-healthcare-bucket` |
    | `GCS_BUCKET` | The main bucket for the Data Lake | `dcn-healthcare-bucket` |
    | `MYSQL_USER` | Username for Cloud SQL | `myuser` |
    | `MYSQL_PASSWORD` | Password for Cloud SQL | `Welcome1234` |
    | `MYSQL_HOST_A` | IP/Host for Hospital A MySQL | `34.100.130.157` |
    | `MYSQL_HOST_B` | IP/Host for Hospital B MySQL | `35.244.46.135` |
    | `MYSQL_DB_A` | Database name for Hospital A | `hospital_a_db` |
    | `MYSQL_DB_B` | Database name for Hospital B | `hospital_b_db` |
    | `NOTIFICATION_EMAIL` | Email for Airflow alerts | `admin@example.com` |

    *Note: For production, it is recommended to store `MYSQL_PASSWORD` in **Secret Manager** instead of environment variables.*

2.  **Upload Code to Cloud Storage**:
    Use the utility script to upload DAGs and PySpark scripts to your Composer environment:
    ```bash
    python utils/add_dags_to_composer.py --dags_directory ./workflows --dags_bucket <YOUR_BUCKET> --data_directory ./data
    ```

3.  **Install Dependencies**:
    Ensure the Python dependencies are installed in your Composer environment. You can upload `utils/requirements.txt` via the Composer "PyPI Packages" tab.

4.  **Configure MySQL**:
    Ensure your Cloud SQL instances allow connections from the Dataproc cluster and that credentials match those in the ingestion scripts.

## 📊 Analytics & Reporting

The Gold layer tables are designed for direct consumption by BI tools like Looker or Tableau.
*   **Financial Health**: Outstanding balance calculations and claim success rates.
*   **Operational Efficiency**: Total encounters and average payments per transaction.

---
*Maintained by Saurabh Sinha*
