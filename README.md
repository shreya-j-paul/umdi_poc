## V4C Modular Ingestion Engine
A generic, configuration-driven Delta Live Tables (DLT) ingestion engine designed to handle both CloudFiles (Auto Loader) and JDBC (Aiven PostgreSQL) sources within the Databricks Lakehouse.

### 🏗 Data Architecture & Catalog Structure
The engine is built on Unity Catalog (UC) and follows the Bronze/Silver pattern for data ingestion.

Catalog: v4c_dev

Schema (Database): umdi_poc

Storage (Volumes): * Source Data: /Volumes/v4c_dev/umdi_poc/source_files/

Schema Storage: /Volumes/v4c_dev/umdi_poc/source_files/_schemas/

### 📁 Repository Structure
src/Ingestion_engine.py: The DLT Factory script that builds the pipeline dynamically.

src/ingestion_rules.py: Contains the AuditScanner class for granular file-level logging.

ingestion_config.yaml: The central configuration file where datasets are defined.

### 🚀 Setup & Deployment Guide
To run this pipeline in your own Databricks environment, follow these steps:

1. Update Configuration Path
In src/Ingestion_engine.py, find the EXECUTION block at the bottom. You must change the CONFIG_PATH to point to the location of your ingestion_config.yaml in your workspace:

Python
# Change this to your actual Workspace or Repo path
CONFIG_PATH = "/Workspace/Users/<your-email>/<project-folder>/ingestion_config.yaml"
2. Configure JDBC Credentials (Aiven)
For security, do not hardcode passwords in the YAML file for production.

Ensure your IP is allowlisted in the Aiven Console.

The current code expects the password in the YAML. It is highly recommended to replace the reader logic with dbutils.secrets.get(scope="aiven", key="pg_pwd").

3. Creating the DLT Pipeline
Go to Delta Live Tables in Databricks and click Create Pipeline.

Pipeline Name: V4C_Modular_Ingestion

Product Edition: Advanced (Required for DQ rules/Quarantine).

Pipeline Mode: Triggered (Batch) or Continuous (Streaming).

Source Code: Select src/Ingestion_engine.py from your repository.

Destination:

Catalog: v4c_dev

Target Schema: umdi_poc

Configuration (Optional): You can add a custom parameter pipeline.config_path if you want to pass the YAML path dynamically.

### 🛠 Features
1. Mixed Ingestion Logic
The engine automatically detects the source_type from the YAML:

file: Uses cloudFiles (Auto Loader) for incremental, efficient ingestion.

jdbc_source: Uses standard JDBC to snapshot data from Aiven PostgreSQL.

2. Automated Audit Logging
Every dataset ingestion creates a corresponding audit_log_<target_table>. This table tracks:

Total rows processed.

Success vs. Failed (rescued) rows.

Ingestion status (SUCCESS, PARTIAL_SUCCESS, or FAILED).

3. Quarantine & Data Quality
If quarantine: true is set in the YAML, any records failing the dq_rules are diverted to a <target_table>_quarantine table instead of being dropped, allowing for easy data reconciliation.

📝 Example Configuration (ingestion_config.yaml)
YAML
- target_table: "postgre_users_table"
  is_active: true
  source_type: "jdbc_source"
  format: "jdbc"
  source_path: "jdbc:postgresql://<host>:<port>/sample?ssl=require"
  dbtable: "sample_schema.users"
  user: "avnadmin"
  password: "your_password"
  load_strategy: "APPEND"
### Important Note for 
[!CAUTION]
If you change the Catalog or Schema names, ensure you update the schema_loc variable inside the generate_pipeline_nodes function in Ingestion_engine.py to match your new Volume path.
