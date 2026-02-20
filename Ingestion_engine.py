# Databricks notebook source
import dlt
import yaml
from pyspark.sql.functions import col, lit, current_timestamp
from ingestion_rules import AuditScanner

# COMMAND ----------

# -------------------------------------------------------------------------
# 1. CONFIGURATION HELPERS
# -------------------------------------------------------------------------
def load_config(path):
    """Loads the YAML configuration file."""
    with open(path, 'r') as f:
        return yaml.safe_load(f).get('datasets', [])


# COMMAND ----------

# -------------------------------------------------------------------------
# 2. SOURCE READER HELPERS
# -------------------------------------------------------------------------
def get_jdbc_reader(cfg, path):
    """Configures a static Spark reader for JDBC sources."""
    return (spark.read.format("jdbc")
            .option("url", path)
            .option("dbtable", cfg.get('dbtable'))
            .option("user", cfg.get('user'))
            .option("password", cfg.get('password'))
            .option("driver", "org.postgresql.Driver")
            .load()
            .withColumn("_ingestion_timestamp", current_timestamp())
            .withColumn("_source_file", lit(cfg.get('dbtable'))))

def get_file_reader(cfg, path, fmt, strategy, schema_loc):
    """Configures a streaming or batch reader for file sources."""
    is_json = (fmt.lower() == "json")
    
    if strategy in ["OVERWRITE", "FULL_LOAD"]:
        # Batch Read
        reader = spark.read.format(fmt)
        if is_json: reader = reader.option("multiLine", "true")
        else: reader = reader.option("header", "true")
        
        return (reader.option("inferSchema", "true").load(path)
                .select("*", col("_metadata.file_path").alias("_source_file"))
                .withColumn("_ingestion_timestamp", current_timestamp()))
    else:
        # Streaming Read (Auto Loader)
        reader = (spark.readStream.format("cloudFiles")
                .option("cloudFiles.format", fmt)
                .option("cloudFiles.schemaLocation", schema_loc) 
                .option("cloudFiles.schemaEvolutionMode", "rescue"))
        if is_json: reader = reader.option("multiLine", "true")
        else: reader = reader.option("header", "true")
            
        return (reader.load(path)
                .withColumn("_ingestion_timestamp", current_timestamp())
                .withColumn("_source_file", col("_metadata.file_path")))


# COMMAND ----------

# -------------------------------------------------------------------------
# 3. CORE FACTORY FUNCTION
# -------------------------------------------------------------------------
def generate_pipeline_nodes(cfg):
    # Extract settings
    target_name = cfg['target_table']
    src_path = cfg['source_path']
    src_type = cfg.get('source_type', 'file')
    fmt = cfg.get('format', 'csv')
    strategy = cfg.get('load_strategy', 'APPEND').upper()
    dq_rules = cfg.get('dq_rules', {})
    is_quarantine_enabled = cfg.get('quarantine', True)

    # Derived names and paths
    raw_view = f"{target_name}_raw"
    audit_table = f"audit_log_{target_name}"
    quarantine_table = f"{target_name}_quarantine"
    schema_loc = f"/Volumes/v4c_dev/umdi_poc/source_files/_schemas/{target_name}"

    # --- NODE 1: RAW VIEW ---
    @dlt.view(name=raw_view)
    def create_raw():
        if src_type == "jdbc_source":
            return get_jdbc_reader(cfg, src_path)
        return get_file_reader(cfg, src_path, fmt, strategy, schema_loc)

    # --- NODE 2: AUDIT LOG ---
    @dlt.table(name=audit_table)
    def create_audit():
        # Conditional read to prevent streaming errors for JDBC
        if src_type == "jdbc_source" or strategy in ["OVERWRITE", "FULL_LOAD"]:
            reader = dlt.read(raw_view)
        else:
            reader = dlt.read_stream(raw_view)
        return AuditScanner.generate_file_status(reader, target_name)

    # --- NODE 3: TARGET TABLE ---
    if strategy == "MERGE":
        dlt.create_streaming_table(name=target_name)
        dlt.apply_changes(
            target=target_name, 
            source=raw_view, 
            keys=cfg.get('primary_keys', []), 
            sequence_by=col("_ingestion_timestamp"), 
            stored_as_scd_type=cfg.get('scd_type', 1)
        )

    else:
        @dlt.table(name=target_name)
        def create_target():
            # Conditional read
            if src_type == "jdbc_source" or strategy in ["OVERWRITE", "FULL_LOAD"]:
                df = dlt.read(raw_view)
            else:
                df = dlt.read_stream(raw_view)
            
            # Apply DQ filtering
            if is_quarantine_enabled and dq_rules:
                valid_expr = " AND ".join([f"({e})" for e in dq_rules.values()])
                df = df.filter(valid_expr)
            return df

    # --- NODE 4: QUARANTINE ---
    if dq_rules and is_quarantine_enabled:
        @dlt.table(name=quarantine_table)
        def create_quarantine():
            if src_type == "jdbc_source" or strategy in ["OVERWRITE", "FULL_LOAD"]:
                reader = dlt.read(raw_view)
            else:
                reader = dlt.read_stream(raw_view)
            
            invalid_expr = " AND ".join([f"({expr})" for expr in dq_rules.values()])
            return reader.filter(f"NOT ({invalid_expr})")


# COMMAND ----------

# -------------------------------------------------------------------------
# 4. EXECUTION
# -------------------------------------------------------------------------
CONFIG_PATH = "/Workspace/Users/shreya.j.paul@v4c.ai/Dev_Final_4/ingestion_config.yaml"
datasets = load_config(CONFIG_PATH)

for dataset_config in datasets:
    if dataset_config.get('is_active', True): 
        generate_pipeline_nodes(dataset_config)