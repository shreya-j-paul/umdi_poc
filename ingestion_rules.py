from pyspark.sql.functions import col, lit, current_timestamp, count, when, sum as _sum

class AuditScanner:
    """
    Refactored for granular File-Level Tracking.
    Ensures that Batch and Streaming ingestion both preserve individual file logs.
    """
    @staticmethod
    def generate_file_status(df, target_name):
        # Determine if Auto Loader or Batch read rescued malformed data
        has_rescue = "_rescued_data" in df.columns
        failure_expr = col("_rescued_data").isNotNull() if has_rescue else lit(False)
        
        # Grouping by _source_file ensures one row per unique file in the audit log
        return df.groupBy("_source_file").agg(
            lit(target_name).alias("source_name"),
            count("*").alias("total_rows"),
            _sum(when(failure_expr, 1).otherwise(0)).alias("failed_rows"),
            _sum(when(~failure_expr, 1).otherwise(0)).alias("success_rows"),
            current_timestamp().alias("audit_timestamp")
        ).withColumn(
            "status",
            when(col("failed_rows") == col("total_rows"), "FAILED") 
            .when(col("failed_rows") > 0, "PARTIAL_SUCCESS")       
            .otherwise("SUCCESS")
        ).withColumn(
            "attempt_count", lit(1) 
        )