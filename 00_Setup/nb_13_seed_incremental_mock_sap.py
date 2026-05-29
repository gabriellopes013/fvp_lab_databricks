# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

mock_sap_path = (
    "/Volumes/fvp_lab/raw/mock_sap"
)

# COMMAND ----------

general_data = [

    # UPDATE
    (
        "P001",
        "POL10001",
        "CANCELLED",
        "AUTO",
        "2025-01-10",
        "2026-01-10"
    ),

    # EXISTENTE
    (
        "P002",
        "POL10002",
        "CANCELLED",
        "HOME",
        "2025-02-15",
        "2026-02-15"
    ),

    # NOVA POLICY
    (
        "P004",
        "POL10004",
        "ACTIVE",
        "LIFE",
        "2025-04-10",
        "2026-04-10"
    )
]

general_columns = [
    "policyId",
    "policyNumber",
    "status",
    "productType",
    "startDate",
    "endDate"
]

general_df = spark.createDataFrame(
    general_data,
    general_columns
)

(
    general_df.write
    .mode("overwrite")
    .option("header", True)
    .csv(
        f"{mock_sap_path}/general"
    )
)

print(
    "Carga incremental SAP criada!"
)

# COMMAND ----------

