# Databricks notebook source
from pyspark.sql.types import *


# COMMAND ----------

from pyspark.sql.types import *

schema = StructType([

    StructField(
        "policyId",
        StringType(),
        True
    ),

    StructField(
        "consentId",
        StringType(),
        True
    ),

    StructField(
        "allowed",
        BooleanType(),
        True
    ),

    StructField(
        "createdAt",
        TimestampType(),
        True
    ),

    # ==================================
    # NOVOS CAMPOS DE CONTROLE
    # ==================================

    StructField(
        "processedAt",
        TimestampType(),
        True
    ),

    StructField(
        "documentAvailable",
        BooleanType(),
        True
    )
])

empty_df = spark.createDataFrame(
    [],
    schema
)

(
    empty_df.write
    .format("delta")
    .mode("overwrite")
    .option(
        "overwriteSchema",
        "true"
    )
    .saveAsTable(
        "fvp_lab.streaming.policy_consent"
    )
)

print(
    "Tabela policy_consent criada!"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE fvp_lab.streaming.policy_consent;

# COMMAND ----------

