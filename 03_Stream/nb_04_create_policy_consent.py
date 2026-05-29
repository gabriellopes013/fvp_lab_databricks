# Databricks notebook source

from pyspark.sql.types import *


# ==========================================
# PARAMETERS
# ==========================================

dbutils.widgets.text(
    "catalog",
    "fvp_lab"
)

CATALOG = dbutils.widgets.get(
    "catalog"
)

print(
    f"Catalog atual: {CATALOG}"
)


# ==========================================
# SCHEMA
# ==========================================

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


# ==========================================
# CREATE EMPTY TABLE
# ==========================================

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
        f"{CATALOG}.streaming.policy_consent"
    )
)

print(
    "Tabela policy_consent criada!"
)