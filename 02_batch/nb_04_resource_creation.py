# Databricks notebook source

from pyspark.sql import functions as F


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
# READ SILVER
# ==========================================

general_df = spark.table(
    f"{CATALOG}.trusted.general_silver"
)


# ==========================================
# BUILD RESOURCE
# ==========================================

resource_df = (

    general_df

    .select(
        "policyId",
        "productType"
    )

    .distinct()

    .withColumnRenamed(
        "productType",
        "resourceType"
    )

    .withColumn(

        "resourceId",

        F.concat(
            F.lit("RES_"),
            F.col("policyId")
        )
    )

    .withColumn(
        "consentId",
        F.lit(None)
        .cast("string")
    )

    .withColumn(
        "upsertTimestamp",
        F.current_timestamp()
    )
)


# ==========================================
# WRITE COSMOS
# ==========================================

(
    resource_df.write
    .format("delta")
    .mode("overwrite")
    .option(
        "overwriteSchema",
        "true"
    )
    .saveAsTable(
        f"{CATALOG}.cosmos.resource_cosmos"
    )
)

print(
    "Resource cosmos criado!"
)