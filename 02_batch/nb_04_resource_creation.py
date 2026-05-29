# Databricks notebook source
from pyspark.sql import functions as F
general_df = spark.table(
    "fvp_lab.trusted.general_silver"
)
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

(
    resource_df.write
    .format("delta")
    .mode("overwrite")
    .option(
        "overwriteSchema",
        "true"
    )
    .saveAsTable(
        "fvp_lab.cosmos.resource_cosmos"
    )
)

print(
    "Resource cosmos criado!"
)


# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC SELECT * FROM fvp_lab.cosmos.resource_cosmos

# COMMAND ----------

