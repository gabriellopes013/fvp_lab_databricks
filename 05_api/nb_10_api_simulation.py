# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

spark.sql("USE CATALOG fvp_lab")

# COMMAND ----------

requested_policy_id = "P003"

# COMMAND ----------

policy_consent = (
    spark.table(
        "fvp_lab.streaming.policy_consent"
    )
    .filter(
        F.col("policyId") == requested_policy_id
    )
)


# COMMAND ----------

consent = policy_consent.collect()

# COMMAND ----------

if len(consent) == 0:

    print(
        "Documento não autorizado "
        "(sem consentimento)"
    )

else:

    consent_row = consent[0]

    if consent_row["allowed"] is False:

        print(
            "Documento bloqueado"
        )

    else:

        consent_id = (
            consent_row["consentId"]
        )

        # ==========================
        # CONSULTA COSMOS
        # ==========================

        cosmos_df = (
            spark.table(
                "fvp_lab.streaming.cosmos_policy"
            )
            .filter(
                F.col("policyId")
                == requested_policy_id
            )
        )

        # ==========================
        # SUBSTITUI NULL
        # ==========================

        response_df = (
            cosmos_df
            .withColumn(
                "consentId",
                F.lit(consent_id)
            )
        )

        print(
            "Documento retornado:"
        )

        display(response_df)

# COMMAND ----------

