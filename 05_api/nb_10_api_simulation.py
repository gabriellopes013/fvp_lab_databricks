# Databricks notebook source

from pyspark.sql import functions as F


# ==========================================
# PARAMETERS
# ==========================================

dbutils.widgets.text(
    "catalog",
    "fvp_lab"
)

dbutils.widgets.text(
    "policy_id",
    "P003"
)

CATALOG = dbutils.widgets.get(
    "catalog"
)

REQUESTED_POLICY_ID = (
    dbutils.widgets.get(
        "policy_id"
    )
)

print(
    f"Catalog atual: {CATALOG}"
)

print(
    f"Policy ID: "
    f"{REQUESTED_POLICY_ID}"
)


# ==========================================
# READ POLICY CONSENT
# ==========================================

policy_consent = (

    spark.table(
        f"{CATALOG}.streaming.policy_consent"
    )

    .filter(
        F.col("policyId")
        == REQUESTED_POLICY_ID
    )
)

consent = (
    policy_consent.collect()
)


# ==========================================
# VALIDATE CONSENT
# ==========================================

if len(consent) == 0:

    print(
        "Documento não autorizado "
        "(sem consentimento)"
    )

else:

    consent_row = (
        consent[0]
    )

    if consent_row["allowed"] is False:

        print(
            "Documento bloqueado"
        )

    else:

        consent_id = (
            consent_row[
                "consentId"
            ]
        )

        # ======================
        # READ COSMOS
        # ======================

        cosmos_df = (

            spark.table(
                f"{CATALOG}.cosmos.general_cosmos"
            )

            .filter(
                F.col("policyId")
                == REQUESTED_POLICY_ID
            )
        )

        # ======================
        # ADD CONSENT ID
        # ======================

        response_df = (

            cosmos_df

            .withColumn(
                "consentId",
                F.lit(
                    consent_id
                )
            )
        )

        print(
            "Documento retornado:"
        )

        display(
            response_df
        )