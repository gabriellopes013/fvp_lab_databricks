# Databricks notebook source

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType


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
# GENERIC MERGE FUNCTION
# ==========================================

def merge_to_silver(
    df,
    target_table,
    merge_condition
):

    # primeira execução
    if not spark.catalog.tableExists(
        target_table
    ):

        (
            df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(
                target_table
            )
        )

        print(
            f"Tabela criada: "
            f"{target_table}"
        )

    # incremental
    else:

        target = DeltaTable.forName(
            spark,
            target_table
        )

        (
            target.alias("t")
            .merge(
                df.alias("s"),
                merge_condition
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

        print(
            f"MERGE executado: "
            f"{target_table}"
        )


# COMMAND ----------
# GENERAL
# COMMAND ----------

general_df = spark.table(
    f"{CATALOG}.raw.general_bronze"
)

merge_to_silver(
    general_df,
    f"{CATALOG}.trusted.general_silver",
    "t.policyId = s.policyId"
)

print(
    "General processada!"
)


# COMMAND ----------
# POLICY INFO
# COMMAND ----------

policy_info_df = spark.table(
    f"{CATALOG}.raw.policy_info_bronze"
)

policy_info_silver = (
    policy_info_df
    .dropDuplicates(
        ["policyId"]
    )
    .withColumn(
        "customerName",
        F.initcap(
            F.col(
                "customerName"
            )
        )
    )
    .withColumn(
        "state",
        F.upper(
            F.col("state")
        )
    )
)

merge_to_silver(
    policy_info_silver,
    f"{CATALOG}.trusted.policy_info_silver",
    "t.policyId = s.policyId"
)

print(
    "Policy info processada!"
)


# COMMAND ----------
# PREMIUM
# COMMAND ----------

premium_df = spark.table(
    f"{CATALOG}.raw.premium_bronze"
)

premium_silver = (
    premium_df
    .withColumn(
        "premiumAmount",
        F.col(
            "premiumAmount"
        ).cast("double")
    )
)

merge_to_silver(
    premium_silver,
    f"{CATALOG}.trusted.premium_silver",
    "t.policyId = s.policyId"
)

print(
    "Premium processada!"
)


# COMMAND ----------
# CLAIM
# COMMAND ----------

claim_df = spark.table(
    f"{CATALOG}.raw.claim_bronze"
)

claim_silver = (
    claim_df
    .withColumn(
        "claimAmount",
        F.col(
            "claimAmount"
        ).cast(
            DoubleType()
        )
    )
    .withColumn(
        "claimStatus",
        F.upper(
            F.col(
                "claimStatus"
            )
        )
    )
)

merge_to_silver(
    claim_silver,
    f"{CATALOG}.trusted.claim_silver",
    "t.claimId = s.claimId"
)

print(
    "Claim processada!"
)


# COMMAND ----------

print(
    "Silver carregada com sucesso!"
)