# Databricks notebook source
from delta.tables import DeltaTable

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    DateType
)

# COMMAND ----------

spark.sql("USE CATALOG fvp_lab")

# COMMAND ----------

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

        target = (
            DeltaTable.forName(
                spark,
                target_table
            )
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

general_df = spark.table(
    "fvp_lab.raw.general_bronze"
)

valid_status = [
    "ACTIVE",
    "CANCELLED",
    "EXPIRED"
]

# ==========================================
# ERROR REASON
# ==========================================

general_validated = (

    general_df

    .withColumn(

        "error_reason",

        F.when(
            F.col("policyId").isNull(),
            "NULL_POLICY_ID"
        )

        .when(
            F.col("policyNumber").isNull(),
            "NULL_POLICY_NUMBER"
        )

        .when(
            ~F.col("status")
            .isin(valid_status),
            "INVALID_STATUS"
        )

    )
)

# ==========================================
# VALID
# ==========================================

general_valid = (

    general_validated

    .filter(
        F.col("error_reason")
        .isNull()
    )

    .drop("error_reason")
)

# ==========================================
# INVALID
# ==========================================

general_invalid = (

    general_validated

    .filter(
        F.col("error_reason")
        .isNotNull()
    )

    .withColumn(
        "quarantine_timestamp",
        F.current_timestamp()
    )
)

# ==========================================
# SILVER
# ==========================================

merge_to_silver(
    general_valid,
    "fvp_lab.trusted.general_silver",
    "t.policyId = s.policyId"
)

# ==========================================
# QUARANTINE
# ==========================================

(
    general_invalid.write
    .format("delta")
    .mode("append")
    .saveAsTable(
        "fvp_lab.quarantine.general_invalid"
    )
)

print(
    "General processada!"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM fvp_lab.raw.general_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from fvp_lab.trusted.general_silver

# COMMAND ----------

# MAGIC %md
# MAGIC ##Policy Info

# COMMAND ----------

policy_info_df = spark.table(
    "fvp_lab.raw.policy_info_bronze"
)

policy_info_silver = (
    policy_info_df
    .dropDuplicates(["policyId"])
    .withColumn(
        "customerName",
        F.initcap(
            F.col("customerName")
        )
    )
    .withColumn(
        "state",
        F.upper(F.col("state"))
    )
)

merge_to_silver(
    policy_info_silver,
    "fvp_lab.trusted.policy_info_silver",
    "t.policyId = s.policyId"
)

print("policy_info_silver criada")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Premium

# COMMAND ----------


premium_df = spark.table(
    "fvp_lab.raw.premium_bronze"
)

premium_validated = (

    premium_df

    .withColumn(

        "error_reason",

        F.when(
            F.col("premiumAmount")
            .cast("double") <= 0,
            "NEGATIVE_PREMIUM"
        )

        .when(
            F.col("currency").isNull(),
            "NULL_CURRENCY"
        )
    )
)

# ==========================================
# VALID
# ==========================================

premium_valid = (

    premium_validated

    .filter(
        F.col("error_reason")
        .isNull()
    )

    .drop("error_reason")
)

# ==========================================
# INVALID
# ==========================================

premium_invalid = (

    premium_validated

    .filter(
        F.col("error_reason")
        .isNotNull()
    )

    .withColumn(
        "quarantine_timestamp",
        F.current_timestamp()
    )
)

# ==========================================
# SILVER
# ==========================================

merge_to_silver(
    premium_valid,
    "fvp_lab.trusted.premium_silver",
    "t.policyId = s.policyId"
)

# ==========================================
# QUARANTINE
# ==========================================

(
    premium_invalid.write
    .format("delta")
    .mode("append")
    .saveAsTable(
        "fvp_lab.quarantine.premium_invalid"
    )
)

print(
    "Premium processada!"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Claim

# COMMAND ----------

claim_df = spark.table(
    "fvp_lab.raw.claim_bronze"
)

claim_silver = (
    claim_df
    .withColumn(
        "claimAmount",
        F.col("claimAmount")
        .cast(DoubleType())
    )
    .withColumn(
        "claimStatus",
        F.upper("claimStatus")
    )
)

merge_to_silver(
    claim_silver,
    "fvp_lab.trusted.claim_silver",
    "t.claimId = s.claimId"
)

print("claim_silver criada")


# COMMAND ----------


print("Silver carregada com sucesso!")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM fvp_lab.trusted.premium_silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     *
# MAGIC FROM fvp_lab.quarantine.premium_invalid
# MAGIC QUALIFY 
# MAGIC     ROW_NUMBER() OVER(PARTITION BY policyId ORDER BY quarantine_timestamp DESC) = 1

# COMMAND ----------

