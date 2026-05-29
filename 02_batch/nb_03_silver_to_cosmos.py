# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

entities = [

    "general",
    "policy_info",
    "premium",
    "claim"
]

for entity in entities:

    source_table = (
        f"fvp_lab.trusted."
        f"{entity}_silver"
    )

    target_table = (
        f"fvp_lab.cosmos."
        f"{entity}_cosmos"
    )

    print(
        f"Processando {entity}"
    )

    source_df = spark.table(
        source_table
    )

    # =====================================
    # CONSENT ID NULL
    # (vem do Kafka depois)
    # =====================================

    if "consentId" not in source_df.columns:

        source_df = (
            source_df.withColumn(
                "consentId",
                F.lit(None)
                .cast("string")
            )
        )

    # =====================================
    # CARIMBO COSMOS
    # =====================================

    source_df = (
        source_df.withColumn(
            "upsertTimestamp",
            F.current_timestamp()
        )
    )

    # =====================================
    # CREATE TABLE
    # =====================================

    if not spark.catalog.tableExists(
        target_table
    ):

        (
            source_df.write
            .format("delta")
            .mode("overwrite")
            .option(
                "overwriteSchema",
                "true"
            )
            .saveAsTable(
                target_table
            )
        )

        print(
            f"Criada: {target_table}"
        )

    # =====================================
    # MERGE INCREMENTAL
    # =====================================

    else:

        target = DeltaTable.forName(
            spark,
            target_table
        )

        # claim usa claimId
        if entity == "claim":

            merge_condition = (
                "t.claimId = s.claimId"
            )

        else:

            merge_condition = (
                "t.policyId = s.policyId"
            )

        (
            target.alias("t")

            .merge(
                source_df.alias("s"),
                merge_condition
            )

            .whenMatchedUpdateAll()

            .whenNotMatchedInsertAll()

            .execute()
        )

        print(
            f"Merge executado:"
            f" {target_table}"
        )

print(
    "Silver → Cosmos finalizado!"
)

# COMMAND ----------

