# Databricks notebook source

from pyspark.sql import functions as F
from delta.tables import DeltaTable


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
# ENTITIES
# ==========================================

entities = [
    "general",
    "policy_info",
    "premium",
    "claim"
]


# ==========================================
# SILVER → COSMOS
# ==========================================

for entity in entities:

    source_table = (
        f"{CATALOG}.trusted."
        f"{entity}_silver"
    )

    target_table = (
        f"{CATALOG}.cosmos."
        f"{entity}_cosmos"
    )

    print(
        f"Processando {entity}"
    )

    source_df = spark.table(
        source_table
    )

    # =====================================
    # CONSENT ID
    # (vem do streaming depois)
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
    # UPSERT TIMESTAMP
    # =====================================

    source_df = (
        source_df.withColumn(
            "upsertTimestamp",
            F.current_timestamp()
        )
    )

    # =====================================
    # FIRST EXECUTION
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
            f"Tabela criada: "
            f"{target_table}"
        )

    # =====================================
    # INCREMENTAL MERGE
    # =====================================

    else:

        target = DeltaTable.forName(
            spark,
            target_table
        )

        # business key

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
            f"MERGE executado: "
            f"{target_table}"
        )

print(
    "Silver → Cosmos finalizado!"
)