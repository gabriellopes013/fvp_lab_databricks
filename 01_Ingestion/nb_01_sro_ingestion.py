# Databricks notebook source
from delta.tables import DeltaTable

# COMMAND ----------

MOCK_SAP = "/Volumes/fvp_lab/raw/mock_sap"
TRANSIENT = "/Volumes/fvp_lab/raw/transient"

# COMMAND ----------

spark.sql("USE CATALOG fvp_lab")

entities = [
    "general",
    "policy_info",
    "premium",
    "claim"
]

# COMMAND ----------

for entity in entities:

    source_path = f"{MOCK_SAP}/{entity}"
    target_path = f"{TRANSIENT}/{entity}"

    print(f"Ingerindo {entity} para transient...")

    df = (
        spark.read
        .option("header", True)
        .csv(source_path)
    )

    (
        df.write
        .mode("overwrite")
        .option("header", True)
        .csv(target_path)
    )

print("Transient carregada com sucesso!")

# COMMAND ----------

for entity in entities:

    transient_path = f"{TRANSIENT}/{entity}"

    table_name = f"fvp_lab.raw.{entity}_bronze"

    print(f"Criando bronze: {table_name}")

    source_df = (
        spark.read
        .option("header", True)
        .csv(transient_path)
    )

    if not spark.catalog.tableExists(
        table_name
    ):

        (
            source_df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(table_name)
        )

        print(
            f"Tabela criada:"
            f" {table_name}"
        )

    # ======================================
    # EXECUÇÕES INCREMENTAIS
    # ======================================

else:

    target = (
        DeltaTable.forName(
            spark,
            table_name
        )
    )

    # ======================================
    # DEFINE BUSINESS KEY
    # ======================================

    if entity == "claim":

        merge_condition = (
            "t.claimId = s.claimId"
        )

    else:

        merge_condition = (
            "t.policyId = s.policyId"
        )

    # ======================================
    # MERGE INCREMENTAL
    # ======================================

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
        f"MERGE executado:"
        f" {table_name}"
    )

# COMMAND ----------

