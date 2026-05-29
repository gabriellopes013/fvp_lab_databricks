# Databricks notebook source

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
# PATHS
# ==========================================

MOCK_SAP = (
    "/Volumes/fvp_lab/raw/mock_sap"
)

TRANSIENT = (
    "/Volumes/fvp_lab/raw/transient"
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
# MOCK SAP → TRANSIENT
# ==========================================

for entity in entities:

    source_path = (
        f"{MOCK_SAP}/{entity}"
    )

    target_path = (
        f"{TRANSIENT}/{entity}"
    )

    print(
        f"Ingerindo "
        f"{entity} "
        f"para transient..."
    )

    df = (
        spark.read
        .option(
            "header",
            True
        )
        .csv(source_path)
    )

    (
        df.write
        .mode("overwrite")
        .option(
            "header",
            True
        )
        .csv(target_path)
    )

print(
    "Transient carregada "
    "com sucesso!"
)


# ==========================================
# TRANSIENT → BRONZE
# ==========================================

for entity in entities:

    transient_path = (
        f"{TRANSIENT}/{entity}"
    )

    table_name = (
        f"{CATALOG}.raw."
        f"{entity}_bronze"
    )

    print(
        f"Processando bronze: "
        f"{table_name}"
    )

    source_df = (
        spark.read
        .option(
            "header",
            True
        )
        .csv(transient_path)
    )

    # =====================
    # PRIMEIRA EXECUÇÃO
    # =====================

    if not spark.catalog.tableExists(
        table_name
    ):

        (
            source_df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(
                table_name
            )
        )

        print(
            f"Tabela criada: "
            f"{table_name}"
        )

    # =====================
    # EXECUÇÃO INCREMENTAL
    # =====================

    else:

        target = (
            DeltaTable.forName(
                spark,
                table_name
            )
        )

        # BUSINESS KEY

        if entity == "claim":

            merge_condition = (
                "t.claimId = "
                "s.claimId"
            )

        else:

            merge_condition = (
                "t.policyId = "
                "s.policyId"
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
            f"{table_name}"
        )


print(
    "Bronze carregada "
    "com sucesso!"
)