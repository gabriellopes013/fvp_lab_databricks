# Databricks notebook source

from pyspark.sql.types import *
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
# CREATE CHECKPOINT VOLUME
# ==========================================

spark.sql(
    f"USE CATALOG {CATALOG}"
)

spark.sql(
    """
    CREATE VOLUME IF NOT EXISTS
    raw.checkpoints
    """
)


# ==========================================
# PATHS
# ==========================================

# ==========================================
# PATHS
# ==========================================

KAFKA_PATH = (
    f"/Volumes/{CATALOG}/raw/"
    f"kafka_messages"
)

CHECKPOINT_PATH = (
    f"/Volumes/{CATALOG}/raw/"
    f"checkpoints/"
    f"policy_consent"
)

POLICY_CONSENT_TABLE = (
    f"{CATALOG}.streaming."
    "policy_consent"
)

COSMOS_TABLES = [

    "general_cosmos",
    "policy_info_cosmos",
    "premium_cosmos",
    "claim_cosmos",
    "resource_cosmos"
]

print(
    f"Kafka path: {KAFKA_PATH}"
)

print(
    f"Checkpoint: "
    f"{CHECKPOINT_PATH}"
)


# ==========================================
# SCHEMA
# ==========================================

schema = StructType([

    StructField(
        "policyId",
        StringType(),
        True
    ),

    StructField(
        "consentId",
        StringType(),
        True
    ),

    StructField(
        "allowed",
        BooleanType(),
        True
    ),

    StructField(
        "createdAt",
        StringType(),
        True
    )
])


# ==========================================
# STREAM
# ==========================================

stream_df = (

    spark.readStream
    .schema(schema)
    .json(KAFKA_PATH)
)


# ==========================================
# FOREACH BATCH
# ==========================================

def process_policy_consent(
    micro_batch_df,
    batch_id
):

    if micro_batch_df.count() == 0:

        print(
            "Batch vazio."
        )

        return

    # =====================
    # CONTROL FIELDS
    # =====================

    consent_df = (

        micro_batch_df

        .withColumn(
            "createdAt",
            F.to_timestamp(
                "createdAt"
            )
        )

        .withColumn(
            "processedAt",
            F.current_timestamp()
        )

        .withColumn(
            "documentAvailable",
            F.col("allowed")
        )
    )

    # =====================
    # POLICY CONSENT UPSERT
    # =====================

    target = (
        DeltaTable.forName(
            spark,
            POLICY_CONSENT_TABLE
        )
    )

    (
        target.alias("t")

        .merge(
            consent_df.alias("s"),
            "t.policyId = s.policyId"
        )

        .whenMatchedUpdateAll()

        .whenNotMatchedInsertAll()

        .execute()
    )

    print(
        f"Policy consent "
        f"atualizado - "
        f"batch {batch_id}"
    )

    # =====================
    # UPDATE COSMOS
    # =====================

    for table in COSMOS_TABLES:

        cosmos_table = (
            f"{CATALOG}.cosmos."
            f"{table}"
        )

        delta_table = (
            DeltaTable.forName(
                spark,
                cosmos_table
            )
        )

        (
            delta_table.alias("t")

            .merge(
                consent_df.alias("s"),
                "t.policyId = s.policyId"
            )

            .whenMatchedUpdate(
                set={

                    "consentId":
                        "s.consentId",

                    "upsertTimestamp":
                        "current_timestamp()"
                }
            )

            .execute()
        )

        print(
            f"Cosmos atualizado: "
            f"{table}"
        )


# ==========================================
# EXECUTE STREAM
# ==========================================

query = (

    stream_df.writeStream

    .foreachBatch(
        process_policy_consent
    )

    .option(
        "checkpointLocation",
        CHECKPOINT_PATH
    )

    .trigger(
        availableNow=True
    )

    .start()
)

query.awaitTermination()

print(
    "Policy consent "
    "stream finalizado!"
)