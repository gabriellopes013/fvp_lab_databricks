# Databricks notebook source
# MAGIC %fs
# MAGIC ls /Volumes/fvp_lab/raw/mock_sap

# COMMAND ----------

mock_sap_path = "/Volumes/fvp_lab/raw/mock_sap"

# COMMAND ----------

# MAGIC %md
# MAGIC ##Policy

# COMMAND ----------

policy_info_data = [
    ("P001", "C001", "Joao Silva", "12345678900", "SP"),
    ("P002", "C002", "Maria Souza", "98765432100", "RJ"),
    ("P003", "C003", "Carlos Lima", "45678912300", "MG")
]

policy_info_columns = [
    "policyId",
    "customerId",
    "customerName",
    "document",
    "state"
]

policy_info_df = spark.createDataFrame(
    policy_info_data,
    policy_info_columns
)

policy_info_df.write.mode("overwrite") \
    .option("header", True) \
    .csv(f"{mock_sap_path}/policy_info")

# COMMAND ----------

# MAGIC %md
# MAGIC ##General

# COMMAND ----------

general_data = [
    ("P001", "POL10001", "ACTIVE", "AUTO", "2025-01-10", "2026-01-10"),
    ("P002", "POL10002", "CANCELLED", "HOME", "2025-02-15", "2026-02-15"),
    ("P003", "POL10003", "ACTIVE", "AUTO", "2025-03-20", "2026-03-20")
]

general_columns = [
    "policyId",
    "policyNumber",
    "status",
    "productType",
    "startDate",
    "endDate"
]

general_df = spark.createDataFrame(
    general_data,
    general_columns
)

general_df.write.mode("overwrite") \
    .option("header", True) \
    .csv(f"{mock_sap_path}/general")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Premium

# COMMAND ----------

premium_data = [
    ("P001", 1200.00, "BRL", "CREDIT_CARD"),
    ("P002", 850.00, "BRL", "PIX"),
    ("P003", 980.00, "BRL", "BANK_SLIP")
]

premium_columns = [
    "policyId",
    "premiumAmount",
    "currency",
    "paymentMethod"
]

premium_df = spark.createDataFrame(
    premium_data,
    premium_columns
)

premium_df.write.mode("overwrite") \
    .option("header", True) \
    .csv(f"{mock_sap_path}/premium")

# COMMAND ----------

# MAGIC %md
# MAGIC Claim

# COMMAND ----------

claim_data = [
    ("CL001", "P001", 5000.00, "OPEN"),
    ("CL002", "P001", 2000.00, "CLOSED"),
    ("CL003", "P003", 3500.00, "OPEN")
]

claim_columns = [
    "claimId",
    "policyId",
    "claimAmount",
    "claimStatus"
]

claim_df = spark.createDataFrame(
    claim_data,
    claim_columns
)

claim_df.write.mode("overwrite") \
    .option("header", True) \
    .csv(f"{mock_sap_path}/claim")


