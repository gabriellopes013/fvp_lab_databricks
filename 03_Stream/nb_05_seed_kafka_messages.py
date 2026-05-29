# Databricks notebook source
import json
from datetime import datetime

# COMMAND ----------

volume_path = (
    "/Volumes/fvp_lab/raw/"
    "kafka_messages"
)

# COMMAND ----------

messages = [
    {
        "policyId": "P001",
        "consentId": "CONS001",
        "allowed": True,
        "createdAt": datetime.now().isoformat()
    },
    {
        "policyId": "P002",
        "consentId": "CONS002",
        "allowed": True,
        "createdAt": datetime.now().isoformat()
    }
]

for i, message in enumerate(messages):

    file_path = (
        f"{volume_path}/"
        f"event_{i}.json"
    )

    dbutils.fs.put(
        file_path,
        json.dumps(message),
        overwrite=True
    )

print("Kafka fake criado!")

# COMMAND ----------

