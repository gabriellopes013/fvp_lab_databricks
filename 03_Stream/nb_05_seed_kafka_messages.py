# Databricks notebook source

import json
from datetime import datetime


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
# KAFKA PATH
# ==========================================

volume_path = (
    f"/Volumes/{CATALOG}/raw/"
    f"kafka_messages"
)

print(
    f"Volume path: "
    f"{volume_path}"
)


# ==========================================
# MOCK EVENTS
# ==========================================

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


# ==========================================
# WRITE EVENTS
# ==========================================

for i, message in enumerate(
    messages
):

    file_path = (
        f"{volume_path}/"
        f"event_{i}.json"
    )

    dbutils.fs.put(
        file_path,
        json.dumps(
            message
        ),
        overwrite=True
    )

    print(
        f"Evento criado: "
        f"{file_path}"
    )

print(
    "Kafka fake criado!"
)