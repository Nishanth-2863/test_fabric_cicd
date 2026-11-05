# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "8282aa1d-2358-4b7d-b303-58da85a95ebf",
# META       "default_lakehouse_name": "PayFlow_LH",
# META       "default_lakehouse_workspace_id": "aada2b73-9eee-44e7-9ae2-ebd4d64bc06b",
# META       "known_lakehouses": [
# META         {
# META           "id": "8282aa1d-2358-4b7d-b303-58da85a95ebf"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "cd3b66c1-746f-4879-a5ee-a1a9fccf71cd",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import current_timestamp

# raw = [
#     ("T1", "C1", "M1", "1,200.50", "SUCCESS",  "2026-11-01 10:00:00"),
#     ("T2", "C2", "M1", "500",      "FAILED",   "2026-11-01 10:05:00"),
#     ("T3", "C1", "M2", "7500",     "SUCCESS",  "01-11-2026 11:00:00"),
#     ("T3", "C1", "M2", "7500",     "SUCCESS",  "01-11-2026 11:00:00"),
#     ("T4", "C3", None, "300",      "SUCCESS",  "2026-11-01 12:00:00"),
#     ("T5", "C2", "M2", "-500",     "REVERSED", "2026-11-01 13:00:00"),
#     ("T6", "C4", "M1", "abc",      "SUCCESS",  "2026-11-01 14:00:00"),
#     ("T7", "C5", "M2", "2000",     "PENDING",  "2026-11-01 15:00:00"),
# ]
# cols = ["txn_id", "customer_id", "merchant_id", "amount", "status", "txn_time"]

# df = spark.createDataFrame(raw, cols).withColumn("ingested_at", current_timestamp())
df.write.mode("overwrite").saveAsTable("bronze_transactions")
print("bronze rows:", df.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
df=(spark.read
.format('csv')
.option('header',True)
.option('inferSchema',True)
.load('Files/input/transactions.csv')
.withColumn('last_updated_at',F.current_timestamp())
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.write.mode("overwrite").saveAsTable("bronze_transactions_")
print("bronze rows:", df.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
