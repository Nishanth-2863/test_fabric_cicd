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

# Welcome to your new notebook
# Type here in the cell editor to add code!
from pyspark.sql import functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_tbl = spark.table('good_transactions')
silver_tbl.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_gold = (silver_tbl
            .groupBy('merchant_id').agg(
                F.count('txn_id').alias("total_transactions"),
                F.sum(F.when(F.col("status")=="SUCCESS",F.col('amount')).otherwise(0)).alias("success_transaction_amt"),
                F.sum(F.when(F.col("status")=="SUCCESS",1).otherwise(0)).alias("success_trans_cx"))
            .withColumn('success_rate',F.col('success_trans_cx')/F.col('total_transactions'))

)

df_gold.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
# CELL ********************
#new line 
