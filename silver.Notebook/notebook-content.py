# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "7131efb6-c747-43ba-9c97-eda80ba845d5",
# META       "default_lakehouse_name": "PayFlow_LH",
# META       "default_lakehouse_workspace_id": "567b025c-c42e-41b4-855a-bfe31c6b01af",
# META       "known_lakehouses": [
# META         {
# META           "id": "7131efb6-c747-43ba-9c97-eda80ba845d5"
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
from pyspark.sql.window import Window

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze_tbl = spark.table('bronze_transactions')

windowspec = Window.partitionBy('txn_id').orderBy(F.col("txn_id").desc())
df_cleaned = (bronze_tbl
                .withColumn('r',F.row_number().over(windowspec))
                .withColumn('status',F.lower(F.trim(col("status"))))
                .withColumn('amount',F.regexp_replace('amount',',','').cast('decimal(12,2)'))
                .withColumn('txn_time',F.coalesce(
                    F.to_timestamp(F.col('txn_time'),'yyyy-MM-dd HH:mm:ss'),
                    F.to_timestamp(F.col('txn_time'),'dd-MM-yyyy HH:mm:ss')))
                .filter(F.col("r")==1)
                .drop('r')
)

df_cleaned.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

valid_cond = (
F.col("merchant_id").isNotNull()
& F.col('amount').isNotNull()
& F.col('txn_time').isNotNull()
)

df_good_transactions = df_cleaned.filter(valid_cond)
df_quarantine_transcations = df_cleaned.filter(~valid_cond)


df_good_transactions.write.format('delta').mode('append').saveAsTable('good_transactions')
df_quarantine_transcations.write.format('delta').mode('append').saveAsTable('quarantine_transactions')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f'Total transactions cx :{df_cleaned.count()} , good_transactions_cx :{df_good_transactions.count()}, bad_transactions : {df_quarantine_transcations.count()}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select * from dbo.bronze_transactions

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
