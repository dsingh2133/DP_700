# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "6c8d800b-356f-4ef6-a844-b99f7730c4cc",
# META       "default_lakehouse_name": "Sales",
# META       "default_lakehouse_workspace_id": "fdcb52e6-6235-412a-9255-f9ea8d995e43",
# META       "known_lakehouses": [
# META         {
# META           "id": "6c8d800b-356f-4ef6-a844-b99f7730c4cc"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
table_name = "products"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *

# Read the new sales data
df = spark.read.format("csv").option("header","true").load("Files/new_data/products.csv")


# Load the data into a table
df.write.format("delta").mode("append").saveAsTable(table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
