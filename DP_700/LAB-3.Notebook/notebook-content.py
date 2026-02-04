# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "535b1803-7321-4094-9d2b-ef7e475a7398",
# META       "default_lakehouse_name": "DP_700_Exam",
# META       "default_lakehouse_workspace_id": "fdcb52e6-6235-412a-9255-f9ea8d995e43",
# META       "known_lakehouses": [
# META         {
# META           "id": "535b1803-7321-4094-9d2b-ef7e475a7398"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ###### **LAB 3 Delta Lake tables**
# 
#  Use this notebook to explore Delta Lake functionality 


# CELL ********************

from pyspark.sql.types import StructType, IntegerType, StringType, DoubleType

# define the schema
schema = StructType() \
.add("ProductID", IntegerType(), True) \
.add("ProductName", StringType(), True) \
.add("Category", StringType(), True) \
.add("ListPrice", DoubleType(), True)

df = spark.read.format("csv").option("header","true").schema(schema).load("Files/products/products.csv")
# df now is a Spark DataFrame containing CSV data from "Files/products/products.csv".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### **Create Delta tables**
# You can save the DataFrame as a **Delta table **by using the** saveAsTable method**. **Delta Lake supports the creation of both managed and external tables:**
# 
# Managed Delta tables benefit from higher performance, as Fabric manages both the schema metadata and the data files.
# External tables allow you to store data externally, with the metadata managed by Fabric.

# CELL ********************

df.write.format("delta").saveAsTable("managed_products")

#abfss://DP_700@onelake.dfs.fabric.microsoft.com/DP_700_Exam.Lakehouse/Files/products

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.write.format("delta").saveAsTable("external_products", path="abfss://DP_700@onelake.dfs.fabric.microsoft.com/DP_700_Exam.Lakehouse/Files/products/external_products")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### **Compare managed and external tables**
# 
# Let’s explore the differences between managed and external tables using the %%sql magic command.

# CELL ********************

# MAGIC %%sql
# MAGIC DESCRIBE FORMATTED managed_products;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DESCRIBE FORMATTED external_products;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE managed_products;
# MAGIC DROP TABLE external_products;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE products;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### **Use SQL to create a Delta table**
# 
# You will now create a Delta table, using the %%sql magic command.
# 
# Add another code cell and run the following code:

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE products
# MAGIC USING DELTA
# MAGIC LOCATION 'Files/products/external_products';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM products;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM DP_700_Exam.products LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### **Explore table versioning**
# 
# Transaction history for Delta tables is stored in JSON files in the delta_log folder. You can use this transaction log to manage data versioning.
# 
# Add a new code cell to the notebook and run the following code which implements a 10% reduction in the price for mountain bikes:

# CELL ********************

# MAGIC %%sql
# MAGIC UPDATE products
# MAGIC SET ListPrice = ListPrice * 0.9
# MAGIC WHERE Category = 'Mountain Bikes';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DESCRIBE HISTORY products;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **The results show the history of transactions recorded for the table.**
# 
# Add another code cell and run the following code:

# CELL ********************

delta_table_path = 'Files/products/external_products'
# Get the current data
current_data = spark.read.format("delta").load(delta_table_path)
display(current_data)

# Get the version 0 data
original_data = spark.read.format("delta").option("versionAsOf", 0).load(delta_table_path)
display(original_data)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# <u>###### **Two result sets are returned - one containing the data after the price reduction, and the other showing the original version of the data.**</u>

# MARKDOWN ********************

# **Analyze Delta table data with SQL queries**
# 
# Using the SQL magic command you can use SQL syntax instead of Pyspark. Here you will create a temporary view from the products table using a SELECT statement.
# 
# Add a new code cell, and run the following code to create and display the temporary view:

# CELL ********************

# MAGIC %%sql
# MAGIC -- Create a temporary view
# MAGIC CREATE OR REPLACE TEMPORARY VIEW products_view
# MAGIC AS
# MAGIC     SELECT Category, 
# MAGIC            COUNT(*) AS NumProducts, 
# MAGIC            MIN(ListPrice) AS MinPrice, 
# MAGIC            MAX(ListPrice) AS MaxPrice, 
# MAGIC            AVG(ListPrice) AS AvgPrice
# MAGIC     FROM products
# MAGIC     GROUP BY Category;
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM products_view
# MAGIC ORDER BY Category;    

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Add a new code cell, and run the following code to return the top 10 categories by number of products:**

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT Category, NumProducts
# MAGIC FROM products_view
# MAGIC ORDER BY NumProducts DESC
# MAGIC LIMIT 10;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Alternatively, you can run a SQL query using PySpark.**
# 
# Add a new code cell, and run the following code:

# CELL ********************

from pyspark.sql.functions import col, desc

df_products = spark.sql("SELECT Category, MinPrice, MaxPrice, AvgPrice FROM products_view").orderBy(col("AvgPrice").desc())
display(df_products.limit(6))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### **Use Delta tables for streaming data**
# Delta Lake supports streaming data. Delta tables can be a sink or a source for data streams created using the Spark Structured Streaming API. In this example, you’ll use a Delta table as a sink for some streaming data in a simulated internet of things (IoT) scenario.
# 
# Add a new code cell and add the following code and run it:

# CELL ********************

from notebookutils import mssparkutils
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Create a folder
inputPath = 'Files/data/'
mssparkutils.fs.mkdirs(inputPath)

# Create a stream that reads data from the folder, using a JSON schema
jsonSchema = StructType([
StructField("device", StringType(), False),
StructField("status", StringType(), False)
])
iotstream = spark.readStream.schema(jsonSchema).option("maxFilesPerTrigger", 1).json(inputPath)

# Write some event data to the folder
device_data = '''{"device":"Dev1","status":"ok"}
{"device":"Dev1","status":"ok"}
{"device":"Dev1","status":"ok"}
{"device":"Dev2","status":"error"}
{"device":"Dev1","status":"ok"}
{"device":"Dev1","status":"error"}
{"device":"Dev2","status":"ok"}
{"device":"Dev2","status":"error"}
{"device":"Dev1","status":"ok"}'''

mssparkutils.fs.put(inputPath + "data.txt", device_data, True)

print("Source stream created...")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Ensure the message Source stream created… is displayed. The code you just ran has created a streaming data source based on a folder to which some data has been saved, representing readings from hypothetical IoT devices.
# 
# In a new code cell, add and run the following code:

# CELL ********************

# Write the stream to a delta table
delta_stream_table_path = 'Tables/iotdevicedata'
checkpointpath = 'Files/delta/checkpoint'
deltastream = iotstream.writeStream.format("delta").option("checkpointLocation", checkpointpath).start(delta_stream_table_path)
print("Streaming to delta sink...")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# This code writes the streaming device data in Delta format to a folder named iotdevicedata. Because the path for the folder location in the Tables folder, a table will automatically be created for it.
# 
# In a new code cell, add and run the following code:

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM IotDeviceData;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# This code queries the IotDeviceData table, which contains the device data from the streaming source.
# 
# In a new code cell, add and run the following code:

# CELL ********************

# Add more data to the source stream
more_data = '''{"device":"Dev1","status":"ok"}
{"device":"Dev1","status":"ok"}
{"device":"Dev1","status":"ok"}
{"device":"Dev1","status":"ok"}
{"device":"Dev1","status":"error"}
{"device":"Dev2","status":"error"}
{"device":"Dev1","status":"ok"}'''

mssparkutils.fs.put(inputPath + "more-data.txt", more_data, True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Add more data to the source stream
more_data = '''{"device":"Dev1","status":"ok"}
{"device":"Dev1","status":"ok"}
{"device":"Dev1","status":"ok"}
{"device":"Dev1","status":"ok"}
{"device":"Dev1","status":"error"}
{"device":"Dev2","status":"error"}
{"device":"Dev1","status":"ok"}'''

mssparkutils.fs.put(inputPath + "more-data.txt", more_data, True)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# This code writes more hypothetical device data to the streaming source.
# 
# Re-run the cell containing the following code:

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM IotDeviceData;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM DP_700_Exam.iotdevicedata LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

deltastream.stop()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
