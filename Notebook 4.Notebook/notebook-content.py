# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e3989d51-bd46-44bf-be34-fc3ec6153077",
# META       "default_lakehouse_name": "ApacheSparkinFabric",
# META       "default_lakehouse_workspace_id": "fdcb52e6-6235-412a-9255-f9ea8d995e43",
# META       "known_lakehouses": [
# META         {
# META           "id": "e3989d51-bd46-44bf-be34-fc3ec6153077"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **# Sales order data exploration
# 
# Use this notebook to explore sales order data**

# MARKDOWN ********************

# **Create a DataFrame**
# 
# Now that you have created a workspace, a lakehouse, and a notebook you are ready to work with your data. You will use PySpark, which is the default language for Fabric notebooks, and the version of Python that is optimized for Spark.
# 


# MARKDOWN ********************

# ###### **Select your new workspace from the left bar. **
# 
# You will see a list of items contained in the workspace including your lakehouse and notebook.
# Select the lakehouse to display the Explorer pane, including the orders folder.
# From the top menu, select Open notebook, Existing notebook, and then open the notebook you created earlier. The notebook should now be open next to the Explorer pane. Expand Lakehouses, expand the Files list, and select the orders folder. The CSV files that you uploaded are listed next to the notebook editor, like this:

# CELL ********************

df = spark.read.format("csv").option("header","true").load("Files/orders/2019.csv")
# df now is a Spark DataFrame containing CSV data from "Files/orders/2019.csv".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **The output shows data from the 2019.csv file displayed in columns and rows. **
# 
# Notice that the column headers contain the first line of the data. To correct this, you need to modify the first line of the code as follows

# CELL ********************

df = spark.read.format("csv").option("header","false").load("Files/orders/2019.csv")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Descriptive column names help you make sense of data. To create meaningful column names, you need to define the schema and data types. You also need to import a standard set of Spark SQL types to define the data types. Replace the existing code with the following:

# CELL ********************

from pyspark.sql.types import *

orderSchema = StructType([
    StructField("SalesOrderNumber", StringType()),
    StructField("SalesOrderLineNumber", IntegerType()),
    StructField("OrderDate", DateType()),
    StructField("CustomerName", StringType()),
    StructField("Email", StringType()),
    StructField("Item", StringType()),
    StructField("Quantity", IntegerType()),
    StructField("UnitPrice", FloatType()),
    StructField("Tax", FloatType())
])

df = spark.read.format("csv").schema(orderSchema).load("Files/orders/2019.csv")

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# This DataFrame includes only the data from the 2019.csv file. Modify the code so that the file path uses a * wildcard to read all the files in the orders folder:

# CELL ********************

from pyspark.sql.types import *

orderSchema = StructType([
    StructField("SalesOrderNumber", StringType()),
    StructField("SalesOrderLineNumber", IntegerType()),
    StructField("OrderDate", DateType()),
    StructField("CustomerName", StringType()),
    StructField("Email", StringType()),
    StructField("Item", StringType()),
    StructField("Quantity", IntegerType()),
    StructField("UnitPrice", FloatType()),
    StructField("Tax", FloatType())
])

df = spark.read.format("csv").schema(orderSchema).load("Files/orders/*.csv")

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Explore data in a DataFrame**
# 
# The DataFrame object provides additional functionality such as the ability to filter, group, and manipulate data.

# MARKDOWN ********************

# **Filter a DataFrame**
# 
# Add a code cell by selecting + Code which appears when you hover the mouse above or below the current cell or its output. Alternatively, from the ribbon menu select Edit and + Add code cell below.
# 
# The following code filters the data so that only two columns are returned. It also uses count and distinct to summarize the number of records:

# CELL ********************

customers = df['CustomerName', 'Email']

print(customers.count())
print(customers.distinct().count())

display(customers.distinct())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Run the code, and examine the output:**
# 
# 
# The code creates a new DataFrame called customers which contains a subset of columns from the original df DataFrame. When performing a DataFrame transformation you do not modify the original DataFrame, but return a new one.
# Another way of achieving the same result is to use the select method:

# CELL ********************

customers = df.select("CustomerName", "Email")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Modify the first line of the code by using select with a where function as follows:**

# CELL ********************

customers = df.select("CustomerName", "Email").where(df['Item']=='Road-250 Red, 52')
print(customers.count())
print(customers.distinct().count())

display(customers.distinct())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Aggregate and group data in a DataFrame**
# 
# Add a code cell, and enter the following code:

# CELL ********************

productSales = df.select("Item", "Quantity").groupBy("Item").sum()

display(productSales)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Run the code. You can see that the results show the sum of order quantities grouped by product. 
# ###### **The groupBy method groups the rows by Item, and the subsequent sum aggregate function is applied to the remaining numeric columns - in this case, Quantity.**

# MARKDOWN ********************

# Add another code cell to the notebook, and enter the following code:

# CELL ********************

from pyspark.sql.functions import *

yearlySales = df.select(year(col("OrderDate")).alias("Year")).groupBy("Year").count().orderBy("Year")

display(yearlySales)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Run the cell. Examine the output. The results now show the number of sales orders per year:**
# 
# 
# 1. - The import statement enables you to use the Spark SQL library.
# 2. - 
# 3. - The select method is used with a SQL year function to extract the year component of the OrderDate field.
# 4. - 
# 5. - The alias method is used to assign a column name to the extracted year value.
# 6. - 
# 7. - The groupBy method groups the data by the derived Year column.
# 8. - 
# 9. - The count of rows in each group is calculated before the orderBy method is used to sort the resulting DataFrame.
