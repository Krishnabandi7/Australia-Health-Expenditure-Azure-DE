# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.healthexpenditure.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.healthexpenditure.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.healthexpenditure.dfs.core.windows.net","app_id")
spark.conf.set("fs.azure.account.oauth2.client.secret.healthexpenditure.dfs.core.windows.net", "secret_key")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.healthexpenditure.dfs.core.windows.net", "https://login.microsoftonline.com/tenent_id/oauth2/token")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
def add_ingestion_date(input_df):
  output_df = input_df.withColumn("ingestion_date", current_timestamp())
  return output_df

# COMMAND ----------



expnd_df = spark.read.format('csv') \
    .option('header', True) \
    .option('inferSchema', True) \
    .load('abfss://raw-data@healthexpenditure.dfs.core.windows.net/australian_health_expenditure.csv')



# COMMAND ----------

expnd_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Define file schema and read file
# MAGIC
# MAGIC will failed if the input file does not follow the defined schema. To prevent populating corrupted data

# COMMAND ----------

expenditure_schema = StructType(fields=[StructField("Year", StringType(), False),
                                    StructField("Jurisdiction", StringType(), False),
                                    StructField("Sector", StringType(), False),
                                    StructField("Area of expenditure", StringType(), False),
                                    StructField("Broad source of funds", StringType(), False),
                                    StructField("Source of funds", StringType(), False),
                                    StructField("Current amount ($)", DecimalType(20,2), False),
                                    StructField("Constant amount ($)", DecimalType(20,2), False)
                                    ])



# COMMAND ----------

# MAGIC %md
# MAGIC **the below command copies the data from database file sysytem into the data frame using the schema**

# COMMAND ----------

expenditure_df = spark.read.schema(expenditure_schema) \
    .option("header", True) \
    .csv('abfss://raw-data@healthexpenditure.dfs.core.windows.net/australian_health_expenditure.csv')

# COMMAND ----------

expenditure_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC now we have to rename the columns of current amount and constant amount so they look properly and can be easy to access 

# COMMAND ----------

expenditure_renamed_df=expenditure_df.withColumnRenamed("Area of Expenditure","Area_of_Expenditure")\
                                    .withColumnRenamed("Broad source of funds","Broad_source_of_funds")\
                                      .withColumnRenamed("Source of funds","Source_of_funds")\
                                        .withColumnRenamed("Current amount ($)","Current_amount")\
                                        .withColumnRenamed("Constant amount ($)","Constant_amount")


# COMMAND ----------

expenditure_renamed_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC here  the below command assigns the literal value of substring of column year and returns 2 values form the positions 6 to yy

# COMMAND ----------

#extract the end year from financial year 
df = expenditure_renamed_df.withColumn("yy", lit(substring(col("year"), 6, 2)) )

# COMMAND ----------

df.display()

# COMMAND ----------

#construct last month of the financial year. Drop the derived column "yy". And captilatise Jurisdiction values
expenditure_eofy_df= df.withColumn("eofy",concat(lit("Jun-"),col("yy")))\
                        .drop("yy")\
                        .withColumn("Jurisdiction",upper(col("Jurisdiction")))

# COMMAND ----------

expenditure_eofy_df.display()

# COMMAND ----------

expenditure_final_df = add_ingestion_date(expenditure_eofy_df)


# COMMAND ----------

expenditure_final_df.display()

# COMMAND ----------

expenditure_final_df.write.format('parquet')\
    .mode('overwrite')\
    .option("path","abfss://processed-data@healthexpenditure.dfs.core.windows.net/australian_health_expenditure")\
    .save()

# COMMAND ----------

