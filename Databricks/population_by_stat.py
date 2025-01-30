# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *


# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.healthexpenditure.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.healthexpenditure.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.healthexpenditure.dfs.core.windows.net","app_id")
spark.conf.set("fs.azure.account.oauth2.client.secret.healthexpenditure.dfs.core.windows.net", "secret_key")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.healthexpenditure.dfs.core.windows.net", "https://login.microsoftonline.com/tenent_id/token")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
def add_ingestion_date(input_df):
  output_df = input_df.withColumn("ingestion_date", current_timestamp())
  return output_df

# COMMAND ----------

population_df=spark.read.format("csv")\
    .option("header",True).load("abfss://raw-data@healthexpenditure.dfs.core.windows.net/australian_population_by_state.csv")

# COMMAND ----------

population_df.display()

# COMMAND ----------

population_schema=StructType(fields=[StructField("Time",StringType(),False),
                                     StructField("Population-New South Wales",LongType(),False),
                                     StructField("population-Victoria",LongType(),False),
                                     StructField("Population-Queensland",LongType(),False),
                                     StructField("Populaton_South Austrialia",LongType(),False),
                                     StructField("Population-Tasmania",LongType(),False),
                                     StructField("Population-Northern Territory",LongType(),False),
                                     StructField("Population-Austrialia Capital Territory",LongType(),False),
                                     StructField("Population-Austrialia",LongType(),False)])

# COMMAND ----------

print(population_schema)

# COMMAND ----------

population_df=   spark.read.schema(population_schema)\
                         .option("header",True)\
                        .csv("abfss://raw-data@healthexpenditure.dfs.core.windows.net/australian_population_by_state.csv")

# COMMAND ----------

population_df.display()

# COMMAND ----------

pattern = "^(Jun)-([1-5][1-9])$"
population_filtered=population_df.filter(col("Time").rlike(pattern))



# COMMAND ----------

population_filtered.display()

# COMMAND ----------

population_renamed_df = population_filtered.withColumnRenamed("Population-New South Wales", "NSW") \
    .withColumnRenamed("Population-Victoria", "VIC") \
    .withColumnRenamed("Population-Queensland", "QLD") \
    .withColumnRenamed("Populaton_South Austrialia", "SA") \
    .withColumnRenamed("Population-Western Australia", "WA") \
    .withColumnRenamed("Population-Tasmania", "TAS") \
    .withColumnRenamed("Population-Northern Territory", "NT") \
    .withColumnRenamed("Population-Austrialia Capital Territory", "ACT") \
    .withColumnRenamed("Population-Austrialia", "AU") \
    .withColumnRenamed("Time", "time")

# COMMAND ----------

population_renamed_df.display()

# COMMAND ----------

population_unpivot_df= population_renamed_df.unpivot("time",["NSW","VIC","QLD","SA","TAS","NT","ACT","Au"],"Jurisdiction","Population")
population_final_df=add_ingestion_date(population_unpivot_df)

# COMMAND ----------

population_final_df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog aude

# COMMAND ----------

population_final_df.write.format("delta")\
           .mode("overwrite")\
            .saveAsTable("austrialian_population_by_state")

# COMMAND ----------

