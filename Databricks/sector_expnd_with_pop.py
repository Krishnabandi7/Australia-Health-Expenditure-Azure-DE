# Databricks notebook source
from pyspark.sql.functions import current_timestamp,expr
from pyspark.sql.functions import sum

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.healthexpenditure.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.healthexpenditure.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.healthexpenditure.dfs.core.windows.net","app_id")
spark.conf.set("fs.azure.account.oauth2.client.secret.healthexpenditure.dfs.core.windows.net", "secret_key")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.healthexpenditure.dfs.core.windows.net", "https://login.microsoftonline.com/tenent_id/oauth2/token")

# COMMAND ----------

expenditure_df=spark.read.format("parquet")\
    .option("header",True)\
        .option("inferSchema",True)\
            .load("abfss://processed-data@healthexpenditure.dfs.core.windows.net/australian_health_expenditure/")

# COMMAND ----------

expenditure_df.display()

# COMMAND ----------

expenditure_df=expenditure_df.withColumnRenamed("eofy","end_of_fy")\
    .withColumnRenamed("Jurisdiction","expnd_jurisdiction")

# COMMAND ----------

expenditure_df.display()

# COMMAND ----------

sector_expand_df=expenditure_df\
    .groupBy("year","expnd_jurisdiction","Sector","sector","end_of_fy")\
        .agg(sum("Constant_amount").alias("total_amount"))

# COMMAND ----------

sector_expand_df.display()

# COMMAND ----------

df_pop=spark.read.format("parquet")\
    .option("header",True)\
        .option("inferSchema",True)\
            .load("abfss://processed-data@healthexpenditure.dfs.core.windows.net/austrialian_population_by_state/")

# COMMAND ----------

df_pop.display()

# COMMAND ----------

df_pop=df_pop.withColumnRenamed("jurisdiction","pop_jurisdiction")

# COMMAND ----------

join_condition=[sector_expand_df.end_of_fy==df_pop.time,sector_expand_df.expnd_jurisdiction==df_pop.pop_jurisdiction]
sector_expand_with_pop_df= sector_expand_df.join(df_pop,join_condition)

# COMMAND ----------

sector_expand_with_pop_df.display()

# COMMAND ----------


final_df=sector_expand_with_pop_df\
    .withColumn("amount_per_person",expr("total_amount/Population"))\
        .withColumn("created_date",current_timestamp())

# COMMAND ----------

df_f123=df_f123.drop("Sector_1")

# COMMAND ----------

# MAGIC %sql
# MAGIC create catalog aude

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog aude

# COMMAND ----------


df_f123.write.format("delta")\
    .mode("overwrite")\
    .saveAsTable("sector_expand_with_pop")

# COMMAND ----------

