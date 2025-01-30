# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.healthexpenditure.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.healthexpenditure.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.healthexpenditure.dfs.core.windows.net","app_id")
spark.conf.set("fs.azure.account.oauth2.client.secret.healthexpenditure.dfs.core.windows.net", "secret_key")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.healthexpenditure.dfs.core.windows.net", "https://login.microsoftonline.com/tenent_id/oauth2/token")

# COMMAND ----------

expnd_df =spark.read.format("parquet")\
    .option("header",True)\
        .option("inferSchema",True)\
            .load("abfss://processed-data@healthexpenditure.dfs.core.windows.net/australian_health_expenditure/") \
            .withColumnRenamed("eofy", "end_of_fy") \
            .withColumnRenamed("jurisdiction", "expnd_jurisdiction")

# COMMAND ----------

expnd_df.display()

# COMMAND ----------

fund_source_df = expnd_df \
    .groupBy("year", "expnd_jurisdiction", "source_of_funds", "end_of_fy") \
    .agg(sum("current_amount").alias("total_amount"))

# COMMAND ----------

pop_df =spark.read.format("parquet")\
    .option("header",True)\
        .option("inferSchema",True)\
            .load("abfss://processed-data@healthexpenditure.dfs.core.windows.net/austrialian_population_by_state/") \
.withColumnRenamed("jurisdiction", "pop_jurisdiction")

# COMMAND ----------

join_condition = [ fund_source_df.end_of_fy == pop_df.time, fund_source_df.expnd_jurisdiction == pop_df.pop_jurisdiction ]
fund_source_with_pop_df = fund_source_df.join(pop_df, join_condition)

# COMMAND ----------


final_df = fund_source_with_pop_df \
                .withColumn("amount_per_person", expr("total_amount / population")) \
                .withColumn("created_date", current_timestamp())

final_df.display()

# COMMAND ----------

final_df.write.format("parquet")\
    .mode("overwrite")\
    .option("path","abfss://presentation-data@healthexpenditure.dfs.core.windows.net/fund_source_with_pop")\
    .save()

# COMMAND ----------

