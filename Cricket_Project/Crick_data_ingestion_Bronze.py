# Databricks notebook source
# MAGIC %run /Users/Saravana_admin@5njbxz.onmicrosoft.com/Cricket_Project/Crick_proj_config

# COMMAND ----------

# One match Data
#crick_input_df = spark.read.option("multiline","true").json(Sample_data)
####One season data
crick_input_df = spark.read.option("multiline","true").json(test_Data)
#display(cricjson_sample_df)

# COMMAND ----------

## Preparing the df to ingest into delta table
crick_flatten_df = flatten(crick_input_df)
new_column_names_inn = [re.sub("\s",".",crick_flatten_df.columns[i]) for i in range(len(crick_flatten_df.columns))]
crick_flatten_fin_df = crick_flatten_df.toDF(*new_column_names_inn)\
                                              .withColumn("Match_id",(regexp_extract(input_file_name(),r'[\w-]+?(?=\.)',0)))

# COMMAND ----------

# MAGIC %sql
# MAGIC --creating the database
# MAGIC CREATE database if not exists  Brnze_cric_dl_db

# COMMAND ----------

crick_flatten_fin_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("Brnze_cric_dl_db.brnz_cric_tbl")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from brnze_cric_dl_db.brnz_cric_tbl

# COMMAND ----------


