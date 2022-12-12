# Databricks notebook source
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
test_Data = "/mnt/raw/IPL_2008_data"
Prod_data = "/mnt/raw/Cricsheet_data"
Sample_data = "/mnt/raw/IPL_2008_data/335982.json"

# COMMAND ----------

csv_df = spark.read.option("header",True).csv("/mnt/crick-data-csv/IPL_2008_data_csv/")

# COMMAND ----------

display(csv_df)

# COMMAND ----------

csv_df.rdd.getNumPartitions()

# COMMAND ----------

csv_df_cs = csv_df.coalesce(2)

# COMMAND ----------

csv_df_cs.count()

# COMMAND ----------

csv_df_cs.explain()

# COMMAND ----------

display(csv_df_cs)

# COMMAND ----------

csv_df_cs.rdd.getNumPartitions()

# COMMAND ----------

csv_df_re = csv_df.repartition(20)

# COMMAND ----------

csv_df_re.rdd.getNumPartitions()

# COMMAND ----------

csv_df_re.explain()

# COMMAND ----------

display(csv_df_re)

# COMMAND ----------

display(csv_df)

# COMMAND ----------

csv_filldf = csv_df.na.fill("0",subset=["wides"])

# COMMAND ----------

display(csv_filldf)

# COMMAND ----------

display(csv_filldf.groupBy(col("match_id"),col("batting_team"),col("striker"),col("season")).agg(sum(col("runs_off_bat"))).orderBy("match_id","batting_team"))

# COMMAND ----------

display(csv_filldf.select("*"))

# COMMAND ----------

csv_filldf.rdd.getNumPartitions()

# COMMAND ----------


