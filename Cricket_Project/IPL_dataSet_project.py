# Databricks notebook source
import pandas as pd
import re,delta
from functools import reduce
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
test_Data = "/mnt/raw/IPL_2008_data"
Prod_data = "/mnt/raw/Cricsheet_data"
Sample_data = "/mnt/raw/IPL_2008_data/335982.json"
prod_delta = "/mnt/processed/Cricket_delta_prod"
sample_delta = "/mnt/processed/Cricket_delta"
def flatten(df):
   # compute Complex Fields (Lists and Structs) in Schema   
   complex_fields = dict([(field.name, field.dataType)
                             for field in df.schema.fields
                             if type(field.dataType) == ArrayType or  type(field.dataType) == StructType])
   while len(complex_fields)!=0:
      col_name=list(complex_fields.keys())[0]
      #print ("Processing :"+col_name+" Type : "+str(type(complex_fields[col_name])))
    
      # if StructType then convert all sub element to columns.
      # i.e. flatten structs
      if (type(complex_fields[col_name]) == StructType):
         expanded = [col(col_name+'.'+k).alias(col_name+'_'+k) for k in [ n.name for n in  complex_fields[col_name]]]
         df=df.select("*", *expanded).drop(col_name)
    
      # if ArrayType then add the Array Elements as Rows using the explode function
      # i.e. explode Arrays
      elif (type(complex_fields[col_name]) == ArrayType):    
         df=df.withColumn(col_name,explode_outer(col_name))
    
      # recompute remaining Complex Fields in Schema       
      complex_fields = dict([(field.name, field.dataType)
                             for field in df.schema.fields
                             if type(field.dataType) == ArrayType or  type(field.dataType) == StructType])
   return df

# COMMAND ----------

# One match Data
#crick_input_df = spark.read.option("multiline","true").json(Sample_data)
####One season data
crick_input_df = spark.read.option("multiline","true").json(test_Data)
#display(cricjson_sample_df)


# COMMAND ----------

crick_flatten_df = flatten(crick_input_df)
new_column_names_inn = [re.sub("\s",".",crick_flatten_df.columns[i]) for i in range(len(crick_flatten_df.columns))]
crick_flatten_fin_df = crick_flatten_df.toDF(*new_column_names_inn)\
                                              .withColumn("Match_id",(regexp_extract(input_file_name(),r'[\w-]+?(?=\.)',0)))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE database if not exists  Brnze_cric_dl_db

# COMMAND ----------

crick_flatten_repar_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("Brnze_cric_dl_db.brnz_cric_tbl")

# COMMAND ----------

# flatten the innings complexarray
crick_flat_innings_df = flatten(crick_input_df.select(col("innings")))

# Rename columns using regex in innings_array
new_column_names_inn = [re.sub("(innings_*)|(overs_)|(deliveries_*)|(wickets_*)","",crick_flat_innings_df.columns[i]) for i in range(len(crick_flat_innings_df.columns))]
crick_innings_rename_df = crick_flat_innings_df.toDF(*new_column_names_inn)\
                                              .withColumn("Match_id",(regexp_extract(input_file_name(),r'[\w-]+?(?=\.)',0)))
#remvove unawamted columns
inn_cols_drop = reduce(list.__add__,[re.findall("(?:miscounted\w+)|(?:replacements\w+)",str(crick_innings_rename_df[i]))for i in range(len(crick_innings_rename_df.columns))])
crick_innings_final_df = crick_innings_rename_df.drop(*inn_cols_drop)

#crick_innings_final_df.count()

# COMMAND ----------

## Flatten the info complex array
crick_flat_info_drt_df = flatten(crick_input_df.select(col("info")))
##To rename the info in the column names in info_array
new_column_names_info = [re.sub("(info_*)","",crick_flat_info_drt_df.columns[i]) for i in range(len(crick_flat_info_drt_df.columns))]
crick_info_renamecols_df = crick_flat_info_drt_df.toDF(*new_column_names_info)\
                                                        .withColumn("Match_id",(regexp_extract(input_file_name(),r'[\w-]+?(?=\.)',0)))
## to remove unwanted columns in the info array
info_cols_todrop = [c for c in crick_info_renamecols_df.columns if "registry"in c ] +[c for c in crick_info_renamecols_df.columns if "players"in c ]+["balls_per_over","crick_info_col_drop","match_type","gender","team_type","officials_reserve_umpires"]
                                                        
crick_info_cls_col_df = crick_info_renamecols_df.drop(*info_cols_todrop)

# COMMAND ----------

# transposing umpires and rows from rows to columns
Col_div_df = crick_info_cls_col_df.groupby(['Match_id'])\
                            .agg(collect_set(col("officials_umpires")).alias("umpires"),collect_set(col("teams")).alias("teams_t"))\
                            .select("Match_id",col("umpires")[0].alias("umpire_1"),col("umpires")[1].alias("umpire_2"),col("teams_t")[0].alias("Team_A"),col("teams_t")[1].alias("Team_B"))         

crick_info_final_df = crick_info_cls_col_df.join(Col_div_df,on=['Match_id'],how="inner")\
                                        .drop("officials_umpires","teams")\
                                        .dropDuplicates()

# COMMAND ----------

crick_join_df = crick_innings_final_df.join(crick_info_final_df,on=['Match_id'],how="inner")\
                                             .orderBy(['Match_id','team','over'])

# COMMAND ----------

display(crick_join_df)

# COMMAND ----------

crick_filled_df = crick_join_df.na.fill({ 'extras_byes':0, 'extras_legbyes':0,'extras_noballs':0,'extras_penalty':0,'extras_wides':0,'event_stage':"League" })

# COMMAND ----------

display(crick_filled_df)

# COMMAND ----------

crick_ref_df.write.format("delta").save(prod_delta)

# COMMAND ----------

delta_df =  spark.read.format("delta").load(prod_delta)

# COMMAND ----------

display(delta_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists ipl_drop

# COMMAND ----------

# MAGIC %sql
# MAGIC drop database ipl_drop

# COMMAND ----------

# MAGIC %sql show databases

# COMMAND ----------


