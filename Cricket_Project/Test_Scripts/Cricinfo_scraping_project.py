# Databricks notebook source
pip install lxml

# COMMAND ----------

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

def generate_links(type,pagenum,format_id):
    #TO generate Links for the scrapping the table
    Links = []
    for j in type:
        for format in range(1,format_id+1):
            for i in range(1,pagenum+1):
                Links.append(f"https://stats.espncricinfo.com/ci/engine/stats/index.html?class={format};size=200;page={i};template=results;type={j}")
    return Links

# COMMAND ----------

linklist = generate_links(['batting'],16,1)
Output_pd_df = pd.DataFrame()
for link in linklist:
    Pandas_df = pd.read_html(link)[2]
    Output_pd_df = pd.concat([Output_pd_df,Pandas_df],ignore_index = True)

# COMMAND ----------

spark_df = spark.createDataFrame(Output_pd_df)
spark_df_dropped = spark_df.drop('Unnamed: 11')
#spark_df_dropped = spark_df_dropped.select(regexp_replace("HS", "[^0-9a-zA-Z_\-]+","").alias('replaced_str'))
spark_df_reg = spark_df_dropped.withColumn("HS",regexp_replace(spark_df_dropped["HS"],"\\*",""))
spark_df_reg = spark_df_reg.withColumn("HS",spark_df_reg["HS"].cast("long"))\
                            .withColumn("Nationality", regexp_extract(spark_df_reg["Player"],"(?<=\().+?(?=\))",0))\
                            .withColumn("Player",regexp_replace(spark_df_reg["Player"],"\(.*?\)",""))\
                            .select("Player","Nationality","Span","Runs","Mat","Inns","NO","HS","Ave","100","50","0")

                            
#spark_df_dropped.write.mode('append').options(header='true').format('csv').save("/mnt/blob_test/Input/Crickinfo_output")
