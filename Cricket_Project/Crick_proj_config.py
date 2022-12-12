# Databricks notebook source
## Config files paramaters and functions required for the project

# COMMAND ----------

import pandas as pd
import re,delta
from functools import reduce
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *

# COMMAND ----------

#Parameters
test_Data = "/mnt/raw/IPL_2008_data"
Prod_data = "/mnt/raw/Cricsheet_data"
Sample_data = "/mnt/raw/IPL_2008_data/335982.json"
prod_delta = "/mnt/processed/Cricket_delta_prod"
sample_delta = "/mnt/processed/Cricket_delta"

# COMMAND ----------

## Function to flatten the complex Json
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
